# main.py
# Cloud Run Job: lê XLSX do GCS (lamina_*), padroniza colunas, gera Parquet em staging e carrega no BigQuery
# Logging no padrão do seu código: google.cloud.logging + logger nomeado + severities, com logs bem mais ricos.

import os
import re
import time
import json
import unicodedata
import traceback
from io import BytesIO
from datetime import datetime, date, timezone
from typing import Dict, List, Tuple
from functools import lru_cache

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import storage
from google.cloud import bigquery
import google.cloud.logging
from google.api_core.exceptions import NotFound, BadRequest


# --------------------------
# Configurações (ENV first)
# --------------------------
GCS_BUCKET = os.getenv("GCS_BUCKET", "laminas_fechamento_invest")
GCS_PATH_PREFIX = os.getenv("GCS_PATH_PREFIX", "").lstrip("/")  # ex: "raw/"

FILE_PREFIX = os.getenv("FILE_PREFIX", "lamina_")
FILE_SUFFIX = os.getenv(
    "FILE_SUFFIX", ""
)  # pode setar "" para aceitar qualquer lamina_*.xlsx

SHEET = os.getenv("SHEET", "0")  # índice "0" ou nome da aba
HEADER_ROW = int(os.getenv("HEADER_ROW", "0"))  # linha do header (0=primeira)
DROP_EMPTY_COLUMNS = os.getenv("DROP_EMPTY_COLUMNS", "true").lower() == "true"
FORCE_STRING = os.getenv("FORCE_STRING", "true").lower() == "true"

STAGING_PREFIX = (
    os.getenv("STAGING_PREFIX", "_staging/laminas_fechamento_invest/").rstrip("/") + "/"
)
CLEANUP_STAGING = os.getenv("CLEANUP_STAGING", "false").lower() == "true"

BQ_PROJECT = (
    os.getenv("BQ_PROJECT", "housi-dados").strip()
    or os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
)
BQ_DATASET = os.getenv("BQ_DATASET", "INVEST_REFINED_ZONE")
BQ_TABLE = os.getenv("BQ_TABLE", "laminas_fechamento_invest")

LOAD_MODE = (
    os.getenv("LOAD_MODE", "overwrite_partition").strip().lower()
)  # overwrite_partition|append|overwrite_table
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
MAX_FILES = int(os.getenv("MAX_FILES", "0") or "0")
FAIL_ON_FILE_ERROR = os.getenv("FAIL_ON_FILE_ERROR", "false").lower() == "true"

RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3") or "3")
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "2") or "2")  # segundos (base)
STDOUT_MIRROR = os.getenv("STDOUT_MIRROR", "true").lower() == "true"

LOGGER_NAME = os.getenv("LOGGER_NAME", "laminas_fechamento")
LOG_TAG = os.getenv("LOG_TAG", "laminas_fechamento")
RUN_ID = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


PT_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


# --------------------------
# Setup de Cloud Logging com Logger Nomeado (padrão do seu código)
# --------------------------
client_logging = google.cloud.logging.Client()
client_logging.setup_logging()
logger = client_logging.logger(LOGGER_NAME)


def _mirror_stdout(payload: Dict):
    if STDOUT_MIRROR:
        print(json.dumps(payload, ensure_ascii=False), flush=True)


def log_event(message: str, severity: str = "INFO", **fields):
    """
    Log estruturado (ótimo para Router/sinks) + log_text (padrão humano)
    + espelho opcional em stdout (ajuda a ver logs no Cloud Run UI mesmo com sink errado)
    """
    payload = {
        "tag": LOG_TAG,
        "run_id": RUN_ID,
        "message": message,
        "severity": severity,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_name": os.getenv("CLOUD_RUN_JOB", ""),
        "execution": os.getenv("CLOUD_RUN_EXECUTION", ""),
        "task_index": os.getenv("CLOUD_RUN_TASK_INDEX", ""),
        **fields,
    }

    # 1) Estruturado (melhor para filtros do Router)
    try:
        logger.log_struct(payload, severity=severity)
    except Exception:
        pass

    # 2) Texto (padrão do seu código)
    try:
        # mensagem curta + campos principais
        extra = ""
        if fields:
            # evita logs gigantes
            keys = [
                "file",
                "gcs_uri",
                "staging_uri",
                "destination",
                "rows",
                "cols",
                "elapsed_ms",
                "error",
            ]
            compact = {k: fields.get(k) for k in keys if k in fields}
            if compact:
                extra = f" | {compact}"
        logger.log_text(f"[{LOG_TAG}] {message}{extra}", severity=severity)
    except Exception:
        pass

    # 3) stdout (para depuração rápida)
    _mirror_stdout(payload)


class StepTimer:
    def __init__(self, step: str, **fields):
        self.step = step
        self.fields = fields
        self.t0 = None

    def __enter__(self):
        self.t0 = time.time()
        log_event(f"START {self.step}", "INFO", **self.fields)
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed_ms = int((time.time() - self.t0) * 1000)
        if exc:
            log_event(
                f"FAIL {self.step}",
                "ERROR",
                elapsed_ms=elapsed_ms,
                error=str(exc),
                traceback=traceback.format_exc(),
                **self.fields,
            )
            return False
        log_event(f"END {self.step}", "INFO", elapsed_ms=elapsed_ms, **self.fields)
        return True


# --------------------------
# Retry (padrão semelhante ao seu)
# --------------------------
def retry_on_failure(func):
    def wrapper(*args, **kwargs):
        delay = RETRY_DELAY
        for attempt in range(RETRY_ATTEMPTS):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    raise
                log_event(
                    f"Tentativa {attempt + 1}/{RETRY_ATTEMPTS} falhou: {e}. Retentando em {delay:.1f}s...",
                    "WARNING",
                    func=func.__name__,
                    attempt=attempt + 1,
                    retry_in_s=delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30)  # backoff simples
        return None

    return wrapper


# --------------------------
# Clients (com cache)
# --------------------------
@lru_cache(maxsize=1)
def get_storage_client():
    return storage.Client(project=BQ_PROJECT or None)


@lru_cache(maxsize=1)
def get_bigquery_client():
    return bigquery.Client(project=BQ_PROJECT or None)


# --------------------------
# Normalização de colunas
# --------------------------
def strip_accents(s: str) -> str:
    s = "" if s is None else str(s)
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def normalize_column_name(raw: str) -> str:
    s = strip_accents(raw).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return ""
    if re.match(r"^[0-9]", s):
        s = f"col_{s}"
    return s[:300]


def dedupe_columns(cols: List[str]) -> List[str]:
    out = []
    seen: Dict[str, int] = {}
    empty_count = 0
    for c in cols:
        base = c or ""
        if not base:
            empty_count += 1
            base = f"col_{empty_count}"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def normalize_columns(original_cols: List[str]) -> List[str]:
    normalized = [normalize_column_name(c) for c in original_cols]
    normalized = dedupe_columns(normalized)

    fixed = []
    for c in normalized:
        if not c:
            fixed.append("col")
        elif not re.match(r"^[a-z_][a-z0-9_]*$", c):
            c2 = re.sub(r"[^a-z0-9_]+", "_", c)
            if re.match(r"^[0-9]", c2):
                c2 = f"col_{c2}"
            fixed.append(c2[:300])
        else:
            fixed.append(c)

    return dedupe_columns(fixed)


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    def is_empty_col(s: pd.Series) -> bool:
        if s.isna().all():
            return True
        if s.dtype == object:
            return s.fillna("").astype(str).str.strip().eq("").all()
        return False

    keep = [c for c in df.columns if not is_empty_col(df[c])]
    return df[keep]


def parse_year_month_from_filename(filename: str) -> Tuple[int, int]:
    """
    Espera: lamina_2024_abril_fechamento.xlsx
    """
    name = strip_accents(filename.lower())

    m_year = re.search(r"(19\d{2}|20\d{2})", name)
    if not m_year:
        raise ValueError(f"Não achei ano no filename: {filename}")
    year = int(m_year.group(1))

    for k, v in PT_MONTHS.items():
        if re.search(rf"\b{k}\b", name):
            return year, v

    m_num = re.search(r"(?:_|-)(0?[1-9]|1[0-2])(?:_|-)", name)
    if m_num:
        return year, int(m_num.group(1))

    raise ValueError(f"Não achei mês (nome pt ou número) no filename: {filename}")


def read_xlsx_to_df(xlsx_bytes: bytes) -> pd.DataFrame:
    sheet_arg = int(SHEET) if str(SHEET).isdigit() else SHEET
    return pd.read_excel(
        BytesIO(xlsx_bytes), sheet_name=sheet_arg, header=HEADER_ROW, engine="openpyxl"
    )


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = BytesIO()
    pq.write_table(table, out, compression="snappy")
    return out.getvalue()


# --------------------------
# BQ: garantir tabela particionada
# --------------------------
@retry_on_failure
def ensure_partitioned_table():
    bq = get_bigquery_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        tbl = bq.get_table(table_id)
        return tbl
    except NotFound:
        schema = [
            bigquery.SchemaField("ano", "INT64"),
            bigquery.SchemaField("mes", "INT64"),
            bigquery.SchemaField("competencia", "DATE"),
            bigquery.SchemaField("arquivo_origem", "STRING"),
            bigquery.SchemaField("gcs_uri", "STRING"),
            bigquery.SchemaField("data_ingestao", "TIMESTAMP"),
        ]
        tbl = bigquery.Table(table_id, schema=schema)
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="competencia",
        )
        created = bq.create_table(tbl)
        log_event(
            "Tabela criada (particionada por competencia)", "INFO", destination=table_id
        )
        return created


@retry_on_failure
def load_parquet_to_bq(source_uri: str, destination: str, write_disposition: str):
    bq = get_bigquery_client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
    )
    job = bq.load_table_from_uri(source_uri, destination, job_config=job_config)
    job.result()
    return True


# --------------------------
# Location guard (bucket vs dataset)
# --------------------------
@retry_on_failure
def check_location_compat():
    st = get_storage_client()
    bq = get_bigquery_client()

    bucket = st.get_bucket(GCS_BUCKET)
    bucket_loc = (bucket.location or "").upper()

    ds_id = f"{BQ_PROJECT}.{BQ_DATASET}"
    dataset = bq.get_dataset(ds_id)
    dataset_loc = (dataset.location or "").upper()

    log_event(
        "Locations detectadas",
        "INFO",
        bucket_location=bucket_loc,
        dataset_location=dataset_loc,
    )

    if dataset_loc in ("US", "EU"):
        if bucket_loc != dataset_loc:
            raise RuntimeError(
                f"Incompatibilidade: dataset={dataset_loc} vs bucket={bucket_loc}"
            )
    else:
        if bucket_loc != dataset_loc:
            raise RuntimeError(
                f"Incompatibilidade: dataset={dataset_loc} vs bucket={bucket_loc}"
            )


# --------------------------
# GCS: list / download / upload / cleanup
# --------------------------
@retry_on_failure
def list_target_blobs() -> List[storage.Blob]:
    st = get_storage_client()
    blobs = st.list_blobs(GCS_BUCKET, prefix=GCS_PATH_PREFIX or None)

    selected: List[storage.Blob] = []
    for blob in blobs:
        base = blob.name.split("/")[-1]

        if base.startswith("~$"):
            continue
        if not base.endswith(".xlsx"):
            continue
        if FILE_PREFIX and not base.startswith(FILE_PREFIX):
            continue
        if FILE_SUFFIX and not base.endswith(FILE_SUFFIX):
            continue

        selected.append(blob)

    selected.sort(key=lambda b: b.name)
    if MAX_FILES > 0:
        selected = selected[:MAX_FILES]
    return selected


@retry_on_failure
def download_blob_bytes(blob: storage.Blob) -> bytes:
    return blob.download_as_bytes()


@retry_on_failure
def upload_bytes(bucket: storage.Bucket, path: str, data: bytes):
    b = bucket.blob(path)
    b.upload_from_string(data, content_type="application/octet-stream")
    return b


@retry_on_failure
def delete_blob(bucket: storage.Bucket, path: str):
    bucket.blob(path).delete()
    return True


# --------------------------
# Main
# --------------------------
def main():
    if not BQ_PROJECT:
        raise RuntimeError("Defina BQ_PROJECT ou GOOGLE_CLOUD_PROJECT.")

    with StepTimer("job_boot", project=BQ_PROJECT, dataset=BQ_DATASET, table=BQ_TABLE):
        log_event(
            "Config carregada",
            "INFO",
            gcs_bucket=GCS_BUCKET,
            gcs_path_prefix=GCS_PATH_PREFIX,
            file_prefix=FILE_PREFIX,
            file_suffix=FILE_SUFFIX,
            sheet=SHEET,
            header_row=HEADER_ROW,
            staging_prefix=STAGING_PREFIX,
            load_mode=LOAD_MODE,
            dry_run=DRY_RUN,
            force_string=FORCE_STRING,
            drop_empty_columns=DROP_EMPTY_COLUMNS,
            max_files=MAX_FILES,
        )

    # Location compat
    with StepTimer("check_location"):
        check_location_compat()

    # Ensure table
    if LOAD_MODE == "overwrite_partition":
        with StepTimer(
            "ensure_partitioned_table",
            destination=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        ):
            ensure_partitioned_table()

    # List files
    with StepTimer("list_files"):
        blobs = list_target_blobs()
        log_event("Arquivos selecionados", "INFO", total_files=len(blobs))

    if not blobs:
        log_event("Nenhum arquivo XLSX encontrado com o padrão informado", "WARNING")
        return

    st = get_storage_client()
    bucket = st.bucket(GCS_BUCKET)

    base_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    processed = 0
    failed = 0
    skipped = 0
    total_rows_loaded = 0

    for idx, blob in enumerate(blobs, start=1):
        filename = blob.name.split("/")[-1]
        gcs_uri = f"gs://{GCS_BUCKET}/{blob.name}"

        with StepTimer(
            "process_file", file=filename, gcs_uri=gcs_uri, index=idx, total=len(blobs)
        ):
            try:
                # parse competencia
                year, month = parse_year_month_from_filename(filename)
                competencia_dt = date(year, month, 1)
                partition_id = competencia_dt.strftime("%Y%m%d")

                log_event(
                    "Arquivo identificado",
                    "INFO",
                    file=filename,
                    year=year,
                    month=month,
                    competencia=str(competencia_dt),
                )

                # download
                with StepTimer("download_xlsx", file=filename):
                    xlsx_bytes = download_blob_bytes(blob)
                    log_event(
                        "Download OK", "INFO", file=filename, bytes=len(xlsx_bytes)
                    )

                # read excel
                with StepTimer(
                    "read_excel", file=filename, sheet=SHEET, header_row=HEADER_ROW
                ):
                    df = read_xlsx_to_df(xlsx_bytes)

                if df is None or df.empty:
                    skipped += 1
                    log_event("Arquivo sem dados (empty)", "WARNING", file=filename)
                    continue

                # cleanup empty cols
                if DROP_EMPTY_COLUMNS:
                    before_cols = len(df.columns)
                    df = drop_empty_columns(df)
                    after_cols = len(df.columns)
                    if after_cols != before_cols:
                        log_event(
                            "Colunas vazias removidas",
                            "INFO",
                            file=filename,
                            before_cols=before_cols,
                            after_cols=after_cols,
                        )

                # normalize columns
                original_cols = [str(c) for c in df.columns.tolist()]
                df.columns = normalize_columns(original_cols)

                log_event(
                    "Excel lido e colunas normalizadas",
                    "INFO",
                    file=filename,
                    rows=len(df),
                    cols=len(df.columns),
                    cols_sample=df.columns.tolist()[:15],
                )

                # force string
                if FORCE_STRING:
                    with StepTimer("force_string", file=filename):
                        for c in df.columns:
                            df[c] = df[c].map(
                                lambda x: None if pd.isna(x) else str(x).strip()
                            )

                # metadata
                df["ano"] = int(year)
                df["mes"] = int(month)
                df["competencia"] = pd.to_datetime(competencia_dt)
                df["arquivo_origem"] = filename
                df["gcs_uri"] = gcs_uri
                df["data_ingestao"] = pd.Timestamp(datetime.now(timezone.utc))

                if DRY_RUN:
                    processed += 1
                    log_event(
                        "DRY_RUN: pronto para parquet/load",
                        "INFO",
                        file=filename,
                        rows=len(df),
                        cols=len(df.columns),
                    )
                    continue

                # parquet
                with StepTimer("to_parquet", file=filename):
                    parquet_bytes = dataframe_to_parquet_bytes(df)
                    log_event(
                        "Parquet gerado",
                        "INFO",
                        file=filename,
                        parquet_bytes=len(parquet_bytes),
                    )

                # staging upload
                safe_base = re.sub(r"[^a-zA-Z0-9_.-]+", "_", filename).replace(
                    ".xlsx", ".parquet"
                )
                staging_path = f"{STAGING_PREFIX}ano={year}/mes={month:02d}/{safe_base}"
                staging_uri = f"gs://{GCS_BUCKET}/{staging_path}"

                with StepTimer(
                    "upload_staging", file=filename, staging_uri=staging_uri
                ):
                    upload_bytes(bucket, staging_path, parquet_bytes)
                    log_event(
                        "Staging OK", "INFO", file=filename, staging_uri=staging_uri
                    )

                # load BQ
                if LOAD_MODE == "overwrite_partition":
                    destination = f"{base_table_id}${partition_id}"
                    write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE
                elif LOAD_MODE == "append":
                    destination = base_table_id
                    write_disp = bigquery.WriteDisposition.WRITE_APPEND
                elif LOAD_MODE == "overwrite_table":
                    destination = base_table_id
                    write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE
                else:
                    raise ValueError(
                        "LOAD_MODE inválido. Use overwrite_partition|append|overwrite_table"
                    )

                with StepTimer("load_bigquery", file=filename, destination=destination):
                    load_parquet_to_bq(staging_uri, destination, write_disp)
                    log_event(
                        "Load BigQuery OK",
                        "INFO",
                        file=filename,
                        destination=destination,
                    )

                if CLEANUP_STAGING:
                    with StepTimer(
                        "cleanup_staging", file=filename, staging_uri=staging_uri
                    ):
                        delete_blob(bucket, staging_path)
                        log_event(
                            "Staging removido",
                            "INFO",
                            file=filename,
                            staging_uri=staging_uri,
                        )

                processed += 1
                total_rows_loaded += len(df)

            except (BadRequest, ValueError, RuntimeError) as e:
                failed += 1
                log_event(
                    "Falha ao processar arquivo",
                    "ERROR",
                    file=filename,
                    gcs_uri=gcs_uri,
                    error=str(e),
                    traceback=traceback.format_exc(),
                )
                if FAIL_ON_FILE_ERROR:
                    raise
            except Exception as e:
                failed += 1
                log_event(
                    "Erro inesperado ao processar arquivo",
                    "ERROR",
                    file=filename,
                    gcs_uri=gcs_uri,
                    error=str(e),
                    traceback=traceback.format_exc(),
                )
                if FAIL_ON_FILE_ERROR:
                    raise

    log_event(
        "Job finalizado",
        "INFO" if failed == 0 else "WARNING",
        ok=processed,
        failed=failed,
        skipped=skipped,
        total_files=len(blobs),
        total_rows_loaded=total_rows_loaded,
        dry_run=DRY_RUN,
        load_mode=LOAD_MODE,
        destination_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
    )

    if failed > 0 and FAIL_ON_FILE_ERROR:
        raise RuntimeError(f"Falhas detectadas: {failed}")


if __name__ == "__main__":
    main()
