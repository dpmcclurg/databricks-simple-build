"""Ingestion: Auto Loader over the drop folder, a file ledger, typed rows, and the lookup.

Each CSV is read as one raw row (binaryFile), so nothing is inferred: a parser
with a column list in code turns the bytes into rows, and reports problems in
parse_error instead of failing the update.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F, types as T

INCOMING = spark.conf.get("simple_build.incoming")
LOOKUP = spark.conf.get("simple_build.lookup")
MAX_BYTES = 10 * 1024 * 1024
COLUMNS = ["customer_id", "adjustment_total", "month"]

PARSED = T.StructType([
    T.StructField("rows", T.ArrayType(T.MapType(T.StringType(), T.StringType()))),
    T.StructField("parse_error", T.StringType()),
])


@F.udf(returnType=PARSED)
def parse_file(content):
    """Parse one CSV file into string rows. Never raises: returns parse_error instead."""
    import csv
    import io
    from decimal import Decimal

    try:
        reader = csv.DictReader(io.StringIO(bytes(content).decode("utf-8-sig")))
        header = [c.strip() for c in (reader.fieldnames or [])]
        if header != COLUMNS:
            return {"rows": [], "parse_error": f"unexpected header: {header}"}
        rows = []
        for n, r in enumerate(reader, start=2):
            row = {k.strip(): (v or "").strip() for k, v in r.items() if k}
            if not any(row.values()):
                continue  # blank line
            int(row["customer_id"])
            Decimal(row["adjustment_total"])
            if not 1 <= int(row["month"]) <= 12:
                raise ValueError(f"line {n}: month out of range")
            rows.append(row)
        return {"rows": rows, "parse_error": None}
    except Exception as e:  # report it, don't fail the update
        return {"rows": [], "parse_error": str(e)[:500]}


@dp.table(
    name="raw_adjustment_files",
    comment="One row per ingested file version (Auto Loader, binaryFile). A full refresh can't reset it.",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dp.expect("bounded_file", f"length > 0 AND length <= {MAX_BYTES}")
def raw_adjustment_files():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.allowOverwrites", "true")
        .load(INCOMING)
        .withColumn("content_sha256", F.sha2(F.col("content"), 256))
        .withColumn("file_id", F.sha2(F.concat_ws("|", "path", "content_sha256"), 256))
        .withColumn("acquired_at", F.current_timestamp())
    )


def _latest_versions():
    """The latest version of each file path, parsed."""
    w = Window.partitionBy("path").orderBy(F.col("modificationTime").desc(), F.col("acquired_at").desc())
    return (
        spark.read.table("raw_adjustment_files")
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("p", parse_file("content"))
        .withColumn("source_file_name", F.element_at(F.split("path", "/"), -1))
    )


@dp.materialized_view(name="adjustment_files", comment="File ledger: one row per file, with its row count and any parse error.")
@dp.expect("parsed", "parse_error IS NULL")
def adjustment_files():
    return _latest_versions().select(
        "file_id",
        "path",
        "source_file_name",
        "content_sha256",
        "acquired_at",
        F.col("modificationTime").alias("source_modified_at"),
        F.size("p.rows").alias("row_count"),
        F.col("p.parse_error").alias("parse_error"),
    )


@dp.materialized_view(name="adjustments_parsed", comment="Typed rows from files that parsed cleanly.")
def adjustments_parsed():
    f = _latest_versions().filter("p.parse_error IS NULL")
    r = f.select("path", "file_id", "source_file_name", "modificationTime", F.explode("p.rows").alias("r"))
    return r.select(
        F.col("r")["customer_id"].cast("int").alias("customer_id"),
        F.col("r")["adjustment_total"].cast("decimal(19,4)").alias("adjustment_total"),
        F.col("r")["month"].cast("int").alias("month"),
        F.col("path").alias("source_file"),
        "source_file_name",
        F.col("modificationTime").alias("source_modified_at"),
        "file_id",
    )


@dp.materialized_view(name="customer_revenue_type", comment="Customer to revenue type lookup.")
def customer_revenue_type():
    return (
        spark.read.format("csv").option("header", "true").load(LOOKUP)
        .select(
            F.col("customer_id").cast("int"),
            F.col("revenue_type_code").cast("int"),
            F.col("revenue_type_name"),
        )
    )
