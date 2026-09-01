# Databricks notebook source
catalog_name = spark.sql(
    "SELECT current_catalog()"
).first()[0]

bronze_schema = "iphone_health_bronze"
landing_volume = "landing"

landing_path = (
    f"/Volumes/{catalog_name}/"
    f"{bronze_schema}/{landing_volume}"
)

print(f"Catalog: {catalog_name}")
print(f"Landing path: {landing_path}")

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

from pathlib import Path
import hashlib
import shutil
import zipfile

zip_path = Path(
    landing_path,
    "apple_health_protected_export.zip"
)

if not zip_path.exists():
    raise FileNotFoundError(
        f"Protected export was not found: {zip_path}"
    )


def calculate_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


source_sha256 = calculate_sha256(zip_path)
ingestion_id = source_sha256[:16]

extraction_directory = Path(
    landing_path,
    "extracted",
    ingestion_id
)

extraction_directory.mkdir(
    parents=True,
    exist_ok=True
)

xml_path = extraction_directory / "export.xml"

with zipfile.ZipFile(zip_path, "r") as archive:
    invalid_member = archive.testzip()

    if invalid_member is not None:
        raise ValueError(
            f"ZIP validation failed for: {invalid_member}"
        )

    export_members = [
        name
        for name in archive.namelist()
        if name == "export.xml"
        or name.endswith("/export.xml")
    ]

    if len(export_members) != 1:
        raise ValueError(
            "Expected exactly one export.xml; "
            f"found {len(export_members)}"
        )

    with archive.open(export_members[0], "r") as source:
        with xml_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)


print("ZIP validation: PASSED")
print(f"Ingestion ID: {ingestion_id}")
print(f"Extracted XML: {xml_path}")
print(f"XML size: {xml_path.stat().st_size:,} bytes")

# COMMAND ----------

display(
    dbutils.fs.ls(
        str(extraction_directory)
    )
)

# COMMAND ----------

input_path = f"{landing_path}/extracted"

schema_path = (
    f"{landing_path}/_schemas/"
    "health_records_bronze"
)

checkpoint_path = (
    f"{landing_path}/_checkpoints/"
    "health_records_bronze"
)

bronze_table = (
    f"`{catalog_name}`."
    "`iphone_health_bronze`."
    "`health_records_raw`"
)

print(f"XML source: {input_path}")
print(f"Schema tracking: {schema_path}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target table: {bronze_table}")

# COMMAND ----------

raw_xml_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "xml")
    .option("rowTag", "Record")
    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )
    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )
    .option(
        "rescuedDataColumn",
        "_rescued_data"
    )
    .load(input_path)
)

# COMMAND ----------

raw_xml_stream.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F


bronze_stream = (
    raw_xml_stream
    .select(
        F.col("_type").alias("record_type"),
        F.col("_sourceName").alias("source_name"),
        F.col("_sourceVersion").alias(
            "source_version"
        ),
        F.col("_creationDate").alias(
            "creation_date_raw"
        ),
        F.col("_startDate").alias(
            "start_date_raw"
        ),
        F.col("_endDate").alias(
            "end_date_raw"
        ),
        F.col("_value").alias("value_raw"),
        F.col("_unit").alias("unit_raw"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias(
            "_source_file"
        ),
        F.col("_metadata.file_size").alias(
            "_source_file_size"
        ),
        F.col(
            "_metadata.file_modification_time"
        ).alias("_source_file_modified_at")
    )
    .withColumn(
        "_ingestion_id",
        F.regexp_extract(
            F.col("_source_file"),
            r"/extracted/([^/]+)/",
            1
        )
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

hash_columns = [
    "record_type",
    "source_name",
    "source_version",
    "creation_date_raw",
    "start_date_raw",
    "end_date_raw",
    "value_raw",
    "unit_raw"
]

bronze_stream = bronze_stream.withColumn(
    "_record_hash",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column),
                    F.lit("<null>")
                )
                for column in hash_columns
            ]
        ),
        256
    )
)

# COMMAND ----------

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

bronze_query.awaitTermination()

print("Bronze ingestion completed.")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

quality_summary = (
    bronze_df
    .agg(
        F.count("*").alias("total_records"),
        F.countDistinct("_record_hash").alias(
            "distinct_record_hashes"
        ),
        F.sum(
            F.when(
                F.col("_rescued_data").isNotNull(),
                1
            ).otherwise(0)
        ).alias("rescued_records")
    )
)

display(quality_summary)


# COMMAND ----------

display(
    bronze_df
    .groupBy("record_type")
    .count()
    .orderBy(F.desc("count"))
)