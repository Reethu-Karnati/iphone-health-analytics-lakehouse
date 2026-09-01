# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)

catalog_name = spark.sql(
    "SELECT current_catalog()"
).first()[0]

bronze_table = (
    f"`{catalog_name}`."
    "`iphone_health_bronze`."
    "`health_records_raw`"
)

silver_table = (
    f"{catalog_name}."
    "iphone_health_silver."
    "health_records_clean"
)

quarantine_table = (
    f"{catalog_name}."
    "iphone_health_quarantine."
    "invalid_health_records"
)

bronze_df = spark.table(bronze_table)

print(f"Bronze count: {bronze_df.count():,}")

# COMMAND ----------

deduplication_window = (
    Window
    .partitionBy("_record_hash")
    .orderBy(F.desc("_ingested_at"))
)

deduplicated_df = (
    bronze_df
    .withColumn(
        "_deduplication_rank",
        F.row_number().over(
            deduplication_window
        )
    )
    .filter(
        F.col("_deduplication_rank") == 1
    )
    .drop("_deduplication_rank")
)

print(
    "Records after deduplication: "
    f"{deduplicated_df.count():,}"
)

# COMMAND ----------

type_prefix_pattern = (
    "^(HKQuantityTypeIdentifier|"
    "HKCategoryTypeIdentifier|"
    "HKDataTypeIdentifier|"
    "HKDataType)"
)

base_silver_df = (
    deduplicated_df
    .withColumn(
        "metric_type",
        F.regexp_replace(
            F.col("record_type"),
            type_prefix_pattern,
            ""
        )
    )
    .withColumn(
        "creation_timestamp",
        F.to_timestamp(
            F.col("creation_date_raw"),
            "yyyy-MM-dd HH:mm:ss Z"
        )
    )
    .withColumn(
        "start_timestamp",
        F.to_timestamp(
            F.col("start_date_raw"),
            "yyyy-MM-dd HH:mm:ss Z"
        )
    )
    .withColumn(
        "end_timestamp",
        F.to_timestamp(
            F.col("end_date_raw"),
            "yyyy-MM-dd HH:mm:ss Z"
        )
    )
    .withColumn(
        "event_date",
        F.to_date("start_timestamp")
    )
    .withColumn(
        "duration_seconds",
        F.unix_timestamp("end_timestamp")
        - F.unix_timestamp("start_timestamp")
    )
    .withColumn(
        "numeric_value_raw",
        F.expr(
            "try_cast(value_raw AS DOUBLE)"
        )
    )
)

# COMMAND ----------

expected_units = {
    "ActiveEnergyBurned": "Cal",
    "AppleWalkingSteadiness": "%",
    "BasalEnergyBurned": "Cal",
    "DistanceWalkingRunning": "mi",
    "FlightsClimbed": "count",
    "HeadphoneAudioExposure": "dBASPL",
    "SleepDurationGoal": "hr",
    "StepCount": "count",
    "WalkingAsymmetryPercentage": "%",
    "WalkingDoubleSupportPercentage": "%",
    "WalkingSpeed": "mi/hr",
    "WalkingStepLength": "in"
}

categorical_types = [
    "SleepAnalysis",
    "HeadphoneAudioExposureEvent"
]

percentage_types = [
    "AppleWalkingSteadiness",
    "WalkingAsymmetryPercentage",
    "WalkingDoubleSupportPercentage"
]

allowed_metric_types = (
    list(expected_units.keys())
    + categorical_types
)

unit_map_expression = F.create_map(
    *[
        item
        for metric, unit
        in expected_units.items()
        for item in (
            F.lit(metric),
            F.lit(unit)
        )
    ]
)

base_silver_df = (
    base_silver_df
    .withColumn(
        "expected_unit_raw",
        unit_map_expression[
            F.col("metric_type")
        ]
    )
)

# COMMAND ----------

quality_checked_df = (
    base_silver_df
    .withColumn(
        "dq_reason",
        F.when(
            F.col("_rescued_data").isNotNull(),
            "unexpected_xml_fields"
        )
        .when(
            ~F.col("metric_type").isin(
                allowed_metric_types
            ),
            "unsupported_metric_type"
        )
        .when(
            F.col("start_timestamp").isNull()
            | F.col("end_timestamp").isNull(),
            "invalid_timestamp"
        )
        .when(
            F.col("end_timestamp")
            < F.col("start_timestamp"),
            "end_before_start"
        )
        .when(
            F.col("expected_unit_raw").isNotNull()
            & (
                F.col("unit_raw")
                != F.col("expected_unit_raw")
            ),
            "unexpected_unit"
        )
        .when(
            ~F.col("metric_type").isin(
                categorical_types
            )
            & F.col("numeric_value_raw").isNull(),
            "non_numeric_quantity"
        )
        .when(
            F.col("numeric_value_raw") < 0,
            "negative_numeric_value"
        )
        .when(
            F.col("metric_type").isin(
                percentage_types
            )
            & ~F.col("numeric_value_raw")
                .between(0.0, 1.0),
            "percentage_out_of_range"
        )
        .when(
            (
                F.col("metric_type")
                == "HeadphoneAudioExposure"
            )
            & (
                F.col("numeric_value_raw")
                > 120.0
            ),
            "audio_exposure_outlier"
        )
        .when(
            (
                F.col("metric_type")
                == "SleepAnalysis"
            )
            & (
                F.col("duration_seconds")
                > 86400
            ),
            "sleep_duration_over_24_hours"
        )
    )
    .withColumn(
        "dq_status",
        F.when(
            F.col("dq_reason").isNull(),
            "VALID"
        ).otherwise("QUARANTINED")
    )
)

# COMMAND ----------

valid_df = (
    quality_checked_df
    .filter(F.col("dq_status") == "VALID")
)

invalid_df = (
    quality_checked_df
    .filter(
        F.col("dq_status") == "QUARANTINED"
    )
)

print(f"Valid records: {valid_df.count():,}")
print(
    "Quarantined records: "
    f"{invalid_df.count():,}"
)

# COMMAND ----------

standardized_df = (
    valid_df
    .withColumn(
        "numeric_value",
        F.when(
            F.col("metric_type")
            == "DistanceWalkingRunning",
            F.col("numeric_value_raw")
            * F.lit(1.609344)
        )
        .when(
            F.col("metric_type")
            == "WalkingSpeed",
            F.col("numeric_value_raw")
            * F.lit(1.609344)
        )
        .when(
            F.col("metric_type")
            == "WalkingStepLength",
            F.col("numeric_value_raw")
            * F.lit(2.54)
        )
        .when(
            F.col("metric_type").isin(
                percentage_types
            ),
            F.col("numeric_value_raw")
            * F.lit(100.0)
        )
        .otherwise(
            F.col("numeric_value_raw")
        )
    )
    .withColumn(
        "canonical_unit",
        F.when(
            F.col("metric_type")
            == "DistanceWalkingRunning",
            "km"
        )
        .when(
            F.col("metric_type")
            == "WalkingSpeed",
            "km/h"
        )
        .when(
            F.col("metric_type")
            == "WalkingStepLength",
            "cm"
        )
        .when(
            F.col("metric_type").isin(
                percentage_types
            ),
            "percent"
        )
        .when(
            F.col("metric_type").isin(
                "ActiveEnergyBurned",
                "BasalEnergyBurned"
            ),
            "kcal"
        )
        .otherwise(
            F.col("unit_raw")
        )
    )
    .withColumn(
        "categorical_value",
        F.when(
            F.col("metric_type").isin(
                categorical_types
            ),
            F.regexp_replace(
                F.col("value_raw"),
                "^HKCategoryValue",
                ""
            )
        )
    )
    .withColumn(
        "duration_minutes",
        F.col("duration_seconds") / 60.0
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

silver_output_df = (
    standardized_df
    .select(
        "metric_type",
        F.col("record_type").alias(
            "original_record_type"
        ),
        "source_name",
        "source_version",
        "creation_timestamp",
        "start_timestamp",
        "end_timestamp",
        "event_date",
        "duration_seconds",
        "duration_minutes",
        "numeric_value",
        "canonical_unit",
        "categorical_value",
        "value_raw",
        "unit_raw",
        "dq_status",
        "_record_hash",
        "_ingestion_id",
        "_source_file",
        "_ingested_at",
        "_silver_processed_at"
    )
)

# COMMAND ----------

quarantine_output_df = (
    invalid_df
    .withColumn(
        "_quarantined_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

def merge_into_delta(
    source_df,
    target_table,
    merge_key="_record_hash"
):
    if not spark.catalog.tableExists(
        target_table
    ):
        (
            source_df
            .limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

    target_delta = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target_delta
        .alias("target")
        .merge(
            source_df.alias("source"),
            (
                f"target.{merge_key} "
                f"= source.{merge_key}"
            )
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

merge_into_delta(
    silver_output_df,
    silver_table
)

print("Silver MERGE completed.")

# COMMAND ----------

merge_into_delta(
    quarantine_output_df,
    quarantine_table
)

print("Quarantine MERGE completed.")

# COMMAND ----------

spark.sql(f"""
COMMENT ON TABLE {silver_table}
IS 'Cleaned, validated, deduplicated and standardized Apple Health records'
""")

spark.sql(f"""
ALTER TABLE {silver_table}
SET TBLPROPERTIES (
    'data_layer' = 'silver',
    'data_domain' = 'personal_health',
    'contains_direct_identifiers' = 'false'
)
""")

spark.sql(f"""
COMMENT ON TABLE {quarantine_table}
IS 'Apple Health records that failed Silver data-quality validation'
""")

# COMMAND ----------

bronze_count = bronze_df.count()

silver_count = (
    spark.table(silver_table).count()
)

quarantine_count = (
    spark.table(quarantine_table).count()
)

reconciliation_df = spark.createDataFrame(
    [
        ("Bronze", bronze_count),
        ("Silver", silver_count),
        ("Quarantine", quarantine_count)
    ],
    ["data_layer", "record_count"]
)

display(reconciliation_df)

# COMMAND ----------

display(
    spark.table(silver_table)
    .groupBy(
        "metric_type",
        "canonical_unit"
    )
    .agg(
        F.count("*").alias("record_count"),
        F.min("event_date").alias(
            "earliest_date"
        ),
        F.max("event_date").alias(
            "latest_date"
        )
    )
    .orderBy(
        F.desc("record_count")
    )
)
