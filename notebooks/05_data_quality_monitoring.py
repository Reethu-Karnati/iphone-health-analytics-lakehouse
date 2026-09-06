# Databricks notebook source

# Version information
project_version = "1.1"

# Existing Version 1 source tables
bronze_table = (
    "iphone_health_bronze.health_records_raw"
)

silver_table = (
    "iphone_health_silver.health_records_clean"
)

gold_daily_table = (
    "iphone_health_gold.daily_health_summary"
)

# New monitoring destination
quality_results_table = (
    "iphone_health_monitoring.data_quality_results"
)

print("Project version:", project_version)
print("Bronze source:", bronze_table)
print("Silver source:", silver_table)
print("Gold source:", gold_daily_table)
print("Quality results:", quality_results_table)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quality_results_table} (
        check_run_id STRING,
        project_version STRING,
        check_id STRING,
        check_name STRING,
        data_layer STRING,
        object_name STRING,
        check_category STRING,
        actual_value STRING,
        expected_value STRING,
        check_status STRING,
        severity STRING,
        checked_at TIMESTAMP,
        details STRING
    )

    USING DELTA

    COMMENT 'Append-only results from automated iPhone Health data-quality checks'

    TBLPROPERTIES (
        'data_domain' = 'personal_health',
        'table_purpose' = 'data_quality_monitoring',
        'contains_health_measurements' = 'false'
    )
""")

print(
    "Data-quality results table is ready:",
    quality_results_table
)

# COMMAND ----------

import uuid

from pyspark.sql import functions as F


# Create a unique ID for this quality-check execution
check_run_id = str(uuid.uuid4())


# Count the records in the Bronze table
bronze_row_count = (
    spark.table(bronze_table)
    .count()
)


# Determine whether the check passed
check_status = (
    "PASS"
    if bronze_row_count > 0
    else "FAIL"
)


# Create one result record
bronze_nonempty_result = [
    (
        check_run_id,
        project_version,
        "BRONZE_001",
        "Bronze table is not empty",
        "bronze",
        bronze_table,
        "row_count",
        str(bronze_row_count),
        "greater than 0",
        check_status,
        "ERROR",
        (
            "Bronze must contain records before "
            "Silver processing can begin."
        )
    )
]


result_columns = [
    "check_run_id",
    "project_version",
    "check_id",
    "check_name",
    "data_layer",
    "object_name",
    "check_category",
    "actual_value",
    "expected_value",
    "check_status",
    "severity",
    "details"
]


# Convert the Python result into a Spark DataFrame
result_df = (
    spark.createDataFrame(
        bronze_nonempty_result,
        result_columns
    )
    .withColumn(
        "checked_at",
        F.current_timestamp()
    )
    .select(
        "check_run_id",
        "project_version",
        "check_id",
        "check_name",
        "data_layer",
        "object_name",
        "check_category",
        "actual_value",
        "expected_value",
        "check_status",
        "severity",
        "checked_at",
        "details"
    )
)


# Append the result to the monitoring table
(
    result_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(quality_results_table)
)


print(
    "Bronze non-empty check:",
    check_status
)

print(
    "Bronze row count:",
    bronze_row_count
)

# COMMAND ----------

def log_quality_result(
    run_id,
    check_id,
    check_name,
    data_layer,
    object_name,
    check_category,
    actual_value,
    expected_value,
    check_status,
    severity,
    details
):
    """
    Append one data-quality result to the
    monitoring Delta table.
    """

    result_record = [
        (
            run_id,
            project_version,
            check_id,
            check_name,
            data_layer,
            object_name,
            check_category,
            str(actual_value),
            str(expected_value),
            check_status,
            severity,
            details
        )
    ]

    result_df = (
        spark.createDataFrame(
            result_record,
            result_columns
        )
        .withColumn(
            "checked_at",
            F.current_timestamp()
        )
        .select(
            "check_run_id",
            "project_version",
            "check_id",
            "check_name",
            "data_layer",
            "object_name",
            "check_category",
            "actual_value",
            "expected_value",
            "check_status",
            "severity",
            "checked_at",
            "details"
        )
    )

    (
        result_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(quality_results_table)
    )

# COMMAND ----------

print("Quality-result logging function is ready.")

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES
        IN iphone_health_quarantine
    """)
)

# COMMAND ----------

quarantine_table = (
    "iphone_health_quarantine.invalid_health_records"
)


# Count unique logical records in Bronze
bronze_distinct_record_count = (
    spark.table(bronze_table)
    .select("_record_hash")
    .distinct()
    .count()
)


# Count valid Silver records
silver_row_count = (
    spark.table(silver_table)
    .count()
)


# Count rejected Quarantine records
quarantine_row_count = (
    spark.table(quarantine_table)
    .count()
)


# All processed records
processed_record_count = (
    silver_row_count
    + quarantine_row_count
)


# Compare source records with processed records
reconciliation_status = (
    "PASS"
    if processed_record_count
    == bronze_distinct_record_count
    else "FAIL"
)


# Save the result
log_quality_result(
    run_id=check_run_id,
    check_id="PIPELINE_001",
    check_name=(
        "Bronze records reconcile with "
        "Silver and Quarantine"
    ),
    data_layer="cross_layer",
    object_name=(
        f"{bronze_table} -> "
        f"{silver_table} + "
        f"{quarantine_table}"
    ),
    check_category="record_reconciliation",
    actual_value=(
        f"bronze_distinct="
        f"{bronze_distinct_record_count}; "
        f"silver={silver_row_count}; "
        f"quarantine={quarantine_row_count}; "
        f"processed={processed_record_count}"
    ),
    expected_value=(
        "processed equals distinct Bronze records"
    ),
    check_status=reconciliation_status,
    severity="ERROR",
    details=(
        "Every distinct Bronze record must be "
        "classified as either valid Silver data "
        "or an invalid Quarantine record."
    )
)


print(
    "Bronze-to-Silver reconciliation:",
    reconciliation_status
)

print(
    "Distinct Bronze records:",
    bronze_distinct_record_count
)

print(
    "Silver records:",
    silver_row_count
)

print(
    "Quarantine records:",
    quarantine_row_count
)

print(
    "Total processed records:",
    processed_record_count
)

# COMMAND ----------

# Calculate total and unique Silver records
silver_uniqueness = (
    spark.table(silver_table)
    .agg(
        F.count("*").alias("total_records"),
        F.countDistinct("_record_hash").alias(
            "unique_record_hashes"
        )
    )
    .first()
)


silver_total_records = (
    silver_uniqueness["total_records"]
)

silver_unique_hashes = (
    silver_uniqueness["unique_record_hashes"]
)

silver_duplicate_count = (
    silver_total_records
    - silver_unique_hashes
)


silver_uniqueness_status = (
    "PASS"
    if silver_duplicate_count == 0
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="SILVER_001",
    check_name=(
        "Silver record hashes are unique"
    ),
    data_layer="silver",
    object_name=silver_table,
    check_category="uniqueness",
    actual_value=(
        f"total={silver_total_records}; "
        f"unique={silver_unique_hashes}; "
        f"duplicates={silver_duplicate_count}"
    ),
    expected_value="duplicate count equals 0",
    check_status=silver_uniqueness_status,
    severity="ERROR",
    details=(
        "Each logical Apple Health record must "
        "appear only once in the Silver table."
    )
)


print(
    "Silver uniqueness check:",
    silver_uniqueness_status
)

print(
    "Total Silver records:",
    silver_total_records
)

print(
    "Unique record hashes:",
    silver_unique_hashes
)

print(
    "Duplicate records:",
    silver_duplicate_count
)

# COMMAND ----------

silver_columns = (
    spark.table(silver_table)
    .columns
)

print("Silver column count:", len(silver_columns))
print()

for column_name in silver_columns:
    print(column_name)

# COMMAND ----------

critical_silver_columns = [
    "_record_hash",
    "metric_type",
    "event_date"
]


# Confirm the expected columns exist
missing_schema_columns = [
    column_name
    for column_name in critical_silver_columns
    if column_name not in silver_columns
]


if missing_schema_columns:
    raise ValueError(
        "Required Silver columns are missing: "
        f"{missing_schema_columns}"
    )


# Count NULL values in each critical column
null_count_row = (
    spark.table(silver_table)
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    1
                ).otherwise(0)
            ).alias(column_name)
            for column_name
            in critical_silver_columns
        ]
    )
    .first()
)


null_counts = {
    column_name: null_count_row[column_name]
    for column_name in critical_silver_columns
}


total_critical_nulls = sum(
    null_counts.values()
)


critical_null_status = (
    "PASS"
    if total_critical_nulls == 0
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="SILVER_002",
    check_name=(
        "Critical Silver columns contain no NULLs"
    ),
    data_layer="silver",
    object_name=silver_table,
    check_category="completeness",
    actual_value=str(null_counts),
    expected_value=(
        "0 NULL values in every critical column"
    ),
    check_status=critical_null_status,
    severity="ERROR",
    details=(
        "Record hash, metric type and event date "
        "are required for deduplication, metric "
        "processing and daily aggregation."
    )
)


print(
    "Critical Silver NULL check:",
    critical_null_status
)

print(
    "NULL counts:",
    null_counts
)

# COMMAND ----------

optional_null_profile_df = (
    spark.table(silver_table)

    .groupBy("metric_type")

    .agg(
        F.count("*").alias(
            "total_records"
        ),

        F.sum(
            F.when(
                F.col("numeric_value").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "null_numeric_value_count"
        ),

        F.sum(
            F.when(
                F.col("canonical_unit").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "null_canonical_unit_count"
        )
    )

    .filter(
        (
            F.col("null_numeric_value_count") > 0
        )
        |
        (
            F.col("null_canonical_unit_count") > 0
        )
    )

    .orderBy(
        F.desc("total_records")
    )
)


display(optional_null_profile_df)

# COMMAND ----------

allowed_non_numeric_metrics = [
    "SleepAnalysis",
    "HeadphoneAudioExposureEvent"
]


optional_null_condition = (
    F.col("numeric_value").isNull()
    |
    F.col("canonical_unit").isNull()
)


optional_null_summary = (
    spark.table(silver_table)

    .agg(
        F.sum(
            F.when(
                optional_null_condition
                &
                F.col("metric_type").isin(
                    allowed_non_numeric_metrics
                ),
                1
            ).otherwise(0)
        ).alias(
            "allowed_null_records"
        ),

        F.sum(
            F.when(
                optional_null_condition
                &
                ~F.col("metric_type").isin(
                    allowed_non_numeric_metrics
                ),
                1
            ).otherwise(0)
        ).alias(
            "unexpected_null_records"
        )
    )

    .first()
)


allowed_null_records = (
    optional_null_summary[
        "allowed_null_records"
    ]
)

unexpected_null_records = (
    optional_null_summary[
        "unexpected_null_records"
    ]
)


optional_null_status = (
    "PASS"
    if unexpected_null_records == 0
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="SILVER_003",
    check_name=(
        "Optional NULL values occur only "
        "in approved non-numeric metrics"
    ),
    data_layer="silver",
    object_name=silver_table,
    check_category="conditional_completeness",
    actual_value=(
        f"allowed_null_records="
        f"{allowed_null_records}; "
        f"unexpected_null_records="
        f"{unexpected_null_records}"
    ),
    expected_value=(
        "unexpected_null_records equals 0"
    ),
    check_status=optional_null_status,
    severity="ERROR",
    details=(
        "SleepAnalysis and "
        "HeadphoneAudioExposureEvent may have "
        "NULL numeric values and units because "
        "they represent categories or events."
    )
)


print(
    "Optional NULL validation:",
    optional_null_status
)

print(
    "Allowed NULL records:",
    allowed_null_records
)

print(
    "Unexpected NULL records:",
    unexpected_null_records
)

# COMMAND ----------

display(
    spark.table(quality_results_table)

    .filter(
        (F.col("check_run_id") == check_run_id)
        &
        (F.col("check_id") == "SILVER_003")
    )

    .select(
        "check_id",
        "check_name",
        "actual_value",
        "expected_value",
        "check_status",
        "severity",
        "checked_at",
        "details"
    )
)

# COMMAND ----------

gold_date_summary = (
    spark.table(gold_daily_table)

    .agg(
        F.count("*").alias(
            "total_rows"
        ),

        F.countDistinct(
            "protected_date"
        ).alias(
            "unique_dates"
        ),

        F.sum(
            F.when(
                F.col("protected_date").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "null_dates"
        )
    )

    .first()
)


gold_total_rows = (
    gold_date_summary["total_rows"]
)

gold_unique_dates = (
    gold_date_summary["unique_dates"]
)

gold_null_dates = (
    gold_date_summary["null_dates"]
)


gold_duplicate_date_rows = (
    gold_total_rows
    - gold_unique_dates
    - gold_null_dates
)


gold_date_uniqueness_status = (
    "PASS"
    if (
        gold_null_dates == 0
        and gold_duplicate_date_rows == 0
    )
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="GOLD_001",
    check_name=(
        "Gold contains one row per protected date"
    ),
    data_layer="gold",
    object_name=gold_daily_table,
    check_category="grain_uniqueness",
    actual_value=(
        f"total_rows={gold_total_rows}; "
        f"unique_dates={gold_unique_dates}; "
        f"null_dates={gold_null_dates}; "
        f"duplicate_date_rows="
        f"{gold_duplicate_date_rows}"
    ),
    expected_value=(
        "null_dates=0 and "
        "duplicate_date_rows=0"
    ),
    check_status=gold_date_uniqueness_status,
    severity="ERROR",
    details=(
        "The combined Gold table must maintain "
        "a one-row-per-protected-day grain."
    )
)


print(
    "Gold date uniqueness check:",
    gold_date_uniqueness_status
)

print(
    "Total Gold rows:",
    gold_total_rows
)

print(
    "Unique protected dates:",
    gold_unique_dates
)

print(
    "NULL protected dates:",
    gold_null_dates
)

print(
    "Duplicate date rows:",
    gold_duplicate_date_rows
)

# COMMAND ----------

gold_date_range = (
    spark.table(gold_daily_table)

    .agg(
        F.min("protected_date").alias(
            "earliest_date"
        ),

        F.max("protected_date").alias(
            "latest_date"
        ),

        F.countDistinct(
            "protected_date"
        ).alias(
            "available_date_count"
        )
    )

    .first()
)


earliest_gold_date = (
    gold_date_range["earliest_date"]
)

latest_gold_date = (
    gold_date_range["latest_date"]
)

available_gold_dates = (
    gold_date_range["available_date_count"]
)


expected_calendar_days = (
    latest_gold_date
    - earliest_gold_date
).days + 1


missing_calendar_days = (
    expected_calendar_days
    - available_gold_dates
)


gold_date_continuity_status = (
    "PASS"
    if missing_calendar_days == 0
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="GOLD_002",
    check_name=(
        "Gold protected-date range is continuous"
    ),
    data_layer="gold",
    object_name=gold_daily_table,
    check_category="date_continuity",
    actual_value=(
        f"earliest={earliest_gold_date}; "
        f"latest={latest_gold_date}; "
        f"available_dates="
        f"{available_gold_dates}; "
        f"expected_dates="
        f"{expected_calendar_days}; "
        f"missing_dates="
        f"{missing_calendar_days}"
    ),
    expected_value="missing_dates equals 0",
    check_status=gold_date_continuity_status,
    severity="ERROR",
    details=(
        "Every calendar date between the "
        "earliest and latest protected dates "
        "must be represented in combined Gold."
    )
)


print(
    "Gold date continuity check:",
    gold_date_continuity_status
)

print(
    "Earliest protected date:",
    earliest_gold_date
)

print(
    "Latest protected date:",
    latest_gold_date
)

print(
    "Expected calendar days:",
    expected_calendar_days
)

print(
    "Available protected dates:",
    available_gold_dates
)

print(
    "Missing calendar days:",
    missing_calendar_days
)

# COMMAND ----------

non_negative_gold_columns = [
    "total_steps",
    "walking_distance_km",
    "active_energy_kcal",
    "basal_energy_kcal",
    "flights_climbed"
]


negative_count_row = (
    spark.table(gold_daily_table)

    .agg(
        *[
            F.sum(
                F.when(
                    F.col(column_name) < 0,
                    1
                ).otherwise(0)
            ).alias(column_name)

            for column_name
            in non_negative_gold_columns
        ]
    )

    .first()
)


negative_counts = {
    column_name: negative_count_row[column_name]

    for column_name
    in non_negative_gold_columns
}


total_negative_values = sum(
    negative_counts.values()
)


non_negative_status = (
    "PASS"
    if total_negative_values == 0
    else "FAIL"
)


log_quality_result(
    run_id=check_run_id,
    check_id="GOLD_003",
    check_name=(
        "Gold activity metrics are non-negative"
    ),
    data_layer="gold",
    object_name=gold_daily_table,
    check_category="validity",
    actual_value=str(negative_counts),
    expected_value=(
        "0 negative values in every "
        "validated activity metric"
    ),
    check_status=non_negative_status,
    severity="ERROR",
    details=(
        "Steps, distance, energy and flights "
        "cannot contain negative daily values."
    )
)


print(
    "Gold non-negative metric check:",
    non_negative_status
)

print(
    "Negative-value counts:",
    negative_counts
)

# COMMAND ----------

current_run_results_df = (
    spark.table(quality_results_table)

    .filter(
        F.col("check_run_id")
        == check_run_id
    )
)


current_run_check_count = (
    current_run_results_df.count()
)

current_run_failed_count = (
    current_run_results_df

    .filter(
        F.col("check_status") == "FAIL"
    )

    .count()
)


print(
    "Quality checks recorded:",
    current_run_check_count
)

print(
    "Failed quality checks:",
    current_run_failed_count
)


display(
    current_run_results_df

    .select(
        "check_id",
        "check_name",
        "data_layer",
        "actual_value",
        "expected_value",
        "check_status",
        "severity",
        "checked_at"
    )

    .orderBy("check_id")
)

# COMMAND ----------

recorded_check_ids = [
    row["check_id"]

    for row in (
        spark.table(quality_results_table)

        .filter(
            F.col("check_run_id")
            == check_run_id
        )

        .select("check_id")

        .distinct()

        .orderBy("check_id")

        .collect()
    )
]


print(
    "Recorded check IDs:",
    recorded_check_ids
)


if "SILVER_002" in recorded_check_ids:
    print(
        "SILVER_002 is already recorded."
    )
else:
    print(
        "SILVER_002 is missing."
    )

# COMMAND ----------

quality_check_results_df = (
    spark.table(quality_results_table)

    .filter(
        F.col("check_run_id")
        == check_run_id
    )

    .filter(
        F.col("check_id")
        != "RUN_001"
    )
)


quality_run_summary = (
    quality_check_results_df

    .agg(
        F.count("*").alias(
            "total_checks"
        ),

        F.sum(
            F.when(
                F.col("check_status") == "PASS",
                1
            ).otherwise(0)
        ).alias(
            "passed_checks"
        ),

        F.sum(
            F.when(
                F.col("check_status") == "FAIL",
                1
            ).otherwise(0)
        ).alias(
            "failed_checks"
        ),

        F.sum(
            F.when(
                (
                    F.col("check_status") == "FAIL"
                )
                &
                (
                    F.col("severity") == "ERROR"
                ),
                1
            ).otherwise(0)
        ).alias(
            "error_failures"
        )
    )

    .first()
)


total_checks = (
    quality_run_summary["total_checks"]
)

passed_checks = (
    quality_run_summary["passed_checks"]
)

failed_checks = (
    quality_run_summary["failed_checks"]
)

error_failures = (
    quality_run_summary["error_failures"]
)


pipeline_quality_status = (
    "PASS"
    if error_failures == 0
    else "FAIL"
)


print(
    "Overall pipeline quality status:",
    pipeline_quality_status
)

print(
    "Total checks:",
    total_checks
)

print(
    "Passed checks:",
    passed_checks
)

print(
    "Failed checks:",
    failed_checks
)

print(
    "ERROR-level failures:",
    error_failures
)

# COMMAND ----------

existing_gate_count = (
    spark.table(quality_results_table)

    .filter(
        (
            F.col("check_run_id")
            == check_run_id
        )
        &
        (
            F.col("check_id")
            == "RUN_001"
        )
    )

    .count()
)


if existing_gate_count == 0:

    log_quality_result(
        run_id=check_run_id,
        check_id="RUN_001",
        check_name=(
            "Overall pipeline quality gate"
        ),
        data_layer="all_layers",
        object_name=(
            "Bronze, Silver, Quarantine and Gold"
        ),
        check_category="quality_gate",
        actual_value=(
            f"total_checks={total_checks}; "
            f"passed_checks={passed_checks}; "
            f"failed_checks={failed_checks}; "
            f"error_failures={error_failures}"
        ),
        expected_value=(
            "error_failures equals 0"
        ),
        check_status=pipeline_quality_status,
        severity="ERROR",
        details=(
            "The pipeline can continue only when "
            "no ERROR-level quality checks fail."
        )
    )

    print(
        "Quality-gate result stored:",
        pipeline_quality_status
    )

else:
    print(
        "Quality-gate result already exists. "
        "No duplicate was added."
    )

# COMMAND ----------

if pipeline_quality_status != "PASS":

    raise RuntimeError(
        "Pipeline stopped because "
        f"{error_failures} ERROR-level "
        "data-quality check(s) failed. "
        f"Check run ID: {check_run_id}"
    )


print(
    "Quality gate passed."
)

print(
    "The pipeline is approved for "
    "downstream processing."
)

print(
    "Check run ID:",
    check_run_id
)
