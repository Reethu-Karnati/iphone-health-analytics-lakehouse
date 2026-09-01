# Databricks notebook source
catalog_name = spark.sql(
    "SELECT current_catalog()"
).first()[0]

silver_table = (
    f"`{catalog_name}`."
    "`iphone_health_silver`."
    "`health_records_clean`"
)

gold_activity_table = (
    f"`{catalog_name}`."
    "`iphone_health_gold`."
    "`daily_activity_summary`"
)

print(f"Source: {silver_table}")
print(f"Target: {gold_activity_table}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_activity_table}
COMMENT 'Daily activity metrics derived from cleaned Apple Health records'
TBLPROPERTIES (
    'data_layer' = 'gold',
    'data_domain' = 'personal_health',
    'aggregation_grain' = 'one_row_per_day',
    'contains_direct_identifiers' = 'false'
)
AS
SELECT
    event_date AS protected_event_date,
    TRUE AS date_is_shifted,

    CAST(
        SUM(
            CASE
                WHEN metric_type = 'StepCount'
                THEN numeric_value
                ELSE 0
            END
        ) AS BIGINT
    ) AS total_steps,

    ROUND(
        SUM(
            CASE
                WHEN metric_type =
                    'DistanceWalkingRunning'
                THEN numeric_value
                ELSE 0
            END
        ),
        3
    ) AS walking_distance_km,

    ROUND(
        SUM(
            CASE
                WHEN metric_type =
                    'ActiveEnergyBurned'
                THEN numeric_value
                ELSE 0
            END
        ),
        2
    ) AS active_energy_kcal,

    ROUND(
        SUM(
            CASE
                WHEN metric_type =
                    'BasalEnergyBurned'
                THEN numeric_value
                ELSE 0
            END
        ),
        2
    ) AS basal_energy_kcal,

    CAST(
        SUM(
            CASE
                WHEN metric_type =
                    'FlightsClimbed'
                THEN numeric_value
                ELSE 0
            END
        ) AS INT
    ) AS flights_climbed,

    COUNT(
        CASE
            WHEN metric_type = 'StepCount'
            THEN 1
        END
    ) AS step_measurement_count,

    MAX(_silver_processed_at) AS last_processed_at

FROM {silver_table}

WHERE metric_type IN (
    'StepCount',
    'DistanceWalkingRunning',
    'ActiveEnergyBurned',
    'BasalEnergyBurned',
    'FlightsClimbed'
)

GROUP BY event_date
""")

print("Gold daily activity table created.")

# COMMAND ----------

from pyspark.sql import functions as F

gold_activity_df = spark.table(
    gold_activity_table
)

print(
    "Gold activity days: "
    f"{gold_activity_df.count():,}"
)

display(
    gold_activity_df
    .orderBy(
        F.desc("protected_event_date")
    )
    .limit(20)
)

# COMMAND ----------

display(
    gold_activity_df
    .filter(
        (F.col("total_steps") < 0)
        | (F.col("walking_distance_km") < 0)
        | (F.col("active_energy_kcal") < 0)
        | (F.col("basal_energy_kcal") < 0)
        | (F.col("flights_climbed") < 0)
    )
)

# COMMAND ----------

gold_mobility_table = (
    f"`{catalog_name}`."
    "`iphone_health_gold`."
    "`daily_mobility_summary`"
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_mobility_table}
COMMENT 'Daily walking mobility metrics derived from cleaned Apple Health records'
TBLPROPERTIES (
    'data_layer' = 'gold',
    'data_domain' = 'personal_health',
    'aggregation_grain' = 'one_row_per_day',
    'contains_direct_identifiers' = 'false',
    'dates_are_privacy_shifted' = 'true'
)
AS

WITH mobility_records AS (
    SELECT
        event_date,
        metric_type,
        numeric_value,
        CASE
            WHEN duration_seconds > 0
            THEN duration_seconds
            ELSE 1
        END AS measurement_weight,
        _silver_processed_at

    FROM {silver_table}

    WHERE metric_type IN (
        'WalkingSpeed',
        'WalkingStepLength',
        'WalkingDoubleSupportPercentage',
        'WalkingAsymmetryPercentage',
        'AppleWalkingSteadiness'
    )
)

SELECT
    event_date AS protected_event_date,
    TRUE AS date_is_shifted,

    ROUND(
        SUM(
            CASE
                WHEN metric_type = 'WalkingSpeed'
                THEN numeric_value
                     * measurement_weight
            END
        )
        /
        NULLIF(
            SUM(
                CASE
                    WHEN metric_type = 'WalkingSpeed'
                    THEN measurement_weight
                END
            ),
            0
        ),
        3
    ) AS avg_walking_speed_kmh,

    ROUND(
        SUM(
            CASE
                WHEN metric_type = 'WalkingStepLength'
                THEN numeric_value
                     * measurement_weight
            END
        )
        /
        NULLIF(
            SUM(
                CASE
                    WHEN metric_type = 'WalkingStepLength'
                    THEN measurement_weight
                END
            ),
            0
        ),
        2
    ) AS avg_step_length_cm,

    ROUND(
        SUM(
            CASE
                WHEN metric_type =
                    'WalkingDoubleSupportPercentage'
                THEN numeric_value
                     * measurement_weight
            END
        )
        /
        NULLIF(
            SUM(
                CASE
                    WHEN metric_type =
                        'WalkingDoubleSupportPercentage'
                    THEN measurement_weight
                END
            ),
            0
        ),
        2
    ) AS avg_double_support_percent,

    ROUND(
        SUM(
            CASE
                WHEN metric_type =
                    'WalkingAsymmetryPercentage'
                THEN numeric_value
                     * measurement_weight
            END
        )
        /
        NULLIF(
            SUM(
                CASE
                    WHEN metric_type =
                        'WalkingAsymmetryPercentage'
                    THEN measurement_weight
                END
            ),
            0
        ),
        2
    ) AS avg_asymmetry_percent,

    ROUND(
        AVG(
            CASE
                WHEN metric_type =
                    'AppleWalkingSteadiness'
                THEN numeric_value
            END
        ),
        2
    ) AS avg_walking_steadiness_percent,

    COUNT(
        CASE
            WHEN metric_type = 'WalkingSpeed'
            THEN 1
        END
    ) AS walking_measurement_count,

    MAX(_silver_processed_at) AS last_processed_at

FROM mobility_records

GROUP BY event_date
""")

print("Gold daily mobility table created.")

# COMMAND ----------

gold_mobility_df = spark.table(
    gold_mobility_table
)

print(
    "Gold mobility days: "
    f"{gold_mobility_df.count():,}"
)

display(
    gold_mobility_df
    .orderBy(
        F.desc("protected_event_date")
    )
    .limit(20)
)

# COMMAND ----------

display(
    gold_mobility_df
    .filter(
        (F.col("avg_walking_speed_kmh") < 0)
        | (F.col("avg_step_length_cm") < 0)
        | (
            ~F.col("avg_double_support_percent")
            .between(0, 100)
        )
        | (
            ~F.col("avg_asymmetry_percent")
            .between(0, 100)
        )
        | (
            ~F.col("avg_walking_steadiness_percent")
            .between(0, 100)
        )
    )
)

# COMMAND ----------

gold_hearing_table = (
    f"`{catalog_name}`."
    "`iphone_health_gold`."
    "`daily_hearing_summary`"
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_hearing_table}
COMMENT 'Daily headphone audio exposure metrics from Apple Health'
TBLPROPERTIES (
    'data_layer' = 'gold',
    'data_domain' = 'personal_health',
    'aggregation_grain' = 'one_row_per_day',
    'contains_direct_identifiers' = 'false',
    'dates_are_privacy_shifted' = 'true'
)
AS

WITH exposure_records AS (
    SELECT
        event_date,
        numeric_value AS exposure_dbaspl,
        CASE
            WHEN duration_seconds > 0
            THEN duration_seconds
            ELSE 1
        END AS exposure_seconds,
        _silver_processed_at

    FROM {silver_table}

    WHERE metric_type =
        'HeadphoneAudioExposure'
),

daily_exposure AS (
    SELECT
        event_date,

        COUNT(*) AS exposure_measurement_count,

        ROUND(
            SUM(exposure_seconds) / 60.0,
            2
        ) AS measured_listening_minutes,

        ROUND(
            10 * LOG10(
                SUM(
                    POWER(
                        10.0,
                        exposure_dbaspl / 10.0
                    ) * exposure_seconds
                )
                /
                NULLIF(
                    SUM(exposure_seconds),
                    0
                )
            ),
            2
        ) AS equivalent_listening_level_dbaspl,

        ROUND(
            MAX(exposure_dbaspl),
            2
        ) AS maximum_exposure_dbaspl,

        MAX(_silver_processed_at)
            AS last_processed_at

    FROM exposure_records

    GROUP BY event_date
),

daily_events AS (
    SELECT
        event_date,
        COUNT(*) AS exposure_limit_event_count

    FROM {silver_table}

    WHERE metric_type =
        'HeadphoneAudioExposureEvent'

    GROUP BY event_date
),

available_dates AS (
    SELECT event_date FROM daily_exposure

    UNION

    SELECT event_date FROM daily_events
)

SELECT
    dates.event_date
        AS protected_event_date,

    TRUE AS date_is_shifted,

    COALESCE(
        exposure.exposure_measurement_count,
        0
    ) AS exposure_measurement_count,

    COALESCE(
        exposure.measured_listening_minutes,
        0
    ) AS measured_listening_minutes,

    exposure.equivalent_listening_level_dbaspl,

    exposure.maximum_exposure_dbaspl,

    COALESCE(
        events.exposure_limit_event_count,
        0
    ) AS exposure_limit_event_count,

    exposure.last_processed_at

FROM available_dates AS dates

LEFT JOIN daily_exposure AS exposure
    ON dates.event_date = exposure.event_date

LEFT JOIN daily_events AS events
    ON dates.event_date = events.event_date
""")

print("Gold daily hearing table created.")

# COMMAND ----------

gold_hearing_df = spark.table(
    gold_hearing_table
)

print(
    "Gold hearing days: "
    f"{gold_hearing_df.count():,}"
)

display(
    gold_hearing_df
    .orderBy(
        F.desc("protected_event_date")
    )
    .limit(20)
)

# COMMAND ----------

display(
    gold_hearing_df
    .filter(
        (
            F.col("measured_listening_minutes")
            < 0
        )
        | (
            F.col(
                "equivalent_listening_level_dbaspl"
            ) < 0
        )
        | (
            F.col("maximum_exposure_dbaspl")
            > 120
        )
        | (
            F.col("exposure_limit_event_count")
            < 0
        )
    )
)

# COMMAND ----------

gold_sleep_table = (
    f"`{catalog_name}`."
    "`iphone_health_gold`."
    "`daily_sleep_summary`"
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_sleep_table}
COMMENT 'Daily in-bed intervals derived from Apple Health sleep records'
TBLPROPERTIES (
    'data_layer' = 'gold',
    'data_domain' = 'personal_health',
    'aggregation_grain' = 'one_row_per_sleep_night',
    'contains_direct_identifiers' = 'false',
    'dates_are_privacy_shifted' = 'true',
    'sleep_measurement_type' = 'in_bed_not_confirmed_asleep'
)
AS

WITH sleep_records AS (
    SELECT
        TO_DATE(
            start_timestamp
            - INTERVAL 18 HOURS
        ) AS protected_sleep_date,

        start_timestamp,
        end_timestamp,
        duration_seconds,
        categorical_value,
        _silver_processed_at

    FROM {silver_table}

    WHERE metric_type = 'SleepAnalysis'
      AND categorical_value =
          'SleepAnalysisInBed'
)

SELECT
    protected_sleep_date,
    TRUE AS date_is_shifted,

    ROUND(
        SUM(duration_seconds) / 3600.0,
        2
    ) AS total_in_bed_hours,

    MIN(start_timestamp)
        AS first_in_bed_timestamp_utc,

    MAX(end_timestamp)
        AS last_out_of_bed_timestamp_utc,

    ROUND(
        (
            UNIX_TIMESTAMP(
                MAX(end_timestamp)
            )
            -
            UNIX_TIMESTAMP(
                MIN(start_timestamp)
            )
        ) / 3600.0,
        2
    ) AS in_bed_window_hours,

    COUNT(*) AS sleep_interval_count,

    MAX(_silver_processed_at)
        AS last_processed_at

FROM sleep_records

GROUP BY protected_sleep_date
""")

print("Gold daily sleep table created.")

# COMMAND ----------

gold_sleep_df = spark.table(
    gold_sleep_table
)

print(
    "Gold sleep nights: "
    f"{gold_sleep_df.count():,}"
)

display(
    gold_sleep_df
    .orderBy(
        F.desc("protected_sleep_date")
    )
    .limit(20)
)

# COMMAND ----------

display(
    gold_sleep_df
    .filter(
        (F.col("total_in_bed_hours") < 0)
        | (F.col("total_in_bed_hours") > 24)
        | (
            F.col("first_in_bed_timestamp_utc")
            >=
            F.col("last_out_of_bed_timestamp_utc")
        )
    )
)

# COMMAND ----------

catalog_name = spark.sql(
    "SELECT current_catalog()"
).first()[0]

gold_activity_table = (
    f"{catalog_name}.iphone_health_gold.daily_activity_summary"
)

gold_mobility_table = (
    f"{catalog_name}.iphone_health_gold.daily_mobility_summary"
)

gold_hearing_table = (
    f"{catalog_name}.iphone_health_gold.daily_hearing_summary"
)

gold_sleep_table = (
    f"{catalog_name}.iphone_health_gold.daily_sleep_summary"
)

gold_daily_health_table = (
    f"{catalog_name}.iphone_health_gold.daily_health_summary"
)


spark.sql(f"""
CREATE OR REPLACE TABLE {gold_daily_health_table}

COMMENT
'Combined daily Apple Health metrics for analytics and dashboards'

TBLPROPERTIES (
    'data_layer' = 'gold',
    'data_domain' = 'personal_health',
    'aggregation_grain' = 'one_row_per_protected_day',
    'contains_direct_identifiers' = 'false',
    'dates_are_shifted' = 'true'
)

AS

WITH date_spine AS (

    SELECT
        protected_event_date AS protected_date
    FROM {gold_activity_table}

    UNION

    SELECT
        protected_event_date AS protected_date
    FROM {gold_mobility_table}

    UNION

    SELECT
        protected_event_date AS protected_date
    FROM {gold_hearing_table}

    UNION

    SELECT
        protected_sleep_date AS protected_date
    FROM {gold_sleep_table}
)

SELECT
    d.protected_date,
    TRUE AS date_is_shifted,

    a.protected_event_date IS NOT NULL
        AS has_activity_data,

    m.protected_event_date IS NOT NULL
        AS has_mobility_data,

    h.protected_event_date IS NOT NULL
        AS has_hearing_data,

    s.protected_sleep_date IS NOT NULL
        AS has_sleep_data,

    a.* EXCEPT (
        protected_event_date,
        date_is_shifted,
        last_processed_at
    ),

    m.* EXCEPT (
        protected_event_date,
        date_is_shifted,
        last_processed_at
    ),

    h.* EXCEPT (
        protected_event_date,
        date_is_shifted,
        last_processed_at
    ),

    s.* EXCEPT (
        protected_sleep_date,
        date_is_shifted,
        last_processed_at
    ),

    GREATEST(
        a.last_processed_at,
        m.last_processed_at,
        h.last_processed_at,
        s.last_processed_at
    ) AS last_processed_at

FROM date_spine d

LEFT JOIN {gold_activity_table} a
    ON d.protected_date = a.protected_event_date

LEFT JOIN {gold_mobility_table} m
    ON d.protected_date = m.protected_event_date

LEFT JOIN {gold_hearing_table} h
    ON d.protected_date = h.protected_event_date

LEFT JOIN {gold_sleep_table} s
    ON d.protected_date = s.protected_sleep_date
""")

print(
    "Combined Gold table created:",
    gold_daily_health_table
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {gold_daily_health_table}
        ORDER BY protected_date DESC
        LIMIT 20
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT protected_date)
                AS unique_date_count,
            MIN(protected_date) AS earliest_protected_date,
            MAX(protected_date) AS latest_protected_date
        FROM {gold_daily_health_table}
    """)
)