# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Apple Health Analytics Views
# MAGIC
# MAGIC This notebook creates reusable daily, weekly, and monthly analytical views
# MAGIC from the privacy-protected Gold tables.
# MAGIC

# COMMAND ----------

# Check the current Databricks catalog and schema

display(
    spark.sql("""
        SELECT
            current_catalog() AS current_catalog,
            current_schema() AS current_schema
    """)
)

# COMMAND ----------

# Display all schemas available in the current catalog

display(
    spark.sql("""
        SHOW SCHEMAS
    """)
)

# COMMAND ----------

# List all tables and views in the Gold schema

display(
    spark.sql("""
        SHOW TABLES IN iphone_health_gold
    """)
)

# COMMAND ----------

# List only the views already created in the Gold schema

display(
    spark.sql("""
        SHOW VIEWS IN iphone_health_gold
    """)
)

# COMMAND ----------

# Show every Gold object and identify whether it is a table or view

display(
    spark.sql("""
        SELECT
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = 'iphone_health_gold'
        ORDER BY table_type, table_name
    """)
)

# COMMAND ----------

# Gold schema
gold_schema = "iphone_health_gold"

# Gold managed tables
gold_activity_table = f"{gold_schema}.daily_activity_summary"
gold_health_table = f"{gold_schema}.daily_health_summary"
gold_hearing_table = f"{gold_schema}.daily_hearing_summary"
gold_mobility_table = f"{gold_schema}.daily_mobility_summary"
gold_sleep_table = f"{gold_schema}.daily_sleep_summary"

# Analytics views
daily_trends_view = f"{gold_schema}.v_daily_health_trends"
weekly_summary_view = f"{gold_schema}.v_weekly_health_summary"
monthly_summary_view = f"{gold_schema}.v_monthly_health_summary"

print("Gold notebook variables configured successfully.")
print("Main source table:", gold_health_table)
print("Daily view:", daily_trends_view)
print("Weekly view:", weekly_summary_view)
print("Monthly view:", monthly_summary_view)

# COMMAND ----------

# Examine the exact columns in the combined daily Gold table

display(
    spark.sql(f"""
        DESCRIBE {gold_health_table}
    """)
)

# COMMAND ----------

# Print every column and its data type

gold_health_df = spark.table(gold_health_table)

print("Source table:", gold_health_table)
print("Number of columns:", len(gold_health_df.columns))
print()

for position, (column_name, data_type) in enumerate(
    gold_health_df.dtypes,
    start=1
):
    print(f"{position:02}. {column_name:<40} {data_type}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {daily_trends_view}
COMMENT 'Daily Apple Health metrics with rolling trends and day-over-day comparisons'
AS

WITH calculated_trends AS (
    SELECT
        protected_date,
        date_is_shifted,

        -- Calendar attributes
        CAST(
            DATE_TRUNC('week', protected_date)
            AS DATE
        ) AS week_start_date,

        CAST(
            DATE_TRUNC('month', protected_date)
            AS DATE
        ) AS month_start_date,

        YEAR(protected_date) AS protected_year,
        MONTH(protected_date) AS protected_month,
        DATE_FORMAT(protected_date, 'EEEE') AS day_name,

        -- Data availability indicators
        has_activity_data,
        has_mobility_data,
        has_hearing_data,
        has_sleep_data,

        -- Activity metrics
        total_steps,
        walking_distance_km,
        active_energy_kcal,
        basal_energy_kcal,
        flights_climbed,
        step_measurement_count,

        -- Mobility metrics
        avg_walking_speed_kmh,
        avg_step_length_cm,
        avg_double_support_percent,
        avg_asymmetry_percent,
        avg_walking_steadiness_percent,
        walking_measurement_count,

        -- Hearing metrics
        exposure_measurement_count,
        measured_listening_minutes,
        equivalent_listening_level_dbaspl,
        maximum_exposure_dbaspl,
        exposure_limit_event_count,

        -- Sleep metrics
        total_in_bed_hours,
        first_in_bed_timestamp_utc,
        last_out_of_bed_timestamp_utc,
        in_bed_window_hours,
        sleep_interval_count,

        -- Previous-day steps
        LAG(total_steps, 1) OVER (
            ORDER BY protected_date
        ) AS previous_day_steps,

        -- Seven-day activity trends
        ROUND(
            AVG(total_steps) OVER (
                ORDER BY protected_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ),
            2
        ) AS avg_steps_7d,

        ROUND(
            AVG(walking_distance_km) OVER (
                ORDER BY protected_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ),
            3
        ) AS avg_walking_distance_7d_km,

        ROUND(
            AVG(active_energy_kcal) OVER (
                ORDER BY protected_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ),
            2
        ) AS avg_active_energy_7d_kcal,

        ROUND(
            AVG(total_in_bed_hours) OVER (
                ORDER BY protected_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ),
            2
        ) AS avg_sleep_7d_hours,

        -- Thirty-day step trend
        ROUND(
            AVG(total_steps) OVER (
                ORDER BY protected_date
                ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
            ),
            2
        ) AS avg_steps_30d,

        last_processed_at

    FROM {gold_health_table}
)

SELECT
    *,

    -- Difference from the previous day's steps
    total_steps - previous_day_steps
        AS steps_change_from_previous_day,

    -- Percentage difference from the previous day
    ROUND(
        CASE
            WHEN previous_day_steps IS NULL
                OR previous_day_steps = 0
            THEN NULL

            ELSE (
                (total_steps - previous_day_steps)
                * 100.0
                / previous_day_steps
            )
        END,
        2
    ) AS steps_change_percent

FROM calculated_trends
""")

print(f"Daily trends view created successfully: {daily_trends_view}")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            protected_date,
            day_name,
            total_steps,
            previous_day_steps,
            steps_change_from_previous_day,
            steps_change_percent,
            avg_steps_7d,
            avg_steps_30d,
            walking_distance_km,
            avg_walking_distance_7d_km
        FROM {daily_trends_view}
        ORDER BY protected_date
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

            MIN(protected_date)
                AS earliest_protected_date,

            MAX(protected_date)
                AS latest_protected_date,

            DATEDIFF(
                MAX(protected_date),
                MIN(protected_date)
            ) + 1 AS calendar_span_days,

            (
                DATEDIFF(
                    MAX(protected_date),
                    MIN(protected_date)
                ) + 1
            ) - COUNT(DISTINCT protected_date)
                AS missing_date_count,

            SUM(
                CASE
                    WHEN protected_date IS NULL THEN 1
                    ELSE 0
                END
            ) AS null_date_count

        FROM {daily_trends_view}
    """)
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {weekly_summary_view}
COMMENT 'Weekly Apple Health summary derived from the daily trends view'
AS

WITH weekly_aggregates AS (
    SELECT
        week_start_date,

        DATE_ADD(
            week_start_date,
            6
        ) AS week_end_date,

        MIN(protected_date)
            AS first_available_date,

        MAX(protected_date)
            AS last_available_date,

        COUNT(*)
            AS days_in_week,

        CASE
            WHEN COUNT(*) = 7 THEN TRUE
            ELSE FALSE
        END AS is_complete_week,

        -- Data availability
        SUM(
            CASE WHEN has_activity_data THEN 1 ELSE 0 END
        ) AS activity_days_count,

        SUM(
            CASE WHEN has_mobility_data THEN 1 ELSE 0 END
        ) AS mobility_days_count,

        SUM(
            CASE WHEN has_hearing_data THEN 1 ELSE 0 END
        ) AS hearing_days_count,

        SUM(
            CASE WHEN has_sleep_data THEN 1 ELSE 0 END
        ) AS sleep_days_count,

        -- Activity summary
        SUM(total_steps)
            AS total_steps,

        ROUND(AVG(total_steps), 2)
            AS avg_daily_steps,

        MIN(total_steps)
            AS minimum_daily_steps,

        MAX(total_steps)
            AS maximum_daily_steps,

        ROUND(SUM(walking_distance_km), 3)
            AS total_walking_distance_km,

        ROUND(AVG(walking_distance_km), 3)
            AS avg_daily_walking_distance_km,

        ROUND(SUM(active_energy_kcal), 2)
            AS total_active_energy_kcal,

        ROUND(AVG(active_energy_kcal), 2)
            AS avg_daily_active_energy_kcal,

        ROUND(SUM(basal_energy_kcal), 2)
            AS total_basal_energy_kcal,

        SUM(flights_climbed)
            AS total_flights_climbed,

        -- Mobility summary
        ROUND(AVG(avg_walking_speed_kmh), 3)
            AS avg_walking_speed_kmh,

        ROUND(AVG(avg_step_length_cm), 2)
            AS avg_step_length_cm,

        ROUND(AVG(avg_double_support_percent), 2)
            AS avg_double_support_percent,

        ROUND(AVG(avg_asymmetry_percent), 2)
            AS avg_asymmetry_percent,

        ROUND(AVG(avg_walking_steadiness_percent), 2)
            AS avg_walking_steadiness_percent,

        -- Hearing summary
        ROUND(SUM(measured_listening_minutes), 2)
            AS total_measured_listening_minutes,

        ROUND(AVG(equivalent_listening_level_dbaspl), 2)
            AS avg_equivalent_listening_level_dbaspl,

        ROUND(MAX(maximum_exposure_dbaspl), 2)
            AS maximum_exposure_dbaspl,

        SUM(exposure_limit_event_count)
            AS exposure_limit_event_count,

        -- Sleep summary
        ROUND(SUM(total_in_bed_hours), 2)
            AS total_in_bed_hours,

        ROUND(AVG(total_in_bed_hours), 2)
            AS avg_daily_in_bed_hours,

        ROUND(AVG(in_bed_window_hours), 2)
            AS avg_in_bed_window_hours,

        SUM(sleep_interval_count)
            AS sleep_interval_count,

        MAX(last_processed_at)
            AS last_processed_at

    FROM {daily_trends_view}

    GROUP BY week_start_date
),

weekly_trends AS (
    SELECT
        *,

        LAG(total_steps, 1) OVER (
            ORDER BY week_start_date
        ) AS previous_week_total_steps,

        ROUND(
            AVG(avg_daily_steps) OVER (
                ORDER BY week_start_date
                ROWS BETWEEN 3 PRECEDING
                AND CURRENT ROW
            ),
            2
        ) AS avg_daily_steps_4w

    FROM weekly_aggregates
)

SELECT
    *,

    total_steps - previous_week_total_steps
        AS steps_change_from_previous_week,

    ROUND(
        CASE
            WHEN previous_week_total_steps IS NULL
                OR previous_week_total_steps = 0
            THEN NULL

            ELSE (
                (total_steps - previous_week_total_steps)
                * 100.0
                / previous_week_total_steps
            )
        END,
        2
    ) AS weekly_steps_change_percent

FROM weekly_trends
""")

print(f"Weekly summary view created successfully: {weekly_summary_view}")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            week_start_date,
            week_end_date,
            days_in_week,
            is_complete_week,
            total_steps,
            avg_daily_steps,
            previous_week_total_steps,
            steps_change_from_previous_week,
            weekly_steps_change_percent,
            avg_daily_steps_4w
        FROM {weekly_summary_view}
        ORDER BY week_start_date
        LIMIT 20
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*)
                AS weekly_row_count,

            COUNT(DISTINCT week_start_date)
                AS unique_week_count,

            SUM(days_in_week)
                AS represented_daily_rows,

            SUM(
                CASE
                    WHEN is_complete_week THEN 1
                    ELSE 0
                END
            ) AS complete_week_count,

            SUM(
                CASE
                    WHEN NOT is_complete_week THEN 1
                    ELSE 0
                END
            ) AS partial_week_count,

            MIN(week_start_date)
                AS earliest_week_start,

            MAX(week_start_date)
                AS latest_week_start,

            MIN(days_in_week)
                AS minimum_days_in_week,

            MAX(days_in_week)
                AS maximum_days_in_week

        FROM {weekly_summary_view}
    """)
)

# COMMAND ----------

gold_schema = "iphone_health_gold"

daily_trends_view = (
    f"{gold_schema}.v_daily_health_trends"
)

monthly_summary_view = (
    f"{gold_schema}.v_monthly_health_summary"
)

print("Daily source view:", daily_trends_view)
print("Monthly target view:", monthly_summary_view)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS daily_row_count,
            MIN(protected_date) AS earliest_date,
            MAX(protected_date) AS latest_date
        FROM {daily_trends_view}
    """)
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {monthly_summary_view}
COMMENT 'Monthly Apple Health summary derived from the daily trends view'
AS

WITH monthly_aggregates AS (
    SELECT
        month_start_date,
        LAST_DAY(month_start_date)
            AS month_end_date,

        YEAR(month_start_date)
            AS protected_year,

        MONTH(month_start_date)
            AS protected_month_number,

        DATE_FORMAT(month_start_date, 'yyyy-MM')
            AS protected_month_label,

        MIN(protected_date)
            AS first_available_date,

        MAX(protected_date)
            AS last_available_date,

        COUNT(*)
            AS days_in_month_available,

        DAYOFMONTH(LAST_DAY(month_start_date))
            AS expected_days_in_month,

        CASE
            WHEN COUNT(*) =
                DAYOFMONTH(LAST_DAY(month_start_date))
            THEN TRUE
            ELSE FALSE
        END AS is_complete_month,

        SUM(
            CASE WHEN has_activity_data
            THEN 1 ELSE 0 END
        ) AS activity_days_count,

        SUM(
            CASE WHEN has_mobility_data
            THEN 1 ELSE 0 END
        ) AS mobility_days_count,

        SUM(
            CASE WHEN has_hearing_data
            THEN 1 ELSE 0 END
        ) AS hearing_days_count,

        SUM(
            CASE WHEN has_sleep_data
            THEN 1 ELSE 0 END
        ) AS sleep_days_count,

        SUM(total_steps)
            AS total_steps,

        ROUND(AVG(total_steps), 2)
            AS avg_daily_steps,

        MIN(total_steps)
            AS minimum_daily_steps,

        MAX(total_steps)
            AS maximum_daily_steps,

        ROUND(SUM(walking_distance_km), 3)
            AS total_walking_distance_km,

        ROUND(AVG(walking_distance_km), 3)
            AS avg_daily_walking_distance_km,

        ROUND(SUM(active_energy_kcal), 2)
            AS total_active_energy_kcal,

        ROUND(AVG(active_energy_kcal), 2)
            AS avg_daily_active_energy_kcal,

        ROUND(SUM(basal_energy_kcal), 2)
            AS total_basal_energy_kcal,

        SUM(flights_climbed)
            AS total_flights_climbed,

        ROUND(AVG(avg_walking_speed_kmh), 3)
            AS avg_walking_speed_kmh,

        ROUND(AVG(avg_step_length_cm), 2)
            AS avg_step_length_cm,

        ROUND(AVG(avg_double_support_percent), 2)
            AS avg_double_support_percent,

        ROUND(AVG(avg_asymmetry_percent), 2)
            AS avg_asymmetry_percent,

        ROUND(
            AVG(avg_walking_steadiness_percent),
            2
        ) AS avg_walking_steadiness_percent,

        ROUND(
            SUM(measured_listening_minutes),
            2
        ) AS total_measured_listening_minutes,

        ROUND(
            AVG(equivalent_listening_level_dbaspl),
            2
        ) AS avg_equivalent_listening_level_dbaspl,

        ROUND(MAX(maximum_exposure_dbaspl), 2)
            AS maximum_exposure_dbaspl,

        SUM(exposure_limit_event_count)
            AS exposure_limit_event_count,

        ROUND(SUM(total_in_bed_hours), 2)
            AS total_in_bed_hours,

        ROUND(AVG(total_in_bed_hours), 2)
            AS avg_daily_in_bed_hours,

        ROUND(AVG(in_bed_window_hours), 2)
            AS avg_in_bed_window_hours,

        SUM(sleep_interval_count)
            AS sleep_interval_count,

        MAX(last_processed_at)
            AS last_processed_at

    FROM {daily_trends_view}

    GROUP BY month_start_date
),

monthly_trends AS (
    SELECT
        *,

        LAG(total_steps, 1)
        OVER (
            ORDER BY month_start_date
        ) AS previous_month_total_steps,

        ROUND(
            AVG(avg_daily_steps)
            OVER (
                ORDER BY month_start_date
                ROWS BETWEEN
                    2 PRECEDING
                    AND CURRENT ROW
            ),
            2
        ) AS avg_daily_steps_3m

    FROM monthly_aggregates
)

SELECT
    *,

    total_steps - previous_month_total_steps
        AS steps_change_from_previous_month,

    ROUND(
        CASE
            WHEN previous_month_total_steps IS NULL
                OR previous_month_total_steps = 0
            THEN NULL
            ELSE (
                total_steps -
                previous_month_total_steps
            ) * 100.0 /
                previous_month_total_steps
        END,
        2
    ) AS monthly_steps_change_percent

FROM monthly_trends
""")

print("Monthly health summary view created.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS monthly_row_count,

            COUNT(DISTINCT month_start_date)
                AS unique_month_count,

            SUM(days_in_month_available)
                AS represented_daily_rows,

            SUM(
                CASE
                    WHEN is_complete_month THEN 1
                    ELSE 0
                END
            ) AS complete_month_count,

            SUM(
                CASE
                    WHEN NOT is_complete_month THEN 1
                    ELSE 0
                END
            ) AS partial_month_count,

            MIN(month_start_date)
                AS earliest_month_start,

            MAX(month_start_date)
                AS latest_month_start,

            MIN(days_in_month_available)
                AS minimum_days_available,

            MAX(days_in_month_available)
                AS maximum_days_available

        FROM {monthly_summary_view}
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            month_start_date,
            month_end_date,
            first_available_date,
            last_available_date,
            days_in_month_available,
            expected_days_in_month,
            is_complete_month
        FROM {monthly_summary_view}
        WHERE NOT is_complete_month
        ORDER BY month_start_date
    """)
)