DROP TABLE IF EXISTS gold_pump_health;

CREATE TABLE gold_pump_health AS

WITH baseline AS (

    SELECT
        AVG(mean_temp) AS global_temp_mean,
        STDDEV(mean_temp) AS global_temp_sd,

        AVG(mean_vibration) AS global_vibration_mean,
        STDDEV(mean_vibration) AS global_vibration_sd

    FROM silver_pump_30min_agg

)

SELECT

    s.window_start,
    s.pump_id,

    s.mean_temp,
    s.mean_vibration,

    ROUND(
        (
            (s.mean_temp - b.global_temp_mean)
            /
            NULLIF(b.global_temp_sd,0)
        )::numeric,
        2
    ) AS temp_zscore,

    ROUND(
        (
            (s.mean_vibration - b.global_vibration_mean)
            /
            NULLIF(b.global_vibration_sd,0)
        )::numeric,
        2
    ) AS vibration_zscore,

    GREATEST(
        0,
        ROUND(
            (
                100
                - ABS(
                    (
                        (s.mean_temp - b.global_temp_mean)
                        /
                        NULLIF(b.global_temp_sd,0)
                    )
                ) * 10
                - ABS(
                    (
                        (s.mean_vibration - b.global_vibration_mean)
                        /
                        NULLIF(b.global_vibration_sd,0)
                    )
                ) * 20
            )::numeric,
            2
        )
    ) AS health_score,

    CASE

        WHEN
            ABS(
                (
                    (s.mean_temp - b.global_temp_mean)
                    /
                    NULLIF(b.global_temp_sd,0)
                )
            ) > 3

            OR

            ABS(
                (
                    (s.mean_vibration - b.global_vibration_mean)
                    /
                    NULLIF(b.global_vibration_sd,0)
                )
            ) > 3

        THEN 'CRITICAL'

        WHEN
            ABS(
                (
                    (s.mean_temp - b.global_temp_mean)
                    /
                    NULLIF(b.global_temp_sd,0)
                )
            ) > 2

            OR

            ABS(
                (
                    (s.mean_vibration - b.global_vibration_mean)
                    /
                    NULLIF(b.global_vibration_sd,0)
                )
            ) > 2

        THEN 'WARNING'

        ELSE 'NORMAL'

    END AS risk_level

FROM silver_pump_30min_agg s
CROSS JOIN baseline b;