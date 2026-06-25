DROP TABLE IF EXISTS silver_pump_30min_agg;

CREATE TABLE silver_pump_30min_agg AS

WITH temp_agg AS (

    SELECT
        pump_id,

        date_trunc('hour', reading_time)
        + floor(extract(minute from reading_time) / 30) * interval '30 minute'
            AS window_start,

        AVG(temperature) AS mean_temp,
        STDDEV(temperature) AS sd_temp,
        MIN(temperature) AS min_temp,
        MAX(temperature) AS max_temp

    FROM silver_temperature

    GROUP BY
        pump_id,
        window_start

),

vibration_agg AS (

    SELECT
        pump_id,

        date_trunc('hour', reading_time)
        + floor(extract(minute from reading_time) / 30) * interval '30 minute'
            AS window_start,

        AVG(vibration) AS mean_vibration,
        STDDEV(vibration) AS sd_vibration,
        MIN(vibration) AS min_vibration,
        MAX(vibration) AS max_vibration

    FROM silver_vibration

    GROUP BY
        pump_id,
        window_start

)

SELECT
    t.window_start,
    t.pump_id,

    t.mean_temp,
    t.sd_temp,
    t.min_temp,
    t.max_temp,

    v.mean_vibration,
    v.sd_vibration,
    v.min_vibration,
    v.max_vibration

FROM temp_agg t
INNER JOIN vibration_agg v
    ON t.pump_id = v.pump_id
   AND t.window_start = v.window_start;