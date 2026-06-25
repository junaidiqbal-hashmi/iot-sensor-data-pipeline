DROP TABLE IF EXISTS silver_vibration;

CREATE TABLE silver_vibration AS
SELECT
    reading_time,
    pump_id,
    value AS vibration
FROM bronze_sensor_logs
WHERE sensor_type = 'vibration';