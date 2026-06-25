DROP TABLE IF EXISTS silver_temperature;

CREATE TABLE silver_temperature AS
SELECT
    reading_time,
    pump_id,
    value AS temperature
FROM bronze_sensor_logs
WHERE sensor_type = 'temperature';