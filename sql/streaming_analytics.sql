-- Checkpoint Redshift: Kinesis Streaming Ingestion + Iceberg/Spectrum
-- Entorno validado: us-east-2, base analytics.
-- El rol de servicio de Redshift obtiene los permisos Kinesis/Glue/S3 desde
-- infra/redshift.tf. La conexión SQL de este despliegue se autenticó por IAM.

-- 1. Mapeo del stream de Kinesis en Redshift.
CREATE EXTERNAL SCHEMA IF NOT EXISTS kds_schema
FROM KINESIS
IAM_ROLE '__REDSHIFT_IAM_ROLE_ARN__';

-- MV de ingesta automática: conserva SUPER para tolerar JSON y campos nuevos.
-- failed_payload permite auditar mensajes que no son JSON válido.
CREATE MATERIALIZED VIEW mv_sensor_readings AUTO REFRESH YES AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    CASE WHEN CAN_JSON_PARSE(kinesis_data)
         THEN JSON_PARSE(kinesis_data) ELSE NULL END AS payload,
    CASE WHEN NOT CAN_JSON_PARSE(kinesis_data)
         THEN kinesis_data ELSE NULL END AS failed_payload
FROM kds_schema."realtime-sensors-dev-events";

-- Vista relacional que proyecta tres campos JSON y metadatos del stream.
-- event_id = sensor_id; event_timestamp = hora de llegada en Kinesis;
-- payload_value = temperatura del evento.
CREATE OR REPLACE VIEW v_sensor_readings AS
SELECT
    payload."sensor_id"::VARCHAR(64) AS event_id,
    payload."timestamp"::VARCHAR(40)::TIMESTAMP AS event_timestamp,
    payload."temperatura"::FLOAT8 AS payload_value,
    payload."timestamp"::VARCHAR(40) AS source_timestamp,
    partition_key,
    shard_id,
    sequence_number
FROM mv_sensor_readings
WHERE payload IS NOT NULL;

-- 2. Catálogo Glue utilizado por el pipeline Iceberg de Flink.
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_spectrum
FROM DATA CATALOG
DATABASE 'lakehouse_db'
REGION 'us-east-2'
IAM_ROLE '__REDSHIFT_IAM_ROLE_ARN__';

-- Refrescar de forma manual para una lectura puntual inmediata. En operación
-- normal AUTO REFRESH mantiene la MV; programar este comando cada 30-60 s solo
-- si el consumidor necesita una cadencia explícita.
REFRESH MATERIALIZED VIEW mv_sensor_readings;

-- Inspección de los agregados históricos escritos en Iceberg.
SELECT sensor_id, window_start, window_end, event_count,
       avg_temperature, min_temperature, max_temperature
FROM lakehouse_spectrum.sensor_window_summaries
ORDER BY window_start DESC
LIMIT 20;

-- Unión de lecturas recientes con la ventana Iceberg a la que pertenecen.
SELECT
    live.event_id AS sensor_id,
    live.event_timestamp AS live_reading_at,
    live.payload_value AS live_temperature,
    hist.window_start,
    hist.window_end,
    hist.avg_temperature AS window_avg_temperature,
    hist.min_temperature AS window_min_temperature,
    hist.max_temperature AS window_max_temperature,
    live.payload_value - hist.avg_temperature AS deviation_from_window_avg
FROM v_sensor_readings AS live
JOIN lakehouse_spectrum.sensor_window_summaries AS hist
  ON live.event_id = hist.sensor_id
 AND live.event_timestamp >= hist.window_start
 AND live.event_timestamp < hist.window_end
ORDER BY live.event_timestamp DESC
LIMIT 50;

-- Rol de base de datos con permisos mínimos de consulta.
-- Ejecutar CREATE ROLE una vez.
CREATE ROLE analytics_reader;
GRANT USAGE ON SCHEMA kds_schema TO ROLE analytics_reader;
GRANT USAGE ON SCHEMA lakehouse_spectrum TO ROLE analytics_reader;
GRANT SELECT ON v_sensor_readings TO ROLE analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_spectrum TO ROLE analytics_reader;

-- Principal IAM que ejecutó la validación de este entorno. Para producción,
-- conceder a un principal IAM dedicado del equipo de analítica.
GRANT ROLE analytics_reader TO "IAM:terraform-curso";
