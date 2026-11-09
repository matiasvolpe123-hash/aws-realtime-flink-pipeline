-- =============================================================================
-- Checkpoint Redshift: analítica avanzada in-stream
-- Pipeline: Kinesis (Módulo 2) -> Redshift Streaming Ingestion (datos calientes)
--           Flink -> Iceberg/Glue (Módulo 4) -> Redshift Spectrum (datos históricos)
--
-- Reemplazá los placeholders <...> por los valores reales de tu despliegue:
--   <REDSHIFT_IAM_ROLE_ARN>  -> output "redshift_iam_role_arn" de terraform
--   <KINESIS_STREAM_NAME>    -> output "stream_name" / "realtime-sensors-dev-events"
--   <AWS_REGION>             -> us-east-2
-- Ejecutar en orden, conectado al workgroup `realtime-sensors-dev-analytics-wg`,
-- base de datos `analytics`, con un cliente SQL que soporte Redshift
-- (query editor v2 de la consola, psql, DBeaver, etc).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1) INGESTA: mapeo del stream de Kinesis como objeto de Redshift
-- -----------------------------------------------------------------------------
-- Streaming Ingestion lee directamente de Kinesis (shard iterators / GetRecords),
-- sin pasar por S3 ni por un job de carga: por eso la latencia extremo a extremo
-- es de segundos en vez de minutos. El rol IAM usado acá (redshift.tf) solo tiene
-- permisos de lectura sobre ESTE stream puntual, nunca de escritura.
CREATE EXTERNAL SCHEMA IF NOT EXISTS kds_schema
FROM KINESIS
IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN>';

-- Vista materializada sobre el stream: cada fila de kds_schema.<stream> expone
-- el payload crudo en bytes (kinesis_data) más metadata de Kinesis (shard,
-- número de secuencia, hora de llegada). JSON_PARSE conviene el payload en un
-- valor SUPER navegable; de ahí extraemos los 3 campos clave pedidos por la
-- consigna (acá mapeados a los campos reales de nuestros eventos de sensores):
--   event_id        <- sensor_id
--   event_timestamp <- approximate_arrival_timestamp (hora de Kinesis; robusta
--                       incluso si el productor no manda timestamp propio)
--   payload_value    <- temperatura
--
-- Refresco: AUTO REFRESH YES deja que Redshift decida cuándo recalcular según
-- el flujo de datos entrante. Para la mayoría de los paneles de sensores (no
-- es trading de alta frecuencia) alcanza con eso; si hiciera falta un control
-- más fino, se reemplaza por REFRESH MATERIALIZED VIEW manual cada 30-60 s
-- desde una tarea programada (ver sección 5 de este archivo, "Optimización").
-- Ojo con el error común de refrescar cada 1 s: eso satura el clúster/workgroup
-- compitiendo por los mismos recursos que las consultas de lectura.
CREATE MATERIALIZED VIEW mv_sensor_readings
AUTO REFRESH YES
AS
SELECT
    approximate_arrival_timestamp                              AS event_timestamp,
    JSON_PARSE(FROM_VARBYTE(kinesis_data, 'utf-8'))             AS payload,
    JSON_PARSE(FROM_VARBYTE(kinesis_data, 'utf-8'))."sensor_id"::VARCHAR(64)   AS event_id,
    JSON_PARSE(FROM_VARBYTE(kinesis_data, 'utf-8'))."temperatura"::FLOAT8      AS payload_value,
    partition_key,
    shard_id,
    sequence_number
FROM kds_schema."<KINESIS_STREAM_NAME>"
-- "Schema drift": si un registro no es JSON válido o le falta un campo,
-- IS_VALID_JSON lo descarta en vez de romper el refresco completo de la MV.
WHERE IS_VALID_JSON(FROM_VARBYTE(kinesis_data, 'utf-8'));


-- -----------------------------------------------------------------------------
-- 2) INTEGRACIÓN LAKEHOUSE: esquema externo apuntando al Glue Data Catalog
-- -----------------------------------------------------------------------------
-- Mismo catálogo (`lakehouse_db`) que usa Flink para escribir las tablas
-- Iceberg (ver infra/main.tf y app/.../SensorAggregationJob.java del Módulo 4).
-- Redshift Spectrum lee los archivos Iceberg directamente desde S3, sin
-- copiar datos a Redshift: por eso "lakehouse_spectrum" no ocupa espacio en
-- el workgroup, solo es un puntero al catálogo + los permisos del rol IAM
-- para leer s3://.../warehouse/* (ver statement "LakehouseDataRead" en
-- infra/redshift.tf).
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_spectrum
FROM DATA CATALOG
DATABASE 'lakehouse_db'
REGION '<AWS_REGION>'
IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN>';

-- Consulta de validación: la tabla Iceberg escrita por Flink debe ser legible
-- tal cual Spectrum la expone (sin transformación adicional de nuestro lado).
SELECT sensor_id, window_start, window_end, event_count,
       avg_temperature, min_temperature, max_temperature
FROM lakehouse_spectrum.sensor_window_summaries
ORDER BY window_start DESC
LIMIT 20;

-- JOIN: combina el dato "caliente" del stream (última lectura cruda, vía la MV)
-- con el agregado "histórico" de la ventana de 1 minuto en la que cae esa
-- lectura (tabla Iceberg). Útil para comparar, por sensor, el valor más
-- reciente contra el promedio ya consolidado de su ventana.
SELECT
    live.event_id                 AS sensor_id,
    live.event_timestamp          AS live_reading_at,
    live.payload_value            AS live_temperature,
    hist.window_start,
    hist.window_end,
    hist.avg_temperature          AS window_avg_temperature,
    hist.min_temperature          AS window_min_temperature,
    hist.max_temperature          AS window_max_temperature,
    live.payload_value - hist.avg_temperature AS deviation_from_window_avg
FROM mv_sensor_readings AS live
JOIN lakehouse_spectrum.sensor_window_summaries AS hist
  ON  live.event_id = hist.sensor_id
  AND live.event_timestamp >= hist.window_start
  AND live.event_timestamp <  hist.window_end
ORDER BY live.event_timestamp DESC
LIMIT 50;


-- -----------------------------------------------------------------------------
-- 3) OPTIMIZACIÓN Y SEGURIDAD
-- -----------------------------------------------------------------------------

-- Refresco incremental explícito (alternativa/complemento a AUTO REFRESH):
-- se puede programar con el scheduler nativo de Redshift (CREATE SCHEDULE +
-- CREATE PROCEDURE, o un EventBridge Scheduler externo) a intervalos de
-- 30-60 s. Documentado acá como el comando a programar, no como AUTO REFRESH,
-- para dejar explícito el control de costo/latencia que pide la consigna:
REFRESH MATERIALIZED VIEW mv_sensor_readings;

-- Rol de analítica restringido: separado del admin del namespace. Solo puede
-- leer los dos esquemas de este checkpoint, nada de DDL ni acceso a otros
-- esquemas/bases.
CREATE ROLE analytics_reader;

GRANT USAGE ON SCHEMA kds_schema        TO ROLE analytics_reader;
GRANT USAGE ON SCHEMA lakehouse_spectrum TO ROLE analytics_reader;
GRANT SELECT ON mv_sensor_readings       TO ROLE analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_spectrum TO ROLE analytics_reader;

-- Autenticación IAM: en vez de usuarios con contraseña propia de Redshift,
-- el rol de base de datos se mapea 1:1 a un rol de IAM. Cualquiera que pueda
-- asumir ese rol de IAM (vía SSO, instancia, etc.) entra a Redshift ya
-- autenticado por IAM y con exactamente los permisos de analytics_reader,
-- sin credenciales adicionales que gestionar ni rotar a mano.
-- Reemplazá <ANALYTICS_IAM_ROLE_NAME> por el nombre del rol de IAM (no el ARN)
-- que usarán los analistas para conectarse (distinto del rol de servicio del
-- workgroup, que es el que usa Redshift para leer Kinesis/Glue).
GRANT ROLE analytics_reader TO "IAMR:<ANALYTICS_IAM_ROLE_NAME>";

-- Verificación rápida de que el rol quedó bien scoped (debe listar solo los
-- dos esquemas de este checkpoint, nunca "public" con permisos de escritura).
SELECT * FROM svv_schema_privileges WHERE grantee = 'analytics_reader';
