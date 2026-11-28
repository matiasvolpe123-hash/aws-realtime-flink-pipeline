# AWS real-time sensor processing

Pipeline incremental de procesamiento de sensores con Kinesis Data Streams, Firehose, S3 y Managed Service for Apache Flink. Este repositorio es independiente y se inicializa desde cero con los archivos de este proyecto.

## Arquitectura

```text
Productor → Kinesis Data Streams ──┬──→ Firehose → S3 (eventos raw)
                                  └──→ Managed Flink → Iceberg/S3 + Glue Catalog
```

Terraform crea la clave KMS, el stream cifrado, el bucket privado del data lake con versionado, la base Glue `lakehouse_db`, Firehose y la aplicación Flink. El JAR se publica en el bucket antes de crear la aplicación. Flink consume el stream directamente; no lee la copia de Firehose en S3.

## Lógica de negocio

Cada evento JSON debe incluir `sensor_id` y `temperatura`. Flink agrupa por sensor en ventanas tumbling de tiempo de evento de un minuto y escribe un resumen con cantidad, promedio, mínimo y máximo a `lakehouse_db.sensor_window_summaries` en Iceberg. CloudWatch conserva los logs de diagnóstico.

El job acepta `event_time` o `timestamp` como epoch en milisegundos o timestamp ISO-8601. Cuando falta el campo temporal, usa la hora de ingestión como respaldo. El `WatermarkStrategy` acepta hasta diez segundos de desorden. Los acumuladores se guardan en el estado keyed de la ventana. Los checkpoints están habilitados cada 60 segundos.

## Paralelismo y KPUs

La configuración inicial asigna dos shards al stream y paralelismo global 2 al job, para consumir ambas particiones. `parallelism_per_kpu = 1` configura una unidad de paralelismo por KPU; el uso real de KPUs depende de la carga del grafo. El autoscaling queda deshabilitado para mantener la capacidad y el costo previsibles. Cambiá estos valores en `terraform/variables.tf` o mediante `terraform.tfvars`.

La aplicación no requiere asociación con una VPC: lee Kinesis y escribe logs mediante los endpoints de servicio de AWS. Si se agrega un destino privado en una VPC, hay que configurar subredes, security groups y conectividad de salida a ese servicio.

## Persistencia Iceberg

La tabla usa particionado oculto `days(window_start)`: los resúmenes de una misma fecha quedan juntos, evitando particiones de alta cardinalidad por sensor o instante. Consultas Athena que acoten `window_start` por fecha permiten *partition pruning* y omiten archivos de otros días. El sink confirma escrituras junto con checkpoints de Flink cada 60 segundos; los JSON de metadatos Iceberg quedan en `s3://<bucket>/warehouse/lakehouse_db.db/sensor_window_summaries/metadata/`.

GlueCatalog aplica control optimista de versiones en los commits. Mantén un único job escritor sobre esta tabla y evita operaciones de mantenimiento concurrentes. Se requiere AWS SDK 2.17.131 o posterior en el runtime para usar el bloqueo optimista de Glue.

Verifica la tabla en Glue Data Catalog → `lakehouse_db` → `sensor_window_summaries`. En Athena:

```sql
SELECT sensor_id, window_start, window_end, event_count,
       avg_temperature, min_temperature, max_temperature
FROM lakehouse_db.sensor_window_summaries
WHERE window_start >= TIMESTAMP '2026-01-01 00:00:00'
  AND window_start <  TIMESTAMP '2026-01-02 00:00:00';
```

Después del despliegue, guarda capturas reales de Glue y Athena en `evidence/iceberg-glue.png` y `evidence/iceberg-athena.png`. La cuenta descrita abajo sigue bloqueada por una SCP, así que no se incluyen capturas simuladas; la comprobación cloud queda pendiente hasta que AWS retire ese bloqueo.

## Compilar y desplegar

Requisitos: JDK 11 o posterior, Maven 3.8+, Terraform 1.5+ y AWS CLI con permisos para crear Kinesis, KMS, S3, Glue, IAM, CloudWatch, Firehose, Managed Flink y Redshift Serverless.

El código de `terraform/` está dividido en cuatro módulos (`modules/storage`, `ingestion`, `flink`, `redshift`) y guarda su estado en un backend remoto S3 con locking en DynamoDB.

```bash
# 1. Compilar el JAR de Flink
cd flink-app
mvn clean package

# 2. Crear una sola vez el bucket y la tabla del backend remoto
cd ../terraform
bash bootstrap_backend.sh          # imprime: Bucket: tfstate-<account-id>
echo 'bucket = "tfstate-<account-id>"' > backend.hcl

# 3. Variables y despliegue
cp terraform.tfvars.example terraform.tfvars   # completar redshift_allowed_cidr con tu IP /32
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

La aplicación de Flink arranca sola al terminar el `apply` (`start_application = true`); no hace falta ningún comando manual. Las vistas de Redshift no las crea Terraform: se ejecutan una vez desde `sql/` (ver más abajo).

Enviar datos de prueba (el ejemplo usa hora de ingestión):

```bash
aws kinesis put-record --stream-name realtime-sensors-dev-events \
  --partition-key sensor-1 \
  --data '{"sensor_id":"sensor-1","temperatura":21.5}' \
  --cli-binary-format raw-in-base64-out --region us-east-2
```

Mandá varios eventos para el mismo sensor y ventana; luego verificá los agregados:

```bash
aws logs tail /aws/kinesis-analytics/realtime-sensors-dev-flink-aggregation \
  --since 10m --follow --region us-east-2
```

## Evidencia de ejecución

### Ejecución real en AWS

El stack completo (Kinesis, Managed Flink, Iceberg/Glue, Redshift Serverless) se destruyó y se volvió a crear desde cero con un solo `terraform apply` (25 recursos en unos 6 minutos, aplicación de Flink arrancada por Terraform). Después se enviaron 150 eventos sintéticos con `scripts/send_test_events.py` y se tomaron las capturas el 27/11/2026. Una vez tomada la evidencia, el stack se destruye para no generar costo.

- `evidence/flink-dashboard.png`: Apache Flink Dashboard, job `sensor-temperature-aggregation` en RUNNING, 3/3 tasks, 0 fallos.
- `evidence/flink-checkpoints.png`: 25 checkpoints completados, 0 fallidos.
- `evidence/redshift-freshness.png`: `event_timestamp` (hora del sensor) junto a `approximate_arrival_timestamp` (hora de llegada a Kinesis).
- `evidence/redshift-iceberg.png`: tabla Iceberg `sensor_window_summaries` leída por Redshift Spectrum.
- `evidence/redshift-join.png`: JOIN entre la vista en vivo de Redshift y la tabla Iceberg.

El análisis completo está en el documento DAAT (`Matias_Volpe_Capstone_RealTime.pdf`).

#### Vistas de Redshift

Después del `apply`, desde la raíz del repositorio:

```bash
bash sql/render_sql.sh        # genera sql/streaming_analytics.rendered.sql con el ARN del rol
```

Conectarse a Redshift (base `analytics`, usuario `admin_analytics`, contraseña con `terraform output -raw redshift_admin_password`) y ejecutar el archivo generado.

### Ejecución local sin AWS

Se añadió `flink-app/src/main/java/com/portfolio/streaming/LocalDemoRunner.java`, que alimenta cinco eventos deterministas en memoria al mismo `WatermarkStrategy`, ventana de un minuto y funciones de agregación del job. Esta ejecución no necesita AWS, Kinesis ni LocalStack. Comprueba la transformación y el formato del resumen local; no prueba el conector Kinesis ni los commits del sink Iceberg en Glue/S3.

Para reproducirla desde la raíz del repositorio (Java 11+, Maven):

```bash
cd flink-app
mvn -q package -Dmaven.test.skip=true
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=compile
java -Djava.net.preferIPv4Stack=true \
  -cp "target/classes:target/flink-kinesis-processor.jar:$(cat cp.txt)" \
  com.portfolio.streaming.LocalDemoRunner
```

La última ejecución local finalizó correctamente el 2026-11-01 con OpenJDK 25.0.4.1. Flink mostró avisos de logging y `Unsafe` no fatales; el job terminó con código 0. La salida de agregación fue:

```text
temperature-summary> {"sensor_id":"sensor-1","window_start_ms":1793545380000,"window_end_ms":1793545440000,"count":5,"avg_temperature":23.5,"min_temperature":21.5,"max_temperature":25.5}
```

Los cinco valores de entrada fueron 21.5, 22.5, 23.5, 24.5 y 25.5. El resumen esperado y obtenido es count=5, promedio=23.5, mínimo=21.5 y máximo=25.5. El log íntegro de consola está guardado en `evidencia-ejecucion-local.txt`.


## Limpieza

```bash
cd terraform
terraform destroy
```
