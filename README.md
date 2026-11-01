# AWS real-time sensor processing

Pipeline incremental de procesamiento de sensores con Kinesis Data Streams, Firehose, S3 y Managed Service for Apache Flink. Este repositorio es independiente y se inicializa desde cero con los archivos de este proyecto.

## Arquitectura

```text
Productor → Kinesis Data Streams ──┬──→ Firehose → S3 (eventos raw)
                                  └──→ Managed Flink → CloudWatch Logs (agregados)
```

Terraform crea la clave KMS, el stream cifrado, el bucket privado del data lake, Firehose y la aplicación Flink. El JAR se publica en el bucket antes de crear la aplicación. Flink consume el stream directamente; no lee la copia de Firehose en S3.

## Lógica de negocio

Cada evento JSON debe incluir `sensor_id` y `temperatura`. Flink agrupa por sensor en ventanas tumbling de tiempo de evento de un minuto y escribe en CloudWatch Logs un resumen con cantidad, promedio, mínimo y máximo.

El job acepta `event_time` o `timestamp` como epoch en milisegundos o timestamp ISO-8601. Cuando falta el campo temporal, usa la hora de ingestión como respaldo. El `WatermarkStrategy` acepta hasta diez segundos de desorden. Los acumuladores se guardan en el estado keyed de la ventana. Los checkpoints están habilitados cada 60 segundos.

## Paralelismo y KPUs

La configuración inicial asigna dos shards al stream y paralelismo global 2 al job, para consumir ambas particiones. `parallelism_per_kpu = 1` configura una unidad de paralelismo por KPU; el uso real de KPUs depende de la carga del grafo. El autoscaling queda deshabilitado para mantener la capacidad y el costo previsibles. Cambiá estos valores en `terraform/variables.tf` o mediante `terraform.tfvars`.

La aplicación no requiere asociación con una VPC: lee Kinesis y escribe logs mediante los endpoints de servicio de AWS. Si se agrega un destino privado en una VPC, hay que configurar subredes, security groups y conectividad de salida a ese servicio.

## Compilar y desplegar

Requisitos: JDK 11 o posterior, Maven 3.8+, Terraform 1.5+ y AWS CLI con permisos para crear Kinesis, KMS, S3, IAM, CloudWatch, Firehose y Managed Flink.

```bash
cd flink
mvn clean package
cd ../terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Para iniciar la aplicación después del `apply`:

```bash
aws kinesisanalyticsv2 start-application \
  --application-name realtime-sensors-dev-flink-aggregation \
  --run-configuration 'ApplicationRestoreConfiguration={ApplicationRestoreType=SKIP_RESTORE}' \
  --region us-east-2
```

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

### Bloqueo de la cuenta de AWS

Esta cuenta de AWS (683590131550) fue creada por el autor del proyecto, conectada con GitHub, sin ninguna organización asignada por el curso. AWS aplica automáticamente guardrails (Service Control Policies) a cuentas nuevas como medida antifraude hasta que la cuenta pasa cierta verificación. En este caso, la SCP `p-uyxw8uhp` deniega explícitamente `kinesis:CreateStream` (y otras acciones de Kinesis) en todas las regiones probadas (us-east-2, us-west-2, eu-west-1), lo que impide desplegar el stream real que necesita este módulo:

```
An error occurred (AccessDeniedException) when calling the CreateStream operation:
User: arn:aws:iam::683590131550:user/terraform-curso is not authorized to perform:
kinesis:CreateStream on resource: arn:aws:kinesis:us-east-2:683590131550:stream/...
with an explicit deny in a service control policy:
arn:aws:organizations::328064416027:policy/o-pqdyvbpw97/service_control_policy/p-uyxw8uhp
```

Se abrió un caso de soporte con AWS pidiendo el levantamiento de esa restricción. Mientras se resuelve, la evidencia de ejecución de este módulo se generó localmente (ver debajo). Cuando AWS levante el bloqueo, hay que desplegar con `terraform apply` y correr `scripts/capture-evidence.sh` (ver más abajo) para generar la evidencia real desde CloudWatch.

### Evidencia generada localmente (LocalStack)

Para probar que el job de Flink funciona end-to-end (consumo de Kinesis, watermark, ventana tumbling, agregación por sensor) sin depender de la cuenta de AWS bloqueada, se emuló Kinesis con [LocalStack](https://www.localstack.cloud/) y se corrió el mismo pipeline de agregación con una clase auxiliar (`flink/src/main/java/com/portfolio/streaming/LocalRunner.java`) que apunta al endpoint local en vez de al Kinesis real. La lógica de negocio (parseo, watermark, ventana, agregación) es idéntica a `SensorAggregationJob`; `LocalRunner` solo cambia la configuración de conexión del consumer.

**Cómo reproducirlo:**

```bash
# 1. Levantar Kinesis emulado
docker compose -f docker-compose.local.yml up -d

# 2. Crear el stream
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
  --stream-name realtime-sensors-dev-events --shard-count 1 --region us-east-2

# 3. Compilar y correr el job local (Java 17 requerido; Flink 1.18 no soporta Java 25+)
cd flink
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=compile
nohup java -cp "target/classes:$(cat cp.txt)" com.portfolio.streaming.LocalRunner \
  > ../flink-local-run.log 2>&1 &
cd ..

# 4. Mandar eventos de prueba (repetir en dos tandas separadas por >1 minuto real,
#    para que el watermark avance y cierre la ventana del primer minuto)
aws --endpoint-url=http://localhost:4566 kinesis put-record \
  --stream-name realtime-sensors-dev-events --partition-key sensor-1 \
  --data '{"sensor_id":"sensor-1","temperatura":21.5}' \
  --cli-binary-format raw-in-base64-out --region us-east-2

# 5. Ver el resultado
tail -f flink-local-run.log
```

**Salida real obtenida** (`evidencia-ejecucion-local.txt`):

```
temperature-summary> {"sensor_id":"sensor-1","window_start_ms":1793540760000,"window_end_ms":1793540820000,"count":5,"avg_temperature":23.5,"min_temperature":21.5,"max_temperature":25.5}
``` 
Esto confirma: consumo del stream de Kinesis, aplicación del `WatermarkStrategy` con tolerancia a desorden, ventana `TumblingEventTimeWindows` de 1 minuto por `sensor_id`, y agregación correcta (5 eventos con temperaturas 21.5–25.5 → promedio 23.5, mínimo 21.5, máximo 25.5).

### Evidencia real en AWS (pendiente del desbloqueo)

Una vez resuelto el caso de soporte y desplegada la aplicación con `terraform apply`, correr desde la raíz del repo:

```bash
scripts/capture-evidence.sh
```

El archivo resultante queda en `evidence/flink-cloudwatch.txt`, generado directamente por AWS CLI contra la cuenta real (no se reemplaza con salida simulada ni de LocalStack, ver `evidence/README.md`).

## Limpieza

```bash
cd terraform
terraform destroy
```
