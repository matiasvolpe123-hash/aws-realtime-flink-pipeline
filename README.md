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

La evidencia de ejecución consiste en una captura del grafo de la aplicación en la consola Managed Flink o logs de CloudWatch que muestren `temperature-summary` y sus métricas. No se incluye una captura simulada.

Para guardar el estado de la aplicación y los logs recientes en un archivo compartible, corré el script desde la raíz del repositorio:

```bash
scripts/capture-evidence.sh
```

El archivo resultante queda en `evidence/flink-cloudwatch.txt`. Enviámelo cuando esté generado para añadirlo al repositorio como evidencia.

## Limpieza

```bash
cd terraform
terraform destroy
```
