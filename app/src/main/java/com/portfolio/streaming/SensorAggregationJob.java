package com.portfolio.streaming;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.types.Types;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class SensorAggregationJob {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties properties = groups.getOrDefault("FlinkApplicationProperties", new Properties());
        Map<String, String> propertyMap = new HashMap<>();
        for (String key : properties.stringPropertyNames()) propertyMap.put(key, properties.getProperty(key));
        ParameterTool params = ParameterTool.fromMap(propertyMap).mergeWith(ParameterTool.fromArgs(args));

        String streamName = required(params, "STREAM_NAME", "STREAM_NAME");
        String region = params.get("AWS_REGION", System.getenv().getOrDefault("AWS_REGION", "us-east-2"));
        int parallelism = params.getInt("PARALLELISM", 2);
        long windowMinutes = params.getLong("WINDOW_MINUTES", 1);
        long outOfOrderSeconds = params.getLong("OUT_OF_ORDER_SECONDS", 10);
        long checkpointInterval = params.getLong("CHECKPOINT_INTERVAL_MS", 60_000L);
        String warehouse = required(params, "LAKEHOUSE_WAREHOUSE", "LAKEHOUSE_WAREHOUSE");
        String database = params.get("GLUE_DATABASE", "lakehouse_db");
        String tableName = params.get("ICEBERG_TABLE", "sensor_window_summaries");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval);

        Properties sourceProperties = new Properties();
        sourceProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        sourceProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.name());
        DataStream<String> records = env.addSource(new FlinkKinesisConsumer<>(streamName,
                new SimpleStringSchema(), sourceProperties)).name("kinesis-sensor-events");

        DataStream<Summary> summaries = records.map(SensorAggregationJob::parse).returns(SensorEvent.class)
                .filter(event -> event != null)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSeconds))
                        .withTimestampAssigner((event, previous) -> event.eventTimeMillis))
                .keyBy(event -> event.sensorId)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowMinutes)))
                .aggregate(new TemperatureAggregate(), new WindowMetadata())
                .name("sensor-temperature-window-aggregation");

        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "sensor_id", Types.StringType.get()),
                Types.NestedField.required(2, "window_start", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "window_end", Types.TimestampType.withoutZone()),
                Types.NestedField.required(4, "event_count", Types.LongType.get()),
                Types.NestedField.required(5, "avg_temperature", Types.DoubleType.get()),
                Types.NestedField.required(6, "min_temperature", Types.DoubleType.get()),
                Types.NestedField.required(7, "max_temperature", Types.DoubleType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergSchema).day("window_start").build();
        TableIdentifier tableId = TableIdentifier.of(Namespace.of(database), tableName);

        // Glue database is provisioned by Terraform. Create the Iceberg table once on first job start.
        GlueCatalog catalog = new GlueCatalog();
        Map<String, String> catalogProperties = Map.of("warehouse", warehouse, "client.region", region);
        catalog.initialize("glue", catalogProperties);
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, icebergSchema, partitionSpec,
                    Map.of("location", warehouse + "/" + database + ".db/" + tableName));
        }
        catalog.close();

        CatalogLoader catalogLoader = CatalogLoader.custom("glue", catalogProperties,
                new org.apache.hadoop.conf.Configuration(), "org.apache.iceberg.aws.glue.GlueCatalog");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
        org.apache.flink.api.common.typeinfo.TypeInformation<RowData> rowDataType =
                FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(icebergSchema));
        MapFunction<Summary, RowData> toRowData = summary -> GenericRowData.of(
                        StringData.fromString(summary.sensorId),
                        TimestampData.fromEpochMillis(summary.windowStartMillis),
                        TimestampData.fromEpochMillis(summary.windowEndMillis),
                        summary.count,
                        summary.sum / summary.count,
                        summary.min,
                        summary.max);
        DataStream<RowData> icebergRows = summaries.map(toRowData)
                .returns(rowDataType)
                .name("map-aggregates-to-iceberg-rowdata");
        FlinkSink.forRowData(icebergRows).tableLoader(tableLoader).append();

        env.execute("sensor-temperature-aggregation");
    }

    private static String required(ParameterTool params, String key, String environmentVariable) {
        String value = params.get(key, System.getenv(environmentVariable));
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing application property: " + key);
        return value;
    }

    private static SensorEvent parse(String text) {
        try {
            JsonNode json = JSON.readTree(text);
            String sensorId = json.path("sensor_id").asText("");
            JsonNode temperature = json.path("temperatura");
            if (sensorId.isBlank() || !temperature.isNumber()) return null;
            JsonNode time = json.has("event_time") ? json.path("event_time") : json.path("timestamp");
            long millis = time.isNumber() ? time.asLong()
                    : time.isTextual() ? Instant.parse(time.asText()).toEpochMilli() : System.currentTimeMillis();
            return new SensorEvent(sensorId, temperature.asDouble(), millis);
        } catch (Exception ignored) {
            return null;
        }
    }

    public static final class SensorEvent {
        public String sensorId;
        public double temperature;
        public long eventTimeMillis;
        public SensorEvent() { }
        SensorEvent(String sensorId, double temperature, long eventTimeMillis) {
            this.sensorId = sensorId; this.temperature = temperature; this.eventTimeMillis = eventTimeMillis;
        }
    }

    public static final class Summary {
        public String sensorId;
        public long windowStartMillis;
        public long windowEndMillis;
        public long count;
        public double sum;
        public double min;
        public double max;
        public Summary() { }
        @Override public String toString() {
            return "{\"sensor_id\":\"" + sensorId + "\",\"window_start_ms\":" + windowStartMillis
                    + ",\"window_end_ms\":" + windowEndMillis + ",\"count\":" + count
                    + ",\"avg_temperature\":" + (sum / count) + ",\"min_temperature\":" + min
                    + ",\"max_temperature\":" + max + "}";
        }
    }

    public static final class TemperatureAggregate implements AggregateFunction<SensorEvent, Summary, Summary> {
        @Override public Summary createAccumulator() { return new Summary(); }
        @Override public Summary add(SensorEvent event, Summary acc) {
            if (acc.count == 0) { acc.sensorId = event.sensorId; acc.min = event.temperature; acc.max = event.temperature; }
            acc.count++; acc.sum += event.temperature; acc.min = Math.min(acc.min, event.temperature);
            acc.max = Math.max(acc.max, event.temperature); return acc;
        }
        @Override public Summary getResult(Summary acc) { return acc; }
        @Override public Summary merge(Summary a, Summary b) {
            a.count += b.count; a.sum += b.sum; a.min = Math.min(a.min, b.min); a.max = Math.max(a.max, b.max);
            if (a.sensorId == null) a.sensorId = b.sensorId; return a;
        }
    }

    public static final class WindowMetadata extends ProcessWindowFunction<Summary, Summary, String, TimeWindow> {
        @Override public void process(String key, Context context, Iterable<Summary> input, Collector<Summary> out) {
            Summary result = input.iterator().next();
            result.sensorId = key;
            result.windowStartMillis = context.window().getStart();
            result.windowEndMillis = context.window().getEnd();
            out.collect(result);
        }
    }
}
