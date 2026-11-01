package com.portfolio.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Runs deterministic in-memory sensor events through the local Flink aggregation. */
public final class LocalDemoRunner {
    private LocalDemoRunner() { }

    public static void main(String[] args) throws Exception {
        long start = Instant.parse("2026-11-01T15:03:00Z").toEpochMilli();
        List<SensorAggregationJob.SensorEvent> events = List.of(
                event(start + 5_000, 21.5), event(start + 10_000, 22.5),
                event(start + 15_000, 23.5), event(start + 20_000, 24.5),
                event(start + 25_000, 25.5));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromCollection(events)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorAggregationJob.SensorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, previous) -> event.eventTimeMillis))
                .keyBy(event -> event.sensorId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SensorAggregationJob.TemperatureAggregate(),
                        new SensorAggregationJob.WindowMetadata())
                .name("sensor-temperature-window-aggregation-local-demo")
                .print("temperature-summary");

        env.execute("sensor-temperature-aggregation-local-demo");
    }

    private static SensorAggregationJob.SensorEvent event(long timestamp, double temperature) {
        return new SensorAggregationJob.SensorEvent("sensor-1", temperature, timestamp);
    }
}
