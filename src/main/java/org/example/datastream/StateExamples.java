package org.example.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StateExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // ✅ Kafka Source (replace topic name as needed)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("crypto-prices")
                .setGroupId("flink-window-demo")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> priceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        // ✅ Parse JSON and extract (currency, price) tuples
        DataStream<Tuple2<String, Double>> parsedStream = priceStream.flatMap((String message, org.apache.flink.util.Collector<Tuple2<String, Double>> out) -> {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(message);
                jsonNode.fieldNames().forEachRemaining(field -> {
                    double price = jsonNode.get(field).asDouble();
                    out.collect(Tuple2.of(field, price));
                });
            } catch (Exception ignored) {}
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));

        // ✅ Tumbling window: Average price every 10 seconds
        SingleOutputStreamOperator<Tuple2<String, Double>> tumblingAvg = parsedStream
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageAggregate());

        // ✅ Sliding window: Average price over last 20s, sliding every 5s
        SingleOutputStreamOperator<Tuple2<String, Double>> slidingAvg = parsedStream
                .keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .aggregate(new AverageAggregate());

        // ✅ Session window: Dynamic average price with 10s inactivity gap
        SingleOutputStreamOperator<Tuple2<String, Double>> sessionAvg = parsedStream
                .keyBy(tuple -> tuple.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .aggregate(new AverageAggregate());

        // ✅ Print results
        tumblingAvg.print("Tumbling Window Avg (10s)");
        slidingAvg.print("Sliding Window Avg (20s window / 5s slide)");
        sessionAvg.print("Session Window Avg (gap 10s)");

        env.execute("Flink Windows Demonstration");
    }

    // ✅ Custom Aggregate Function to compute average price
    public static class AverageAggregate implements AggregateFunction<Tuple2<String, Double>, Tuple2<Double, Integer>, Tuple2<String, Double>> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(Tuple2<String, Double> value, Tuple2<Double, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<Double, Integer> accumulator) {
            double avg = accumulator.f1 == 0 ? 0.0 : accumulator.f0 / accumulator.f1;
            return Tuple2.of("Average", avg);
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
