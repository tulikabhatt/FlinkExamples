package org.example.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformationExamples {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Single Data Source: Names and Ages in String format
        DataStream<String> sourceStream = env.fromData(
                "Tom Holland,23", "Michael John,75", "Sam Smith,20", "Max Viktor,55", "Harry,62",
                "Sally Kim,68", "Tom Andrews,30", "Max Krum,40", "Michael John,70", "Tina Li, 45"
        );

        // 1️⃣ Map Transformation: Convert all text to uppercase
        DataStream<String> upperCaseStream = sourceStream.map(String::toUpperCase);

        // 2️⃣ Filter Transformation: Extract People older than 50
        DataStream<Person> personStream = sourceStream
                .filter(s -> s.contains(","))  // Ensure it contains an age
                .map(new MapFunction<String, Person>() {
                    @Override
                    public Person map(String value) {
                        String[] parts = value.split(",");
                        return new Person(Integer.parseInt(parts[1].trim()), parts[0].trim());
                    }
                });

        DataStream<Integer> ageStream = personStream
                .filter(person -> person.getAge() > 50)
                .map(Person::getAge);

        // 3️⃣ FlatMap Transformation: Split names into words
        DataStream<String> wordsStream = personStream.flatMap(new FlatMapFunction<Person, String>() {
            @Override
            public void flatMap(Person person, Collector<String> out) {
                for (String word : person.getName().split(" ")) {
                    out.collect(word);
                }
            }
        });

        // 4️⃣ KeyBy & Reduce: Find max age per name
        SingleOutputStreamOperator<Person> maxAgePerName = personStream
                .map(person -> new Person(person.getAge(), person.getName().split(" ")[0]))  // Normalize before keyBy
                .keyBy(person -> person.getName().split(" ")[0])  // Key by first name only
                .reduce((p1, p2) -> new Person(Math.max(p1.getAge(), p2.getAge()), p1.getName()));  // Retain max age

        // 5️⃣ Union Transformation: Merge words stream with another stream
        DataStream<String> stream2 = env.fromData("Extra1", "Extra2");
        DataStream<String> mergedStream = wordsStream.union(stream2);

        // Print outputs
//        upperCaseStream.print("Upper Case Stream");
//        ageStream.print("Ages > 50");
//        wordsStream.print("Words Stream");
        maxAgePerName.print("Max Age per Name");
//        mergedStream.print("Merged Stream");

        // Execute Flink pipeline
        try {
            env.execute("Transformations Examples");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
