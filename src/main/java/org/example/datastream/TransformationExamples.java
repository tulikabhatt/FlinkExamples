package org.example.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        // Map Transformation: Convert all text to uppercase
        DataStream<String> upperCaseStream = sourceStream.map(String::toUpperCase);

        // Filter Transformation: Extract People older than 50
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

        // FlatMap Transformation: Split names into words
        DataStream<String> wordsStream = personStream.flatMap(new FlatMapFunction<Person, String>() {
            @Override
            public void flatMap(Person person, Collector<String> out) {
                for (String word : person.getName().split(" ")) {
                    out.collect(word);
                }
            }
        });

        // Union Transformation: Merge words stream with another stream
        DataStream<String> stream2 = env.fromData("Extra1", "Extra2");
        DataStream<String> mergedStream = wordsStream.union(stream2);

        // Print outputs
        upperCaseStream.print("Upper Case Stream");
        ageStream.print("Ages > 50");
        wordsStream.print("Words Stream");
        mergedStream.print("Merged Stream");

        // Execute Flink pipeline
        try {
            env.execute("Transformations Examples");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
