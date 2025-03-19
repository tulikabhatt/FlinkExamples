package org.example.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class OnlineSource {

    public static void main(String[] args) throws URISyntaxException {
        String kafkaTopic = "crypto-prices";
        String bootstrapServers = "localhost:9092";
        String streamUrl = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum";

        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // WebSocket client to consume data from Binance
        WebSocketClient wsClient = new WebSocketClient(new URI(streamUrl)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                System.out.println("Connected to Binance WebSocket");
            }

            @Override
            public void onMessage(String message) {
                System.out.println("Sending to Kafka: " + message);
                producer.send(new ProducerRecord<>(kafkaTopic, message));
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("WebSocket closed: " + reason);
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };

        // Start streaming
        wsClient.connect();

        // Keep the main thread running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            wsClient.close();
        }));
    }
}
