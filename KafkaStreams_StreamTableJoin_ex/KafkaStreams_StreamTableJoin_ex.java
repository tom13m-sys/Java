package com.example.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamEnrichmentExample {

    public static void main(String[] args) {
        // 1. Configure the Kafka Streams Application
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-enrichment-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // Disable caching temporarily if you want to see updates instantly during testing
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // 2. Build the Topology
        StreamsBuilder builder = new StreamsBuilder();

        // Source 1: KTable representing the user profile lookup data (from DB via Kafka Connect)
        // Expected message format: Key = "userId123", Value = "123 Main St, New York"
        KTable<String, String> userProfilesTable = builder.table(
                "db-user-profiles-topic",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Source 2: KStream representing the high-throughput live event stream
        // Expected message format: Key = "userId123", Value = "Purchase: Laptop - $999"
        KStream<String, String> transactionStream = builder.stream(
                "transactions-topic",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Step 3: Perform the KStream-KTable Join
        // This is an inner join. Events are only forwarded if the userId exists in the KTable.
        KStream<String, String> enrichedStream = transactionStream.join(
                userProfilesTable,
                // ValueJoiner defines how to combine the stream value and table value
                (transactionValue, addressValue) -> {
                    return String.format("[Transaction: %s] -> [Shipping Address: %s]", 
                            transactionValue, addressValue);
                },
                // Configuration specifying Serdes for the join operation
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        // Step 4: Write the enriched data to an output topic
        enrichedStream.to("enriched-transactions-topic", 
                Produced.with(Serdes.String(), Serdes.String()));

        // 5. Start the Application
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        
        // Add a shutdown hook to close the streams gracefully
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
            System.out.println("Streams application safely stopped.");
        }));

        try {
            System.out.println("Starting Kafka Streams enrichment pipeline...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
