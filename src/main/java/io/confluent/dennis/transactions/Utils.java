package io.confluent.dennis.transactions;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {
    public static final String PROPERTIES_FILE_PATH = "src/main/resources/connection.properties";
    public static final short DEFAULT_REPLICATION_FACTOR = 3;

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/connection.properties")) {
            properties.load(fis);
            return properties;
        }
    }


    public static NewTopic createTopic(final String topicName, int partitions){
        return new NewTopic(topicName, partitions, DEFAULT_REPLICATION_FACTOR);
    }

    public static Callback producerCallback = (metadata, exception) -> {
        if(exception != null) {
            System.out.printf("Producing records encountered error %s %n", exception);
        } else {
            System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
        }
    };

}
