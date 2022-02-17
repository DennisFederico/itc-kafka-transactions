package io.confluent.dennis.transactions;

import io.confluent.dennis.transactions.model.Transaction;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TransactionsTopicTearDown {

    public static void main(String[] args) throws IOException {
        tearDownTopics();
    }

    static void tearDownTopics() throws IOException {
        Properties properties = Utils.loadProperties();
        try (Admin adminClient = Admin.create(properties)) {
            final String transactionsTopic = properties.getProperty("transactions.topic");
            final String movementsTopic = properties.getProperty("account.movements.topic");
            final String debitsTopic = properties.getProperty("debits.movements.topic");
            final String creditsTopic = properties.getProperty("credits.movements.topic");
            final String groupId = properties.getProperty("group.id");

            adminClient.deleteTopics(List.of(transactionsTopic, movementsTopic, debitsTopic, creditsTopic));
            adminClient.deleteConsumerGroups(Collections.singleton(groupId));
        }
    }
}
