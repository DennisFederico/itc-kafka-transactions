package io.confluent.dennis.transactions;

import io.confluent.dennis.transactions.model.Transaction;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class TransactionsTopicLoader {

    public static void main(String[] args) throws IOException {
        prepareTransactionsTopic();
    }

    static void prepareTransactionsTopic() throws IOException {
        Properties properties = Utils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        try (Admin adminClient = Admin.create(properties);
             Producer<String, Transaction> producer = new KafkaProducer<>(properties)) {
            final String transactionsTopic = properties.getProperty("transactions.topic");
            final String movementsTopic = properties.getProperty("account.movements.topic");
            final String debitsTopic = properties.getProperty("debits.movements.topic");
            final String creditsTopic = properties.getProperty("credits.movements.topic");

            adminClient.createTopics(
                    List.of(
                            Utils.createTopic(transactionsTopic, 1),
                            Utils.createTopic(movementsTopic, 1),
                            Utils.createTopic(debitsTopic, 1),
                            Utils.createTopic(creditsTopic, 1)
                    )
            );

            List<Transaction> transactions = List.of(
                    new Transaction("tx-1", "client-1", "client-2", 1234.5),
                    new Transaction("tx-2", "client-2", "client-1", 500),
                    new Transaction("tx-3", "client-3", "client-2", 250),
                    new Transaction("tx-4", "client-1", "client-2", 89.99),
                    new Transaction("tx-5", "client-2", "client-3", 100)
            );

            transactions.stream()
                    .map(t -> new ProducerRecord<>(transactionsTopic, t.getTxId(), t))
                    .forEach(r -> producer.send(r, Utils.producerCallback));

        }
    }
}
