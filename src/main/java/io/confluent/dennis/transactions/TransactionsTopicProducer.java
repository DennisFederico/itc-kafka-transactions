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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsTopicProducer {

    public static void main(String[] args) throws IOException, InterruptedException {
        prepareTransactionsTopic();
    }

    static void prepareTransactionsTopic() throws IOException, InterruptedException {
        Properties properties = Utils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        ThreadLocalRandom random = ThreadLocalRandom.current();

        try (Producer<String, Transaction> producer = new KafkaProducer<>(properties)) {
            final String transactionsTopic = properties.getProperty("transactions.topic");

            int numRecords = 0;

            while (numRecords <= 35) {
                numRecords++;
                int tx = numRecords + 6;
                int client1 = Math.abs(random.nextInt() % 4);
                int client2 = Math.abs(random.nextInt() % 4);
                //Poison pill
                double amount = numRecords==30 ? -1000 : random.nextInt(0, 1000000) / Math.pow(10, random.nextInt(0, 3));
                Transaction transaction = new Transaction("tx-" + tx, "client-" + client1, "client-" + client2, amount);
                producer.send(new ProducerRecord<>(transactionsTopic, transaction.getTxId(), transaction), Utils.producerCallback);
                Thread.sleep(250);
            }
        }
    }
}
