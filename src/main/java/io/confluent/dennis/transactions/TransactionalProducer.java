package io.confluent.dennis.transactions;

import io.confluent.dennis.transactions.model.Movement;
import io.confluent.dennis.transactions.model.Transaction;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class TransactionalProducer {

    public static void main(String[] args) throws IOException {
        processTransactions();
    }

    static void processTransactions() throws IOException {
        Properties properties = Utils.loadProperties();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Transaction.class.getName());
        //properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG, "tx-consumer-grp"); //Already set in properties file

        final String transactionsTopic = properties.getProperty("transactions.topic");
        final String movementsTopic = properties.getProperty("account.movements.topic");
        final String debitsTopic = properties.getProperty("debits.movements.topic");
        final String creditsTopic = properties.getProperty("credits.movements.topic");
        final String consumerGroup = properties.getProperty("group.id");

        try (Producer<String, Object> producer = new KafkaProducer<>(properties);
             KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(properties)) {

            producer.initTransactions();
            consumer.subscribe(Collections.singleton(transactionsTopic));
            try {
                while (true) {
                    ConsumerRecords<String, Transaction> transactionRecords = consumer.poll(Duration.ofMillis(1000));
                    if (!transactionRecords.isEmpty()) {

                        producer.beginTransaction();

                        for (ConsumerRecord<String, Transaction> record : transactionRecords) {
                            System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
                            System.out.println("Key: " + record.key() + ", Value: " + record.value());
                            System.out.println("----------------------------------------------------------------");

                            Transaction value = record.value();
                            Movement debit = new Movement(value.getFirstPartyId(), value.getTxId(), "DEBIT", -value.getAmount());
                            producer.send(new ProducerRecord<>(debitsTopic, debit.getAccountId(), debit));
                            producer.send(new ProducerRecord<>(movementsTopic, debit.getAccountId(), debit));
                            Movement credit = new Movement(value.getSecondPartyId(), value.getTxId(), "CREDIT", value.getAmount());
                            producer.send(new ProducerRecord<>(creditsTopic, credit.getAccountId(), credit));
                            producer.send(new ProducerRecord<>(movementsTopic, credit.getAccountId(), credit));

//                            //CUSTOM LOGIC TO FORCE AN EXCEPTION
//                            if (value.getAmount() < 0) {
//                                throw new IOException("BROKEN RECORD");
//                            }
                        }

                        producer.sendOffsetsToTransaction(consumerOffsets(transactionRecords), consumerGroup);
                        producer.commitTransaction();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
                producer.abortTransaction();
            }
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerOffsets(ConsumerRecords<String, Transaction> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, Transaction>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }
}
