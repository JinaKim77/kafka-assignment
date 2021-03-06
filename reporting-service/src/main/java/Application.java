import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

//Consumer 2 - reporting service (Receives Kafka messages with information on all transactions from the valid-transactions and suspicious-transactions topics)
public class Application {

    private static final String TOPIC1 = "valid-transactions";
    private static final String TOPIC2 = "suspicious-transactions";

    private static final String BOOTSTRAP_SERVER = "localhost:9092, localhost:9093, localhost:9094, localhost:9095";

    public static void main(String[] args) {
        Application kafkaConsumerApp = new Application();

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(TOPIC1);
        topics.add(TOPIC2);

        String consumerGroup = "reporting service group";

        if (args.length == 1) {
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        // Create consumer
        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVER, consumerGroup);

        kafkaConsumerApp.consumeMessages(topics, kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Arrays.asList(topics.get(0),topics.get(1)));


        // To continually consume message from the topic
        while(true){
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if(consumerRecords.isEmpty()) {
                //do something
            }

            for(ConsumerRecord<String, Transaction> record: consumerRecords){
                //System.out.println(String.format("Record with (user name : %s) received ", record.key()));
                Transaction transaction = record.value();
                String topic = record.topic();

                //Call this method to print
                Application.recordTransactionForReporting(topic, transaction);
            }

            //do some processing

            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        return new KafkaConsumer<>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print a different message depending on whether transaction is suspicious or valid
        System.out.println(String.format("Received received (key : %s, value : %s) from topic %s ", transaction.getUser() ,transaction.toString(), topic));
    }

}
