import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//Consumer 1 - user notification service (Receives Kafka messages with information on suspicious transactions from the suspicious-transactions topic.)
public class Application {

    private static final String TOPIC2 = "suspicious-transactions";

    private static final String BOOTSTRAP_SERVER = "localhost:9092, localhost:9093, localhost:9094, localhost:9095";

    public static void main(String[] args) {
        Application kafkaConsumerApp = new Application();

        String consumerGroup = "user motification service group";

        if (args.length == 1) {
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        // Create consumer
        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVER, consumerGroup);

        kafkaConsumerApp.consumeMessages(TOPIC2, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        // To continually consume message from the topic
        while(true){
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if(consumerRecords.isEmpty()) {
                //do something
            }

            for(ConsumerRecord<String, Transaction> record: consumerRecords){
                //System.out.println(String.format("Record with (user name : %s ", record.key()));
                Transaction transaction = record.value();
                sendUserNotification(transaction);
            }

            //do some processing

            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        // To create a properties object
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        // Every consumer has to be part of consumer group
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup);
        // To manually commit to Kafka
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        return new KafkaConsumer<>(properties);
    }

    private static void sendUserNotification(Transaction transaction) {
        // Prints suspicious transaction information to the screen.
        System.out.println(String.format("Received received (key : %s, value : %s) ", transaction.getUser() ,transaction.toString()));
    }

}
