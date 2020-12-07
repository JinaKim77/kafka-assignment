import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {

    private static final String TOPIC1 = "valid-transactions";
    private static final String TOPIC2 = "suspicious-transactions";

    private static final String BOOTSTRAP_SERVER = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();

        Application kafkaApplication = new Application();
        Producer<String, Transaction> kafkaProducer = kafkaApplication.createKafkaProducer(BOOTSTRAP_SERVER);

        //Ctrl + Alt + T
        try {
            kafkaApplication.processTransactions(incomingTransactionsReader,customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally{
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {


        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction
        // location match or not.
        // Print record metadata information

        // I have 3 partitions for valid-transactions and 2 partitions for suspicious-transactions
        for(IncomingTransactionsReader reader = incomingTransactionsReader ; reader.hasNext();  )
        {
            Transaction usersInfo = reader.next();

            String key = usersInfo.getUser();  // Key: user (String)
            Transaction value = usersInfo;  // Value: Transaction

            // There are 2 topics (valid-transactions, suspicious-transactions)
            // Create records to be sent to Kafka
            //ProducerRecord<String, Transaction> record1 = new ProducerRecord<>(TOPIC1, key,value);
            //ProducerRecord<String, Transaction> record2 = new ProducerRecord<>(TOPIC2, key,value);

            //RecordMetadata recordMetadata1 = kafkaProducer.send(record1).get();  // to valid-transactions
            //RecordMetadata recordMetadata2 = kafkaProducer.send(record2).get();  // to suspicious-transactions

            //System.out.println(String.format("%s %f %s",usersInfo.getUser(),usersInfo.getAmount(),usersInfo.getTransactionLocation()));



            //Get locations
            String userTransactionLocation = usersInfo.getTransactionLocation();  // transaction location
            String residenceLo =  customerAddressDatabase.getUserResidence(key);  //home address

            //To test
            System.out.println(userTransactionLocation);
            System.out.println(residenceLo);

            //Compare user residence to transaction location.
            if(userTransactionLocation.equals(residenceLo)){
                System.out.println("locations match");
                //System.out.println("it's a valid transaction, the message should be sent to the valid -transactions topic");

                // There are 2 topics (valid-transactions, suspicious-transactions)
                // Create records to be sent to Kafka
                ProducerRecord<String, Transaction> record1 = new ProducerRecord<>(TOPIC1, key,value);
                RecordMetadata recordMetadata1 = kafkaProducer.send(record1).get();  // to valid-transactions

                //System.out.println(String.format("%s %f %s",usersInfo.getUser(),usersInfo.getAmount(),usersInfo.getTransactionLocation()));

                System.out.println(String.format("Record with (user name : %s, amount: %f, address :%s) was sent to -> valid -transactions topic", usersInfo.getUser(),usersInfo.getAmount(),usersInfo.getTransactionLocation()));
            }else{
                System.out.println("locations don't match");
                //System.out.println("it's a suspicious transaction, the message should be sent to the suspicious-transactions topic");

                // There are 2 topics (valid-transactions, suspicious-transactions)
                // Create records to be sent to Kafka
                ProducerRecord<String, Transaction> record2 = new ProducerRecord<>(TOPIC2, key,value);
                RecordMetadata recordMetadata2 = kafkaProducer.send(record2).get();  // to suspicious-transactions

                //System.out.println(String.format("%s %f %s",usersInfo.getUser(),usersInfo.getAmount(),usersInfo.getTransactionLocation()));

                System.out.println(String.format("Record with (user name : %s, amount: %f, address :%s) was sent to -> suspicious-transactions topic", usersInfo.getUser(),usersInfo.getAmount(),usersInfo.getTransactionLocation()));
            }
        }

    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        //to create a properties object
        Properties properties = new Properties();

        //to pass in the location of the bootstrap server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);

        //to give the producer name
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<String, Transaction>(properties);
    }

}
