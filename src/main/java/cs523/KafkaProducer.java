package cs523;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static cs523.KafkaConstant.*;

public class KafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        String csvFilePath = "src/main/java/cs523/employees-data-set.csv";
        Producer<String, String> producer = getStringStringProducer();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int j = 1;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(j), line));
                producer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(j++), line), new KafkaMessage(System.currentTimeMillis(), String.valueOf(j++), line));
                System.out.println(line);
                Thread.sleep(1000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Producer<String, String> getStringStringProducer() {
        Properties kafkaProp = new Properties();
        kafkaProp.put("metadata.broker.list", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProp.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProp.put("client.id", KAFKA_CLIENT_ID);

        kafkaProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String > producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProp);
        return producer;
    }
}