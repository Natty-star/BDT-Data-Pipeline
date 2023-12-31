package cs523;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.logging.Logger;

public class KafkaMessage implements Callback {

    private static final Logger LOGGER = Logger.getLogger(KafkaMessage.class.getName());

    private final long startTime;
    private final String key;
    private final String message;

    public KafkaMessage(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            String streamedData = "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +"), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms";
            LOGGER.info(streamedData);

        } else {
            LOGGER.warning("Exception: " + exception.getMessage());
        }
    }
}