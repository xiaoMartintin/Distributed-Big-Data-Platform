import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.HashSet;

public class KafkaJsonProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "model-data";
        HashSet<String> modelIds = new HashSet<>();

        try {
            InputStream inputStream = KafkaJsonProducer.class.getClassLoader().getResourceAsStream("hf_metadata_small.json");
            if (inputStream == null) {
                throw new IllegalArgumentException("file not found!");
            } else {
                JsonFactory jsonFactory = new JsonFactory();
                JsonParser jsonParser = jsonFactory.createParser(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

                ObjectMapper objectMapper = new ObjectMapper();
                if (jsonParser.nextToken() == JsonToken.START_ARRAY) {
                    while (jsonParser.nextToken() == JsonToken.START_OBJECT) {
                        JsonNode modelInfo = objectMapper.readTree(jsonParser);
                        String modelId = modelInfo.path("modelId").asText();
                        if (!modelIds.contains(modelId)) {
                            modelIds.add(modelId);
                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, modelInfo.toString());
                            producer.send(record);
                            System.out.println("Model info sent successfully for modelId: " + modelId);

                            // 设置发送间隔
                            Thread.sleep(0, 100); // 等待100ns
                        } else {
                            System.out.println("Duplicate modelId skipped: " + modelId);
                        }
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted during sleep");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
                System.out.println("Producer has been closed successfully.");
            }
        }
    }
}
