import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaStormTopology {
public static void main(String[] args) {
    try {
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder("localhost:9092", "model-data")
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "model-data-group")
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(spoutConfig));
        builder.setBolt("data-cleansing-bolt", new DataCleansingBolt()).shuffleGrouping("kafka-spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-storm-topology", config, builder.createTopology());
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
