import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.storm.tuple.Fields;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashSet;

public class DataCleansingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PrintWriter printWriter;
    private HashSet<String> modelIds = new HashSet<>();
    private int missingCount = 0;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            printWriter = new PrintWriter(new FileWriter("/home/hadoop/kafka/model_data.csv", true), true); // Append mode
        } catch (IOException e) {
            throw new RuntimeException("Error opening file", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String json = input.getStringByField("value");
        JSONObject jsonObject = new JSONObject(json);

        JSONObject config = jsonObject.optJSONObject("config");
        JSONObject cardData = jsonObject.optJSONObject("cardData");
        
        if (config == null) config = new JSONObject();
        if (cardData == null) cardData = new JSONObject();

        String modelId = jsonObject.optString("modelId", null);
        if (!modelIds.contains(modelId)) {
            modelIds.add(modelId);
            ModelData modelData = new ModelData(
                modelId,
                jsonObject.optInt("downloads", 0),
                jsonObject.optString("author", "unknown"),
                jsonObject.optString("pipeline_tag", null),
                jsonObject.optString("datasets", "unknown"),
                jsonObject.optString("library_name", "unknown"),
                jsonObject.optInt("likes", 0)
            );

            if (modelData.containsNull()) {
                missingCount++;
            } else {
                printWriter.printf("%s,%d,%s,%s,\"%s\",%s,%d\n",
                    modelData.modelId,
                    modelData.downloads,
                    modelData.author,
                    modelData.pipelineTag,
                    modelData.datasets,
                    modelData.libraryName,
                    modelData.likes
                );
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("model_id", "downloads", "author", "pipeline_tag", "datasets", "library_name", "likes"));
    }

    @Override
    public void cleanup() {
        if (printWriter != null) {
            printWriter.close();
        }
        System.out.println("Data cleansing complete, processed " + modelIds.size() + " items, " + missingCount + " items had missing data and were excluded.");
    }

    private static class ModelData {
        String modelId;
        int downloads;
        String author;
        String pipelineTag;
        String datasets;
        String libraryName;
        int likes;

        public ModelData(String modelId, int downloads, String author, String pipelineTag, String datasets, String libraryName, int likes) {
            this.modelId = modelId;
            this.downloads = downloads;
            this.author = author;
            this.pipelineTag = pipelineTag;
            this.datasets = datasets;
            this.libraryName = libraryName;
            this.likes = likes;
        }

        boolean containsNull() {
            return modelId == null || pipelineTag == null;
        }
    }
}

