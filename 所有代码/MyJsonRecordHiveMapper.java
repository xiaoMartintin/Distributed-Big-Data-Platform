import org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper;
import org.apache.storm.tuple.Tuple;
import java.util.List;

public class MyJsonRecordHiveMapper extends JsonRecordHiveMapper {
@Override
public List<Object> mapRecord(Tuple tuple) {
List<Object> mapRecord = super.mapRecord(tuple);
// 如果需要，可以对mapRecord进行进一步处理
return mapRecord;
}
}
