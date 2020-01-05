package entity.kafka;

import lombok.Data;

import java.util.Map;

/**
 * Description: Kafka 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
@Data
public class DataPointValue {
    String id;
    String gateway;
    String equipment;
    Map<String, DataPointValueField> fields;
}
