package entity.kafka;

import lombok.Data;

/**
 * Description: Kafka 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
@Data
public class DataPointValueField {
    Long time;
    Object value;
}
