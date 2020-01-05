package entity.kafka;

import lombok.Data;

/**
 * Description: Kafka 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
@Data
public class DataPointKey {
    String tenant_code;
    String template_code;
    String device_code;
}
