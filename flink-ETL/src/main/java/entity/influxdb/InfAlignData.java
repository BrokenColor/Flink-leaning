package entity.influxdb;


import lombok.Data;

import java.util.HashMap;

/**
 * Description: influxdb 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
@Data
public class InfAlignData {
    String name;

    Tags tags;
    HashMap<String, Object> fields;

    // 对齐时间戳
    Long time;
}
