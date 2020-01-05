package entity.influxdb;


import lombok.Data;

/**
 * Description: influxdb 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
@Data
public class Tags{
    String device_id;
    String direction;
    String equipment_id;
    String gateway_id;
    String tenant_id;
}
