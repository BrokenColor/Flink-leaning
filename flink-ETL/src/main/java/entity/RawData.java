package entity;

import lombok.Data;

/**
 * Description: kafka->influxdb 数据结构
 * <p>
 * Author: GWL
 * Date: Create in 20:36 2019/12/23
 */
@Data
public class RawData {
    //设备id
    String device_id;
    //租户id
    String tenant_id;
    //模板id
    String template_id;

    //数据来源（up设备上传，down云端下发）
    String direction;
    //kafka中消息ID
    String id;
    //机器id
    String equipment_id;
    //设备所属网关
    String gateway_id;
    //属性点编码
    String prop_id;

    long time;
    Object value;

}
