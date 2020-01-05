package source;

import com.alibaba.fastjson.JSON;
import config.Config;
import entity.RawData;
import entity.kafka.DataPointValue;
import entity.kafka.DataPointValueField;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sink.InfAlignDataBatchSink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Description: Kafka 数据源
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
public class KafkaSource implements Serializable {
    public static Logger logger = LoggerFactory.getLogger(InfAlignDataBatchSink.class);

    public FlinkKafkaConsumer<List<RawData>> genSource() {
        Properties properties = new Properties();
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.sourceKafkaURL);
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
//        properties.setProperty("zookeeper.connect", "hadoop01:2181");
        //auto.commit.interval.ms
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Config.sourceKafkaAutoCommitIntervalMs);

//        properties.put("enable.auto.commit", Config.sourceKafkaEnableAutoCommit);
        //只要将group.id换成全新的，不论"auto.offset.reset”是否设置，设置成什么，都会从最新的位置开始消费
        //从最新的offset开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Config.sourceKafkaAutoOffsetReset);
        // 在一次调用poll（）中返回的最大记录数。默认是500
//        properties.put("max.poll.records", 5);
        //kafka消费者在一次poll内，业务处理时间不能超过这个时间,默认是300s
//        properties.put("max.poll.interval.ms", 6000);

////        //设置消费者心跳间隔
//        properties.put("heartbeat.interval.ms",3000);
//        properties.put("session.timeout.ms", 5000);

        //flink consumer flink的消费者的group.id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Config.sourceKafkaGroupID);
        System.out.println("kafka 配置");
        FlinkKafkaConsumer<List<RawData>> consumer =
                new FlinkKafkaConsumer<>(Config.sourceKafkaDatapointTopic,
                        new MessageDeserialize(), properties);
        return consumer;
    }

    private class MessageDeserialize implements KafkaDeserializationSchema<List<RawData>> {

        @Override
        public List<RawData> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
//            System.out.println("deserialize DataPoint.key:" + consumerRecord.key().toString());
            List<RawData> rawDataList = new ArrayList<>();
            RawData rawData;
//            System.out.println("deserialize DataPoint.time:" + consumerRecord.timestamp());
            String key = new String(consumerRecord.key());
            String[] keys = key.split("/");
            String tenant_id = "";
            String template_id = "";
            String device_id = "";
            if (keys.length == 3) {
                tenant_id = keys[0];
                template_id = keys[1];
                device_id = keys[2];
            }
            DataPointValue dataPointValue = JSON.parseObject(new String(consumerRecord.value()),DataPointValue.class);
            for (Map.Entry<String,DataPointValueField> entry: dataPointValue.getFields().entrySet()){
                rawData = new RawData();
                rawData.setDevice_id(device_id);
                rawData.setTemplate_id(template_id);
                rawData.setTenant_id(tenant_id);
                rawData.setDirection("up");
                rawData.setEquipment_id(dataPointValue.getEquipment());
                rawData.setGateway_id(dataPointValue.getGateway());
                rawData.setId(dataPointValue.getId());
                rawData.setProp_id(entry.getKey());
                rawData.setTime(entry.getValue().getTime());
                rawData.setValue(entry.getValue().getValue());
                rawDataList.add(rawData);
            }
//            System.out.println("deserialize DataPoint.value:" + consumerRecord.value().toString());
            return rawDataList;
        }

        @Override
        public TypeInformation<List<RawData>> getProducedType() {
            return Types.LIST(Types.POJO(RawData.class));
        }

        @Override
        public boolean isEndOfStream(List<RawData> RawDatas) {
            return false;
        }
    }

    public KafkaSource() {
    }
}
