import entity.RawData;
import entity.influxdb.InfAlignData;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import process.AlignProcess;
import sink.InfAlignDataBatchSink;
import source.KafkaSource;

import java.util.List;

/**
 * Description: Kafka->flink->influxdb 冀北数据对齐
 * <p>
 * Author: GWL
 * Date: Create in 15:04 2019/12/23
 */
public class ETLTask {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        //将kafka生产者发来的数据进行处理，本例子我进任何处理
        DataStream<List<RawData>> sourceStream = env.addSource(new KafkaSource().genSource());
        System.out.println("kafka 数据源添加");
//        DataStream<RawData> rasDataStream = keyedStream.process(new C2InfRawData()).setParallelism(1).name("list-rawData");
//        keyedStream.print();//直接将从生产者接收到的数据在控制台上进行打
        DataStream<InfAlignData> infStream = sourceStream.filter(list->list.size()>0)
                .keyBy(list->list.get(0).getDevice_id())
                .process(new AlignProcess())
                .setParallelism(1)
                .name("数据转换");
        infStream.addSink(new InfAlignDataBatchSink())
                .setParallelism(1)
                .name("写入influxdb");
        // execute program
        System.out.println("写入influxdb");
//        infStream.print();
        System.out.println("数据控制台打印");
        env.execute("Flink Streaming Java API Skeleton");
    }
}
