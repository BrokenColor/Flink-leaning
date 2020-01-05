package process;

import entity.RawData;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class C2InfRawData extends ProcessFunction<List<RawData>, RawData> {
    @Override
    public void processElement(List<RawData> rawDatas, Context context, Collector<RawData> out) {
        for (RawData rawData:rawDatas){
            out.collect(rawData);
        }
    }
}
