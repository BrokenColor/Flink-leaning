package process;

import entity.RawData;
import entity.influxdb.InfAlignData;
import entity.influxdb.Tags;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * @Auther dier
 * @Date 10/10/2019 17:08
 * @Description 数据对齐
 */
public class AlignProcess extends KeyedProcessFunction<String, List<RawData>, InfAlignData> {
    public static Logger logger = LoggerFactory.getLogger(AlignProcess.class);

    private MapState<Long, InfAlignData> mapState;
    private ValueState<Long> minTime;
    private ValueState<InfAlignData> lastData;

    // 对齐周期
    private Long deltaTime = 1000L * 5;
    // 最大延迟
    private Long latenTime = deltaTime * 6;
    // 数据对齐周期
    private String cycle = "5s";

    public AlignProcess() {
    }

    public AlignProcess(String cycle) {
        this.cycle = cycle;
        switch (this.cycle) {
            case "5s":
                this.deltaTime = 1000L * 5;
                break;
            case "1h":
                this.deltaTime = 1000L * 60 * 60;
                break;
            default:
                break;
        }
    }

    public String getCycle() {
        return cycle;
    }

    public void setCycle(String cycle) {
        this.cycle = cycle;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext rc = getRuntimeContext();
        mapState = rc.getMapState(new MapStateDescriptor<>("mapState", Long.class, InfAlignData.class));
        minTime = rc.getState(new ValueStateDescriptor<>("minTimeState", Long.class));
        lastData = rc.getState(new ValueStateDescriptor<>("lastDataState", InfAlignData.class));

    }

    @Override
    public void processElement(List<RawData> values, Context ctx, Collector<InfAlignData> out) throws Exception {
        // 初始时间归一化
        Long curSysTime = System.currentTimeMillis() / deltaTime * deltaTime;
        Long curRdTime_5s = 0L;
        for (RawData value : values) {
            Long time = value.getTime();
            curRdTime_5s = (time / deltaTime) * deltaTime;

            if (minTime.value() == null) {
                logger.debug("对齐首次接入数据: " + value.getTime());
                minTime.update(curSysTime - latenTime);
//            ctx.timerService().registerProcessingTimeTimer(curSysTime + deltaTime);
            }
            if (curRdTime_5s < minTime.value()) {
                logger.debug("对齐数据延迟过高: " + value.getTime());
                return;
            }
            InfAlignData data;
            if (!mapState.contains(curRdTime_5s)) {
                data = new InfAlignData();
                data.setTags(new Tags());
                data.setFields(new HashMap<>());
                mapState.put(curRdTime_5s, c2align(curRdTime_5s, data, value));
            } else {
                data = mapState.get(curRdTime_5s);
                if (data.getTime() <= time) {
                    mapState.put(curRdTime_5s, c2align(curRdTime_5s, data, value));
                }
            }
        }
        ctx.timerService().registerProcessingTimeTimer(curSysTime + deltaTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InfAlignData> out) throws Exception {
        ctx.timerService().registerProcessingTimeTimer(timestamp + deltaTime);
        this.outClolectData(out);

        //若是延迟时间超过30s，再次推进5s
        if (timestamp - minTime.value() > latenTime) {
            this.outClolectData(out);
        }
    }

    /**
     * 发送数据
     *
     * @param out
     * @throws Exception
     */
    private void outClolectData(Collector<InfAlignData> out) throws Exception {
        if (mapState.contains(minTime.value())) {
            lastData.update(mapState.get((minTime.value())));
        }
        if (lastData.value() != null) {
            lastData.value().setTime(minTime.value());
            out.collect(lastData.value());
            logger.debug("对齐数据： " + minTime.value() + " " +
                    lastData.value().getTags().getDevice_id() + " " +
                    lastData.value().getName() + " " +
                    lastData.value().getTags().getTenant_id() + " " +
                    lastData.value().getFields().toString());
        }
        mapState.remove(minTime.value());
        minTime.update(minTime.value() + deltaTime);
    }

    private InfAlignData c2align(Long time, InfAlignData alignData, RawData rawData) {
        if (alignData.getTags().getDevice_id() == null &&
                !rawData.getDevice_id().equals(alignData.getTags().getDevice_id())) {
            alignData.setName("io_" + rawData.getTemplate_id() + "_property");
            alignData.setTime(time);
            alignData.getTags().setDevice_id(rawData.getDevice_id());
            alignData.getTags().setDirection(rawData.getDirection());
            alignData.getTags().setTenant_id(rawData.getTenant_id());
            alignData.getTags().setEquipment_id(rawData.getEquipment_id());
            alignData.getTags().setGateway_id(rawData.getGateway_id());
        }
        alignData.getFields().put("storage_time", System.currentTimeMillis());
        alignData.getFields().put(rawData.getProp_id(), rawData.getValue());
        return alignData;
    }
}
