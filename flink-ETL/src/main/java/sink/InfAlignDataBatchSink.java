package sink;

import config.Config;
import entity.influxdb.InfAlignData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @Auther dier
 * @Date 10/16/2019 11:30
 * @Description
 */
public class InfAlignDataBatchSink extends RichSinkFunction<InfAlignData> {
    public static Logger logger = LoggerFactory.getLogger(InfAlignDataBatchSink.class);
    private static InfluxDB db;

    public InfAlignDataBatchSink(){}

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.db = InfluxDBFactory.connect(
                Config.sinkInfluxdbURL,
                Config.sinkInfluxdbUsername,
               Config.sinkInfluxdbPassword);

        db.setDatabase(Config.sinkInfluxdbDatabase);
       // db.enableBatch(5000,1,TimeUnit.SECONDS);
    }

    @Override
    public void invoke(InfAlignData data, Context context) throws Exception {
        Point.Builder builder = Point.measurement(data.getName());

        builder.tag("device_id",data.getTags().getDevice_id());
        builder.tag("direction",data.getTags().getDirection());
        builder.tag("equipment_id",data.getTags().getEquipment_id());
        builder.tag("gateway_id",data.getTags().getGateway_id());
        builder.tag("tenant_id",data.getTags().getTenant_id());

        builder.fields(data.getFields());

        // TODO 时间戳默认 ms
        builder.time(data.getTime(), TimeUnit.MILLISECONDS);
        db.write(builder.build());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (db != null) {
            db.close();
        }
    }
}
