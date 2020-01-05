package config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Description: 配置文件
 * <p>
 * Author: GWL
 * Date: Create in 11:57 2019/12/25
 */
public class Config {
    public static Logger logger = LoggerFactory.getLogger(Config.class);
    static Properties prop = new Properties();
    InputStreamReader propertiesReader = null;

    public static String sinkInfluxdbURL ;
    public static String sinkInfluxdbUsername ;
    public static String sinkInfluxdbPassword ;
    public static String sinkInfluxdbDatabase ;

    public static String sourceKafkaURL ;
    public static String sourceKafkaDatapointTopic ;
    public static String sourceKafkaAutoCommitIntervalMs ;
    public static String sourceKafkaAutoOffsetReset ;
    public static String sourceKafkaEnableAutoCommit ;
    public static String sourceKafkaGroupID ;

    static {
        try {
            init("src/main/resources/config.properties");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Config() throws Exception {
        init("src/main/resources/config.properties");
    }

    public Config(String path) throws Exception {
        init(path);
    }

    static void init(String path) throws Exception {
        File propertiesFile = new File(path);
        logger.debug("配置文件地址：" + propertiesFile.getAbsolutePath());
        InputStream in = new BufferedInputStream(new FileInputStream(propertiesFile));

        // 通过 Properties 类的 load 方法来读取 properties 文件中的变量
        prop.load(in);
        // 开始读取 properties 文件中数据
        sinkInfluxdbURL = prop.getProperty("sink.influxdb.url");
        sinkInfluxdbUsername = prop.getProperty("sink.influxdb.username");
        sinkInfluxdbPassword = prop.getProperty("sink.influxdb.password");
        sinkInfluxdbDatabase = prop.getProperty("sink.influxdb.database");

        sourceKafkaURL = prop.getProperty("source.kafka.url");
        sourceKafkaDatapointTopic = prop.getProperty("source.kafka.datapoint.topic");
        sourceKafkaAutoCommitIntervalMs = prop.getProperty("source.kafka.auto.commit.interval.ms");
        sourceKafkaAutoOffsetReset = prop.getProperty("source.kafka.auto.offset.reset");
        sourceKafkaEnableAutoCommit = prop.getProperty("source.kafka.enable.auto.commit");
        sourceKafkaGroupID = prop.getProperty("source.kafka.group.id");

    }
}
