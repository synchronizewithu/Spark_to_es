package com.xiaoe;


import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.xiaoe.utils.ConfigUtils;
import com.xiaoe.utils.DealWithOffsetUtil;
import com.xiaoe.utils.RedisUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class InitUserData {
    private final static Logger logger = LoggerFactory.getLogger(InitUserData.class);

    public static void main(String[] args) throws InterruptedException {


        Config config = ConfigUtils.getConfig();

        String appName = config.getString("spark.appname");
        Long duration = config.getLong("spark.streaming.duration");
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.xiaoe.utils.EntryRegisterKryo");
        //反压参数
        sparkConf.set("spark.streaming.backpressure.enabled", config.getString("spark.streaming.backpressure.enabled"));
        sparkConf.set("spark.streaming.backpressure.initialRate", config.getString("spark.streaming.backpressure.initialRate"));
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", config.getString("spark.streaming.kafka.maxRatePerPartition"));
        //es连接配置
        String resource=config.getString("es.index") + "/" + config.getString("es.type");
        sparkConf.set("es.nodes", config.getString("es.hostnames"));
        sparkConf.set("es.port", config.getString("es.port"));
//        sparkConf.set("es.nodes.wan.only","true");
        sparkConf.set("es.net.http.auth.user", config.getString("es.username"));
        sparkConf.set("es.net.http.auth.pass", config.getString("es.password"));
        sparkConf.set("es.resource", resource);
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));
        jsc.checkpoint(config.getString("spark.checkpoint.dir"));

        String topicForOrder = config.getString("kafka.topic.common");
        String brokerOrder = config.getString("kafka.bootstrap.servers.common");
        String groupId = config.getString("kafka.group_id");
        String offsetReset = config.getString("kafka.offset.reset");
        Boolean autoCommit = config.getBoolean("kafka.auto.commit");

        String offsetPrefix= config.getString("kafka.offsetkey.prefix");
        int dbIndex=config.getInt("kafka.offset.dbindex");

        offsetReset = offsetReset.isEmpty() ? "latest" : offsetReset;

        //配置kafkaorder
        Map<String, Object> parmas = new HashMap<String, Object>();
        parmas.put("bootstrap.servers", brokerOrder);
        parmas.put("group.id", groupId);
        parmas.put("key.deserializer", StringDeserializer.class);
        parmas.put("value.deserializer", StringDeserializer.class);
        parmas.put("auto.offset.reset", offsetReset);
        parmas.put("enable.auto.commit", autoCommit);


        //查询上次offset
        Map<TopicPartition, Long> orderOffsets = DealWithOffsetUtil.getOffsetsByGroupIdAndTopic(dbIndex,offsetPrefix,groupId, topicForOrder);

        if (orderOffsets.isEmpty()) {
            parmas.put("auto.offset.reset", "latest");
        }


        JavaInputDStream<ConsumerRecord<String, String>> partOfDocStream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Arrays.asList(topicForOrder), parmas, orderOffsets)
                );

        //过滤出待发送到es的用户数据
        JavaDStream<String> jsonDStream = partOfDocStream.filter(x -> {
            JSONObject row = null;
            try {
                row = JSONObject.parseObject(x.value());
                if (row == null || row.getString("data_source").isEmpty()) {
                    logger.warn("cann't find data_source,row = {}",   row);
                    return false;
                }
                if (row.getString("app_id").isEmpty()) {
                    logger.warn("cann't find app_id,row = {}",   row);
                    return false;
                }
                if (row.getString( "user_id").isEmpty()) {
                    logger.warn("cann't find user_id,row = {}",   row);
                    return false;
                }
            } catch (Exception e) {
                logger.warn("signalJson parse fail:{}", x.value());
                e.printStackTrace();

            }
            return false;
        })
                .map(row -> {
                    JSONObject rowJson = JSONObject.parseObject(row.value());
                    JSONObject jsonObject = new JSONObject();
                    //获取用户信息
                    jsonObject.put("id",rowJson.getString("app_id")+"|"+rowJson.getString("user_id"));
                    jsonObject.put("app_id",rowJson.getString("app_id"));
                    jsonObject.put("user_id",rowJson.getString("user_id"));
                    jsonObject.put("nick_name",rowJson.getString("nick_name"));
                    jsonObject.put("real_name",rowJson.getString("real_name"));
                    jsonObject.put("user_created_at",rowJson.getLongValue("user_created_at"));
                    jsonObject.put("user_from",rowJson.getIntValue("user_from"));
                    jsonObject.put("user_identity",rowJson.getString("user_identity"));
                    jsonObject.put("phone",rowJson.getString("phone"));
                    jsonObject.put("collection_phone",rowJson.getString("collection_phone"));

                    return jsonObject.toJSONString();
                });

        Map<String, String> esConf = new HashMap<>();
        esConf.put("es.write.operation", "upsert");
        esConf.put("es.mapping.id", "id");
        //save TO elasticsearch
        try {
            JavaEsSparkStreaming.saveJsonToEs(jsonDStream, resource, esConf);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("errormessage:{}", e.getMessage());
            logger.error("数据同步程序异常,请重新启动!");
            jsc.close();
        }


        //保存offset
        DealWithOffsetUtil.saveOffsetsByStream(dbIndex,offsetPrefix,groupId, partOfDocStream);


        jsc.start();
        jsc.awaitTermination();
    }
}
