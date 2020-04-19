package com.xiaoe;


import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.xiaoe.utils.ConfigUtils;
import com.xiaoe.utils.DealWithOffsetUtil;
import com.xiaoe.utils.ESUtils;
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
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BulkWriteES {
    private final static Logger logger = LoggerFactory.getLogger(BulkWriteES.class);

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

        //过滤出待发送到es的部分文档
        partOfDocStream.filter(x -> {
            JSONObject rowJson = null;
            try {
                rowJson = JSONObject.parseObject(x.value());
                if (rowJson.containsKey("id")) {
                    if (!rowJson.getString("id").isEmpty()) {
                        return true;
                    }
                }
            } catch (Exception e) {
                logger.warn("signalJson parse fail:{}", x.value());
                e.printStackTrace();

            }
            return false;
        }).map(row-> JSONObject.parseObject(row.value()))
                .foreachRDD(rdd->rdd.foreachPartition(rows->{
            HashMap<String,JSONObject> tmpRows=new HashMap<>();
            Config config1=ConfigUtils.getConfig();
            String index=config1.getString("es.index");
            String type=config1.getString("es.type");
            RestHighLevelClient client = ESUtils.getClient(config1);
            while (rows.hasNext()){
                JSONObject jsonObject=rows.next();
                tmpRows.put(jsonObject.getString("id"),jsonObject);
                if(tmpRows.size()>10000||!rows.hasNext()){
//                    ESUtils.batchUpsert(client,index,type,tmpRows);
                    tmpRows.clear();
                }
            }
            client.close();

        }));



        // 处理信号值
        partOfDocStream.filter(x -> {
            JSONObject signalJson = null;
            try {
                signalJson = JSONObject.parseObject(x.value());
            } catch (Exception e) {
                logger.warn("signalJson parse fail:{}", x.value());
                e.printStackTrace();
                return false;
            }
            if (signalJson.containsKey("data_source")
                    && signalJson.containsKey("is_last_batch")) {
                if (signalJson.getString("data_source").equals("tag_operate_queue")
                        && signalJson.getIntValue("is_last_batch") == 1) {
                    return true;
                }
            };
            return false;
        }).map(signal->{
            JSONObject signalJson = JSONObject.parseObject(signal.value());
            signalJson.put("tag_id", signalJson.getString("tag_id"));
            signalJson.put("batch_id", signalJson.getString("batch_id"));
            signalJson.put("operate_source", "tag_system_operate");//"tag_system_operate"
            signalJson.put("task_status", 0); //0-完成计算；1-失败
            return signalJson;
        }).foreachRDD(rdd -> rdd.foreachPartition(signals -> {
            Jedis jedis = RedisUtils.getJedis();
            jedis.select(14);
            Pipeline pipeline = jedis.pipelined();
            int count = 0;
            while (signals.hasNext()) {
                JSONObject signalsJson = signals.next();
                pipeline.lpush("tags:task:queue", signalsJson.toJSONString());
                if (count++ > 10000 || !signals.hasNext()) {
                    count = 0;
                    pipeline.sync();
                }
            }
            jedis.close();
        }));

        //保存offset
        DealWithOffsetUtil.saveOffsetsByStream(dbIndex,offsetPrefix,groupId, partOfDocStream);


        jsc.start();
        jsc.awaitTermination();
    }
}
