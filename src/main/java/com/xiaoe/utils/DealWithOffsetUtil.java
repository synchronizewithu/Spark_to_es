package com.xiaoe.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author DoubleHu
 * @Description: kafka偏移量处理类
 * @date 2019/11/08 16:54
 */
public class DealWithOffsetUtil {

    private static Logger logger = LoggerFactory.getLogger(DealWithOffsetUtil.class);

    /**
     * 保存偏移量
     * @param dbIndex 偏移量保存的redis db索引
     * @param prefix  key前缀一般使用项目名
     * @param groupId 消费者组
     * @param kafkaStream spark数据流
     */

    public static void saveOffsetsByStream(int dbIndex,String prefix,String groupId, JavaInputDStream<ConsumerRecord<String, String>> kafkaStream) {
        kafkaStream.foreachRDD(record -> {
            //维护offsets
            if (!record.isEmpty()) {
                logger.warn("维护offsets.....");
                OffsetRange[] offsetRanges = ((HasOffsetRanges) record.rdd()).offsetRanges();
                saveOffsets(dbIndex,prefix,groupId, offsetRanges);
            }
        });
    }


    private static void saveOffsets(int dbIndex,String prefix,String groupId, OffsetRange[] offsetRanges) {

        String preKey = "";
        Jedis jedis= RedisUtils.getJedis();
        jedis.select(dbIndex);

        for (OffsetRange offsetRange : offsetRanges) {

            //获取当前批次的偏移量
            String topic = offsetRange.topic();
            int partition = offsetRange.partition();
            long untilOffset = offsetRange.untilOffset();

            logger.warn("prefix:"+prefix+" groupId:" + groupId + " topic:" + topic + " partition:" + partition + " untilOffset:" + untilOffset);
            preKey = prefix+":" + groupId + ":" + topic + ":" + partition;

            jedis.set(preKey, String.valueOf(untilOffset));
            jedis.expire(preKey, 60 * 60 * 24 * 2);

        }

        jedis.close();
    }

    /**
     * 从redis查询offset
     *
     * @param dbIndex
     * @param prefix
     * @param groupId
     * @param topic
     * @return
     * @throws InterruptedException
     */
    public static Map<TopicPartition, Long> getOffsetsByGroupIdAndTopic(int dbIndex,String prefix,String groupId, String topic) throws InterruptedException {

        Map<TopicPartition, Long> returnOffset = new HashMap<TopicPartition, Long>();
        Jedis jedis= RedisUtils.getJedis();
        jedis.select(dbIndex);
        String keyPattern = prefix+":" + groupId + ":" + topic + ":*";
        Set<String> keys = jedis.keys(keyPattern);

        for (String key : keys) {
            //在key中找到parition
            String[] split = key.split(":");
            int partition = Integer.valueOf(split[split.length - 1]);
            String s = jedis.get(key);
            if (null != s && !"".equals(s)) {
                returnOffset.put(new TopicPartition(topic, partition), Long.valueOf(s));
            } else {
                returnOffset.put(new TopicPartition(topic, partition), Long.valueOf("0"));
            }
            logger.warn("the offset by redis  key:{},value:{}", key, s);
        }

        return returnOffset;
    }

}
