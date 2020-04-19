package com.xiaoe.utils;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author DoubleHu
 * @Description:
 * @date 2018/11/16 14:05
 */
public class RedisUtils implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(RedisUtils.class);
    //共享连接池
    private static JedisPool pool;//jedis连接池
    //私有化构造方法
    private RedisUtils(){}

    private static JedisPool init() {
        Config config = ConfigUtils.getConfig();
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(20);
        jedisPoolConfig.setMaxWaitMillis(1000 * 10);
        jedisPoolConfig.setTestOnBorrow(false);
        jedisPoolConfig.setTestOnReturn(true);

        // 初始化JedisPool
        String redisHost = config.getString("redis.host");
        int port = config.getInt("redis.port");
        int timeout = config.getInt("redis.timeout");
        String redisPasswd = config.getString("redis.password");
        return new JedisPool(jedisPoolConfig, redisHost, port, timeout, redisPasswd);
    }

    /**
     * 从连接池中获取连接
     *
     * @return
     */
    public static synchronized Jedis getJedis() {
        if (pool == null) {
            pool = init();
        }
        return pool.getResource();
    }




    /**
     * 模糊匹配
     *
     * @param pattern key的正则表达式
     * @param count   每次扫描多少条 记录，值越大消耗的时间越短，但会影响redis性能。建议设为一千到一万
     * @return 匹配的key对应的value集合
     */

    public static List<String> patternGet(String pattern, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.select(14);
            List<String> tagsList = new ArrayList<>();
            List<String> keys = null;
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams();
            scanParams.count(count);
            scanParams.match(pattern);
            Pipeline pipeline = jedis.pipelined();
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                keys = scanResult.getResult();
                if (keys != null) {
                    for (String key : keys) {
                        pipeline.get(key);
                    }
                    List<Object> oTagsStr = pipeline.syncAndReturnAll();
                    for (Object o : oTagsStr) {
                        tagsList.add((String) o);
                    }
                }
                cursor = scanResult.getStringCursor();
            } while (!"0".equals(cursor));
            return tagsList;
        } catch (Exception e) {
            logger.error("redis patternGet err! ,pattern={},exception={}", pattern, e);
            throw new RuntimeException("get tag fail !");
        } finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }


}
