package com.xiaoe.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author DoubleHu
 * @Description: 配置文件加载类
 * 默认位置  src\main\resources
 * 默认配置文件名 conf.properties
 * @date 2019/08/14 16:54
 */
public class ConfigUtils {
    private final static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);
    public static Config config =null;
    static {
        if(config==null){
            config=init();
        }
    }
    private static Config init() {
        Config config = ConfigFactory.parseResources("conf.properties");
//        Config config = ConfigFactory.parseResources("conf-dev.properties");
//        Config config = ConfigFactory.parseResources("conf.properties");
        if (config.isEmpty()) {
            logger.error("conf.properties not find!");
            throw new RuntimeException("conf.properties not find!");
        }
        return config;
    }
    public static Config getConfig(){
        if(config==null){
            config=init();
        }
        return config;
    }
}
