package com.xiaoe.utils;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
/**
 * @author shelwin
 * @version $Id: null.java 2018-10-30 20:09 $
 */
public class EntryRegisterKryo implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo arg0) {
        arg0.register(ConsumerRecord.class);
    }
}