package com.iflytek.edcc;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * created with idea
 * user:ztwu
 * date:2019/3/25
 * description
 */
public class MyRegistrator implements KryoRegistrator {

    public void registerClasses(Kryo kryo) {
        kryo.register(Event2.class);
    }
}
