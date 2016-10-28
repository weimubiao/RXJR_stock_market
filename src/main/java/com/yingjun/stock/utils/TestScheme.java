package com.yingjun.stock.utils;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.yingjun.stock.dto.StockTestEvent;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author yingjun
 */
public class TestScheme implements Scheme {

    private static final Logger log = LoggerFactory.getLogger(TestScheme.class);

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes, "UTF-8");
            StockTestEvent stockTestEvent = JSONObject.parseObject(msg, StockTestEvent.class);
            Values values = new Values(stockTestEvent);
            return values;
        } catch (Exception e) {
            log.error("Exception:", e);
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
