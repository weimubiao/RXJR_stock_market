package com.yingjun.stock.bolt;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;

import com.yingjun.stock.dto.StockTestEvent;
import com.yingjun.stock.utils.BloomFilter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Administrator on 2016/3/11.
 */
public class MessageSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
    	StockTestEvent info = (StockTestEvent) input.getValue(0);
    	
        /*ObjectMapper objectMapper = new ObjectMapper();
        String msg = input.getString(0);*/
        try {
            //Map<String, Object> info = objectMapper.readValue(msg, Map.class);
            String area = info.getArea()== null ? "default" : info.getArea().toString();
            double speed = 0.00d;
            try{
                speed = (double) info.getSpeed();
            }catch(Exception e) {
                e.printStackTrace();
            }
            String uuid = info.getUuid() == null ? UUID.randomUUID().toString() : info.getUuid().toString();
            String key = "splitter-" + uuid;
            if (!bloomFilter.contains(key)) {
                bloomFilter.add(key);
                outputCollector.emit(input, new Values(area, speed, uuid));
            }
            outputCollector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "speed", "uuid"));
    }
}