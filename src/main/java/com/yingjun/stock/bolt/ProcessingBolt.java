package com.yingjun.stock.bolt;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.yingjun.stock.utils.BloomFilter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ProcessingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, AtomicLong> countMap;
    private Map<String, Double> speedMap;
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        countMap = new HashMap<>();
        speedMap = new HashMap<>();

        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
        String area = input.getString(0);
        double speed = input.getDouble(1);
        String uuid = input.getString(2);

        String key = "processor-" + uuid;

        if(!bloomFilter.contains(key)) {
            bloomFilter.add(key);
            Calendar calendar = Calendar.getInstance();
            long year = calendar.get(Calendar.YEAR);
            long month = calendar.get(Calendar.MONTH) + 1;
            long date = calendar.get(Calendar.DATE);
            long hour = calendar.get(Calendar.HOUR_OF_DAY);
            long minute = calendar.get(Calendar.MINUTE) / 10 + 1;

            String row = area + year + month + date + hour + minute;
            AtomicLong count = this.countMap.get(row);
            if (count == null) {
                count = new AtomicLong();
                this.countMap.put(row, count);
            }

            Double speedAvg = this.speedMap.get(row);
            if (speedAvg == null) {
                speedAvg = 0.00D;
                this.speedMap.put(row, speedAvg);
            }
            speedAvg = (count.get() * speedAvg + speed) / (count.get() + 1);
            count.addAndGet(1);
            outputCollector.emit(input, new Values(row, area, year, month, date, hour, minute, count.get(), speedAvg, uuid));
        }
        outputCollector.ack(input);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("row", "area", "year", "month", "date", "hour", "minute", "count", "speed", "uuid"));
    }
}