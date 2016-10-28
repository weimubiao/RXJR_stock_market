package com.yingjun.stock.bolt;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
public class StockHQSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, AtomicLong> countMap;
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        countMap = new HashMap<>();
        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
    	//20161021	20161021090018000	20161021090018202	20161021085952066	20161021085952067	10	900	399001	102 	10784.328000	0.000000	0.000000	0.000000	0.000000	0	0.000000	0.000000	O 	
        String msg = input.getString(0);
        try {
        	String datas[]=msg.split("\\s+");
            String uuid = datas[1] == null ? UUID.randomUUID().toString() : datas[1].toString()+ datas[7].toString();
            String key = "splitter-" + uuid;
            if (!bloomFilter.contains(key)) {
                bloomFilter.add(key);
                Calendar calendar = Calendar.getInstance();
                long year = calendar.get(Calendar.YEAR);
                long month = calendar.get(Calendar.MONTH) + 1;
                long date = calendar.get(Calendar.DATE);
                long hour = calendar.get(Calendar.HOUR_OF_DAY);
                long minute = calendar.get(Calendar.MINUTE) / 10 + 1;

                String row =  datas[1]+datas[7] + year + month + date + hour + minute;
                AtomicLong count = this.countMap.get(row);
                if (count == null) {
                    count = new AtomicLong();
                    this.countMap.put(row, count);
                }
                outputCollector.emit(input, new Values(row,datas[0], datas[1], datas[2], datas[3], datas[4], datas[5], datas[6], datas[7], datas[8], datas[9], datas[10], datas[11], datas[12], datas[13], datas[14], datas[15], datas[16], datas[17]));
            }
            outputCollector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("row", "data1", "data2", "data3", "date4", "data5", "data6", "data7", "data8", "data9", "data10", "data11", "data12", "data13", "date14", "data15", "data16", "data17","data18"));
    }
}