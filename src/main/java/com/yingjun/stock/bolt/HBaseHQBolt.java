package com.yingjun.stock.bolt;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.yingjun.stock.utils.BloomFilter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HBaseHQBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Connection connection;
    private BloomFilter bloomFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        Configuration cfg = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        bloomFilter = new BloomFilter();
    }

    @Override
    public void execute(Tuple input) {
        String row = input.getString(0);
        String col1 = input.getString(1);
        String col2 = input.getString(2);
        String col3 = input.getString(3);
        String col4 = input.getString(4);
        String col5 = input.getString(5);
        String col6 = input.getString(6);
        String col7 = input.getString(7);
        String col8 = input.getString(8);
        String col9 = input.getString(9);
        String col10 = input.getString(10);
        String col11 = input.getString(11);
        String col12 = input.getString(12);
        String col13 = input.getString(13);
        String col14 = input.getString(14);
        String col15 = input.getString(15);
        String col16 = input.getString(16);
        String col17 = input.getString(17);
        String col18 = input.getString(18);

        String key = "hbase-" + row;

        if (!bloomFilter.contains(key)) {
            bloomFilter.add(key);
            try {
                Admin admin = connection.getAdmin();
                TableName tableName = TableName.valueOf("hangqing");
                if (admin.tableExists(tableName)) {
                    Put put = new Put(Bytes.toBytes(row));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col1"), Bytes.toBytes(col1));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col2"), Bytes.toBytes(col2));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col3"), Bytes.toBytes(col3));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col4"), Bytes.toBytes(col4));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col5"), Bytes.toBytes(col5));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col6"), Bytes.toBytes(col6));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col7"), Bytes.toBytes(col7));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col8"), Bytes.toBytes(col8));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col9"), Bytes.toBytes(col9));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col10"), Bytes.toBytes(col10));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col11"), Bytes.toBytes(col11));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col12"), Bytes.toBytes(col12));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col13"), Bytes.toBytes(col13));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col14"), Bytes.toBytes(col14));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col15"), Bytes.toBytes(col15));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col16"), Bytes.toBytes(col16));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col17"), Bytes.toBytes(col17));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col18"), Bytes.toBytes(col18));
                    Get get = new Get(Bytes.toBytes(row));
                    get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("created"));
                    Result result = connection.getTable(tableName).get(get);
                    if (result.isEmpty()) {
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("created"), Bytes.toBytes(new Date().getTime()));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("modified"), Bytes.toBytes(new Date().getTime()));
                    } else {
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("created"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("created")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("modified"), Bytes.toBytes(new Date().getTime()));
                    }
                    admin.getConnection().getTable(tableName).put(put);
                }else{
                	 HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                	 tableDescriptor.setDurability(Durability. SYNC_WAL );
                     //add a column family " info "
                     HColumnDescriptor hcd =  new  HColumnDescriptor( "info" );
                     tableDescriptor.addFamily(hcd);
                     tableDescriptor.addFamily(new HColumnDescriptor("col1"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col2"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col3"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col4"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col5"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col6"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col7"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col8"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col9"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col10"));
                     tableDescriptor.addFamily(new HColumnDescriptor("col11"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col12"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col13"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col14"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col15"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col16"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col17"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("col18"));     
                     tableDescriptor.addFamily(new HColumnDescriptor("created"));  
                     tableDescriptor.addFamily(new HColumnDescriptor("modified")); 
                     admin.createTable(tableDescriptor);  
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}