package com.yingjun.stock.topology;

import java.util.Arrays;

import org.apache.thrift.TException;

import com.yingjun.stock.bolt.HBaseHQBolt;
import com.yingjun.stock.bolt.StockHQSplitBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * @author yingjun
 */
public class StockTestTopology {

    public static void main(String[] args) throws TException {

        // Configure Kafka
        //String zks="HQ01:12181,HQ02:12181,HQ03:12181";
    	String zks="hadoop-master:12181,hadoop-node1:12181,hadoop-node2:12181";
        String topic = "stock";
        // default zookeeper root configuration for storm
        String zkRoot = "/jstorm";//"/kafkaStorm";
        String spoutId = "kafkaSpout";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        //spoutConfig.scheme = new SchemeAsMultiScheme(new EventScheme());
        //spoutConfig.scheme = new SchemeAsMultiScheme(new TestScheme());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        //该Topology因故障停止处理，下次正常运行时是否从Spout对应数据源Kafka中的该订阅Topic的起始位置开始读取
        spoutConfig.forceFromStart = false;
        spoutConfig.zkServers = Arrays.asList(new String[] {"hadoop-master","hadoop-node1","hadoop-node2"});
        spoutConfig.zkPort = 12181;

        //创建topology的生成器
        TopologyBuilder builder = new TopologyBuilder();

        // Kafka里创建了一个2分区的Topic，这里并行度设置为2
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig), 2)
                .setNumTasks(2);
        // 设置数据处理节点名称，实例，并行度。
         /**
         * 存入hbase
         */
        builder.setBolt("splitter", new StockHQSplitBolt(), 2).shuffleGrouping("kafka-reader").setNumTasks(2);
        //builder.setBolt("processor", new ProcessingBolt(), 2).fieldsGrouping("splitter", new Fields("area")).setNumTasks(2); // 相同区域交由同一个bolt处理
        builder.setBolt("writer",new HBaseHQBolt(),1).fieldsGrouping("splitter",new Fields("row")).setNumTasks(1); // 相同key交由同一个bolt处理
        
        
        
        Config config = new Config();
        //设置一个spout task上面最多可以多少个没有处理的tuple，以防止tuple队列爆掉
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);
        config.setDebug(false);
        //分配几个进程来运行这个这个topology，建议大于物理机器数量。
        config.setNumWorkers(5);
        String name = StockTestTopology.class.getSimpleName();

        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            config.put(Config.NIMBUS_HOST, args[0]);
            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        } else {
        	 System.setProperty("hadoop.home.dir", "F:/developIde/hadoop-common-2.2.0-bin-master");
        	 config.setMaxTaskParallelism(3);
            // 这里是本地模式下运行的启动代码。
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
        }

    }
}
