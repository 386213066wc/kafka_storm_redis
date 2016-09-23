package com.socialmaster.job;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import com.socialmaster.blot.AppendCityBolt;
import com.socialmaster.blot.CountBolt;
import com.socialmaster.blot.ParseBolt;
/**
 * Created by liuxiaojun on 2016/8/22.
 */
public class MainTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String topicName = "topic-log";
        String zkConnString = "ZK01:2181,ZK02:2181,ZK03:2181/kafka";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout", kafkaSpout, 3);
        builder.setBolt("parse", new ParseBolt(), 3).localOrShuffleGrouping("kafka_spout");
        builder.setBolt("append", new AppendCityBolt(), 3).localOrShuffleGrouping("parse");
        builder.setBolt("count", new CountBolt(), 6).fieldsGrouping("conversion", new Fields("minute","cityCode"));
        Config conf = new Config();
        conf.setMaxSpoutPending(5000);

        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topicName, conf, builder.createTopology());
    }
}