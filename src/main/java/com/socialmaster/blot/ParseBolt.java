package com.socialmaster.blot;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2016/8/22.
 */
public class ParseBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // 20160803000925,h,58696c71e704
        String line = tuple.getString(0);
        String[] splits = line.split(",");
        if (splits.length >= 3) {
            Long minute = Long.valueOf(splits[0].substring(0, 12));
            String apmac = splits[2];
            collector.emit(new Values(minute, apmac));
        }else{
            System.err.println("can not parse for log " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "apMac"));
    }
}