package com.socialmaster.blot;

import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.socialmaster.tool.DateUtil;
import com.socialmaster.tool.RedisUtil;

/**
 * Created by liuxiaojun on 2016/8/22.
 */
public class AppendCityBolt extends BaseBasicBolt {
    private TreeMap<Long, Map<String, Set<String>>> timeCounts = new TreeMap<Long, Map<String, Set<String>>>();

    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long timeStamp = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        String currentMinute = sdf.format(new Date(timeStamp));

        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
            Iterator<Map.Entry<Long, Map<String, Set<String>>>> iter = timeCounts.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Map<String, Set<String>>> entry = iter.next();
                long minute = entry.getKey();
                if (DateUtil.getMinuteDiff(String.valueOf(minute),currentMinute) > 5) {
                    for (Map.Entry<String, Set<String>> counts : entry.getValue().entrySet()) {
                        String key = "ap_city:"+counts.getKey();
                        Integer count = counts.getValue().size();
                        RedisUtil.getInstance().toRedisOnline(key, count.toString(), 1200);
                    }
                    iter.remove();
                } else {
                    break;
                }
            }
            return;
        }else {
            long minute = tuple.getLongByField("minute");
            String cityCode = tuple.getStringByField("cityCode");
            String apMac = tuple.getStringByField("apMac");

            if (DateUtil.getMinuteDiff(String.valueOf(minute),currentMinute) > 10) {
                System.out.println("drop outdated tuple " + tuple);
                return;
            }

            Map<String, Set<String>> counts = timeCounts.get(minute);
            if (counts == null) {
                counts = new HashMap<String, Set<String>>();
                timeCounts.put(minute, counts);
            }
            if (counts.get(cityCode) == null){
                counts.put(cityCode,new HashSet<String>());
            }
            if (timeCounts.get(minute).get(cityCode).contains(apMac) == false){
                timeCounts.get(minute).get(cityCode).add(apMac);
                collector.emit(new Values(cityCode, apMac));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cityCode", "apMac"));
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }
}