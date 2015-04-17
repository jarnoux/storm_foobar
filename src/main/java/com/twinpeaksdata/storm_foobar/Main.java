package com.twinpeaksdata.storm_foobar;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 *
 * @author jacques
 */
public class Main {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .directGrouping("words");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
        builder.setBolt("exclaim3", new ExclamationBolt(), 3)
                .directGrouping("exclaim1");
        builder.setBolt("exclaim4", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim2").shuffleGrouping("exclaim3");

        StormTopology topology = builder.createTopology();
        
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology);
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
        
    }

}
