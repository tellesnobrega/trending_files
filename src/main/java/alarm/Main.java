package main.java.alarm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import main.java.alarm.bolt.AddToDB;
import main.java.alarm.spout.ConsumptionSpout;

public class Main {
	private static final long TEN_MIN = 10*60*1000;

    public static void main(String[] args) throws Exception {
		
		int messagesPerSecond = Integer.parseInt(args[0]);
        boolean latency = Boolean.parseBoolean(args[1]);


		int spouts;
        int bolts;
        int tasks;
		try{
			spouts = Integer.parseInt(args[2]);
            bolts = Integer.parseInt(args[3]);
            tasks = bolts * 3;
		}catch(Exception e) {
			spouts = 1;
            bolts = 3;
            tasks = 12;
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("source", new ConsumptionSpout(messagesPerSecond, latency), spouts);
		builder.setBolt("average", new AddToDB(latency), bolts).setNumTasks(tasks).fieldsGrouping("source", new Fields("key"));

		Config conf = new Config();
		conf.put(Config.TOPOLOGY_DEBUG, false);
		conf.setNumWorkers(7);

		StormSubmitter.submitTopology("trending-files", conf, builder.createTopology());
		
	}
	
	private static void sleep() {
		System.out.println("Going to sleep...");
		Utils.sleep(TEN_MIN);
		System.out.println("Finished!!!");
	}
}
