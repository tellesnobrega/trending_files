package main.java.alarm.bolt;

import java.util.Map;

import main.java.alarm.Event;
import main.java.alarm.spout.ConsumptionSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class AlarmBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	public static final String NAME = "ALARM";
	public OutputCollector _collector;
//	private Map<Integer, Integer> averages = new HashMap<Integer, Integer>();
	private static final Logger log = LoggerFactory.getLogger(AlarmBolt.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple input) {
		Event event = (Event) input.getValue(input.fieldIndex("event"));
//		if (sendAlarm(event.getValue(), event.getAverage())) {
//			log.info("Alarm alert! Measurement: " + event.getValue());
//		}
	}

	@Override
	public void cleanup() { }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
	}	

	@Override
	public Map<String, Object> getComponentConfiguration() { return null; }

	private boolean sendAlarm(double measurement, double average) {
		return measurement > average + average*0.1;
	}

}
