package main.java.alarm.spout;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;

import main.java.alarm.Event;
import main.java.utils.LocalUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ConsumptionSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private static final long ONE_SEC = 1*1000;
	private Random rand;
	public SpoutOutputCollector _collector;
    private static final Logger log = LoggerFactory.getLogger(ConsumptionSpout.class);
    private long sleepTime;
    boolean latency;
	

    public ConsumptionSpout(int measuresPerSec, boolean latency) {
		this.sleepTime = ONE_SEC/measuresPerSec;
		rand = new Random();
        this.latency = latency;
	}
    
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void close() { }

	@Override
	public void activate() { }

	@Override
	public void deactivate() { }

	@Override
	public void nextTuple() {
		Utils.sleep(this.sleepTime);
		int key = rand.nextInt(10);
        int userId = rand.nextInt(50);
        int fileId = rand.nextInt(100);
        String[] action = {"created", "updated", "viewed"}
        String value = "user " + userId + " " + action[rand.nextInt(action.length)] + " " + fileId;
        Long timestamp = new GregorianCalendar().getTimeInMillis();
        Event event = new Event(value, timestamp);
        String id = key+";"+value;
		_collector.emit(new Values(key, event));
		if(!latency) {
    		log.info("EventSent");
        }
	}

	@Override
	public void ack(Object msgId) {
//		log.info("ACK: " + msgId.toString());
	}

	@Override
	public void fail(Object msgId) {
//        log.info("FAIL: " + msgId.toString());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","event"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() { return null; }
}
