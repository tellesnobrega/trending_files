package main.java.alarm.bolt;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import main.java.alarm.Event;
import main.java.utils.LocalUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AddToDB implements IRichBolt {
	private static final long serialVersionUID = 1L;
	public static final int MAX_SIZE = 10;
	public OutputCollector _collector;
	private static final Logger log = LoggerFactory.getLogger(AddToDB.class);
    private boolean latency;

    public AddToDB(boolean latency) {
        this.latency = latency;
    }

    @Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Integer key = input.getIntegerByField("key");
		Event event = (Event) input.getValue(input.fieldIndex("event"));
        String value = event.getValue();
        long startTime = event.getTimestamp();
        Long timestamp = new GregorianCalendar().getTimeInMillis();
        String parsedLine = this.parseLine(value);
        //Curl to influxdb
        curlToInfluxDB(parsedLine);
        if(latency) {
            long latency = timestamp - startTime;
            log.info("AckSent;" + latency);
        } else {
            log.info("AckSent");
        }
    }

	@Override
	public void cleanup() { }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
	}	

	@Override
	public Map<String, Object> getComponentConfiguration() { return null; }
	
	private String parseLine(String line) {
        try {
            String[] splittedLine = line.substring(line.indexOf(" ")+1).split(" ");
            return arrayToString(splittedLine);
        }catch (Exception e) {
            log.error("Line not compatible " + e.getMessage());
        }
        return "";
	}

    private String arrayToString(String[] splittedLine) {
        String result = "[";
        for (String i: splittedLine) {
            result +='"' + i + '"' + ",";
        }
        result = result.substring(0,result.length()-1);
        result += "]";
        return result;
    }

    private void curlToInfluxDB(String line) {
        String[] command = {"curl", "-X", "POST", "-d", "[{\"name\" : \"files\", \"columns\" : [\"userid\", \"action\", \"fileid\"], \"points\" : [" + line + "] } ]", "http://10.1.0.13:8086/db/trendfiles/series?u=root&p=root"};
        log.info(arrayToString(command));
        try {
            Process pb = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
