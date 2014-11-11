package main.java.alarm.bolt;

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

public class AverageCalcBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	public static final int MAX_SIZE = 10;
	private List<Integer> measurements = new ArrayList<Integer>();
	public OutputCollector _collector;
	private static final Logger log = LoggerFactory.getLogger(AverageCalcBolt.class);
    private boolean latency;

    public AverageCalcBolt(boolean latency) {
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
        Event average = new Event(calcAverage(), value, timestamp);
        String parsedLine = this.parseLine(value);
        //Curl to influxdb
        curlToInfluxDB(parseLine);
        //_collector.emit(new Values(average));
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
	
	private String[] parseLine(String line) {
        try {
            String[] splittedLine = line.substring(line.indexOf(" ")+1).split(" ");
            return arrayToString(splittedLine);
        }catch (Exception e) {
            log.error("Line not compatible " + e.getMessage());
        }
	}

    private String arrayToString(String[] splittedLine) {
        String result = "[";
        for (String i: splittedLine) {
            result +='"' + i + '",';
        }
        result = result.substring(result.length()-1);
        result += "]"
    }

    private void curlToInfluxDB(String line) {
        HttpURLConnection httpcon = (HttpURLConnection) ((new URL("10.1.0.13").openConnection()));
        httpcon.setDoOutput(true);
        httpcon.setRequestProperty("Content-Type", "application/json");
        httpcon.setRequestProperty("Accept", "application/json");
        httpcon.setRequestMethod("POST");
        httpcon.connect();

        byte[] outputBytes = "[{'name' : 'files', 'columns' : ['user', 'file', 'type'], 'points' : [" + line + "]]}]".getBytes("UTF-8");
        OutputStream os = httpcon.getOutputStream();
        os.write(outputBytes);

        os.close();
    }
}
