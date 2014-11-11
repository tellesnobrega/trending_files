package main.java.alarm;

import java.io.*;
import java.util.Date;

public class Event implements Serializable {
	private static final long serialVersionUID = 1L;
	private int average;
	private String value;

    private Long timestamp;

    public Event(int average, String value, Long timestamp) {
		this.average = average;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Event(String value, Long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getAverage() {
		return average;
	}

    public Long getTimestamp() {
        return timestamp;
    }

	public String getValue() {
		return value;
	}

	public byte[] getBytes() {
		byte[] yourBytes = {};
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(this);
			yourBytes = bos.toByteArray();
			out.close();
			bos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return yourBytes;
	}
}
