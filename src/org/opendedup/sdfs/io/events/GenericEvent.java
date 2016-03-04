package org.opendedup.sdfs.io.events;

import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonObject;

public class GenericEvent {
	private long sequence;
	private static final AtomicLong sq = new AtomicLong(0);
	
	public GenericEvent() {
		sequence = sq.incrementAndGet();
	}
	
	public JsonObject toJSONObject() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("squence", sequence);
		return dataset;
	}
	

}
