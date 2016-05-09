package org.opendedup.sdfs.io.events;

import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.sdfs.Main;

import com.google.gson.JsonObject;

public class GenericEvent {
	private long sequence;
	private static final AtomicLong sq = new AtomicLong(0);
	private static final long MAX = Long.MAX_VALUE - (100000);

	public GenericEvent() {
		sequence = sq.incrementAndGet();
		if (sequence >= MAX) {
			synchronized (sq) {
				if (sequence >= MAX) {
					sq.set(0);
				}
			}
		}

	}

	public JsonObject toJSONObject() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("sequence", sequence);
		dataset.addProperty("volumeid", Long.toString(Main.DSEID));
		dataset.addProperty("timestamp",
				Long.toString(System.currentTimeMillis()));
		return dataset;
	}

}
