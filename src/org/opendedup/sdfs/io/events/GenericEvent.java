/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
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
