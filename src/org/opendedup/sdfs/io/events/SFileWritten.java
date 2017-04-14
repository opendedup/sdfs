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

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SFileWritten  extends GenericEvent{
	public SparseDedupFile sf = null;
	long location = -1;

	public SFileWritten(SparseDedupFile f) {
		this.sf = f;
	}
	
	public SFileWritten(SparseDedupFile f,long location) {
		this.sf = f;
		this.location = location;
	}
	
	public long getLocation() {
		return this.location;
	}

	public String toJSON() {
		String fl = sf.mf.getPath().substring(Main.volume.getPath().length());
		while(fl.startsWith("/") || fl.startsWith("\\"))
			fl =fl.substring(1, fl.length());
		JsonObject dataset = this.toJSONObject();
		dataset.addProperty("actionType", "sfileWritten");
		dataset.addProperty("volumeid", Long.toString(Main.DSEID));
		dataset.addProperty("timestamp", Long.toString(System.currentTimeMillis()));
		dataset.addProperty("location", this.location);
		dataset.addProperty("file", fl);
		dataset.addProperty("object", sf.getGUID());
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls()
				.setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
				.create();
		return gson.toJson(dataset);
	}

}
