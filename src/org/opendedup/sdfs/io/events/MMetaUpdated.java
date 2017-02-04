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
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class MMetaUpdated extends GenericEvent {
	private static final int pl = Main.volume.getPath().length();
	public MetaDataDedupFile mf;
	public String name,value;

	public MMetaUpdated(MetaDataDedupFile f,String name,String value) {
		super();
		this.mf = f;
		this.name = name;
		this.value = value;
	}

	public String toJSON() {
		JsonObject dataset = this.toJSONObject();
		dataset.addProperty("actionType", "mmetaUpdated");
		dataset.addProperty("name", name);
		if(this.value != null)
			dataset.addProperty("value", value);
		dataset.addProperty("object", mf.getPath().substring(pl));
			Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls()
				.setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
				.create();
		return gson.toJson(dataset);
	}
}
