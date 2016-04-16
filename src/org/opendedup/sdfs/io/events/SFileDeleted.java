package org.opendedup.sdfs.io.events;


import java.io.File;

import org.opendedup.sdfs.Main;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SFileDeleted {
	public String GUID = null;
	public String sf = null;
	public SFileDeleted(String  GUID) {
		this.GUID = GUID;
		sf = Main.dedupDBStore + File.separator
				+ this.GUID.substring(0, 2) + File.separator + this.GUID+ File.separator
				+ this.GUID + ".map";
	}
	
	public String toJSON() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("actionType", "sfileDeleted");
		dataset.addProperty("GUID", GUID);
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		return gson.toJson(dataset);
	}

}
