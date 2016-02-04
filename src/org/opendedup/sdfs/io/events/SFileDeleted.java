package org.opendedup.sdfs.io.events;


import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SFileDeleted {
	public String sf = null;
	
	public SFileDeleted(String  f) {
		this.sf = f;
	}
	
	public String toJSON() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("actionType", "sfileDeleted");
		dataset.addProperty("GUID", sf);
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		return gson.toJson(dataset);
	}

}
