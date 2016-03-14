package org.opendedup.sdfs.io.events;


import java.io.File;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SFileDeleted {
	public SparseDedupFile sf = null;
	public String sfp = null;
	public SFileDeleted(SparseDedupFile  sf) {
		sfp = Main.dedupDBStore + File.separator
				+ this.sf.getGUID().substring(0, 2) + File.separator + this.sf.getGUID();
	}
	
	public String toJSON() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("actionType", "sfileDeleted");
		dataset.addProperty("GUID", this.sf.getGUID());
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		return gson.toJson(dataset);
	}

}
