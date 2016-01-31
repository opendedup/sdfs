package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class MFileRenamed {
	
	public MetaDataDedupFile mf;
	public String from;
	public String to;
	public MFileRenamed(MetaDataDedupFile f,String from,String to) {
		this.mf = f;
		this.from = from;
		this.to = to;
	}
	
	public String toJSON() {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("actionType", "mfileRename");
		dataset.addProperty("path", mf.getPath());
		dataset.addProperty("from", this.from);
		dataset.addProperty("to", this.to);
		if(mf.isSymlink())
			dataset.addProperty("fileType", "symlink");
		else if(mf.isDirectory())
			dataset.addProperty("fileType", "dir");
		else
			dataset.addProperty("fileType", "file");
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		return gson.toJson(dataset);
	}

}
