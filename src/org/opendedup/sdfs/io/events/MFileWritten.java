package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class MFileWritten extends GenericEvent{
	private static final int pl = Main.volume.getPath().length();
	public MetaDataDedupFile mf;
	public MFileWritten(MetaDataDedupFile f) {
		super();
		this.mf = f;
	}
	
	public String toJSON() {
		JsonObject dataset = this.toJSONObject();
		dataset.addProperty("actionType", "mfileWritten");
		dataset.addProperty("path",  mf.getPath().substring(pl));
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
