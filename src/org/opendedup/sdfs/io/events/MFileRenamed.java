package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class MFileRenamed extends GenericEvent {

	public MetaDataDedupFile mf;
	public String from;
	public String to;
	private static final int pl = Main.volume.getPath().length();

	public MFileRenamed(MetaDataDedupFile f, String from, String to) {
		super();
		this.mf = f;
		this.from = from;
		this.to = to;
	}

	public String toJSON() {
		JsonObject dataset = this.toJSONObject();
		dataset.addProperty("actionType", "mfileRename");
		dataset.addProperty("path", mf.getPath().substring(pl));
		dataset.addProperty("from", this.from);
		dataset.addProperty("to", this.to);
		if (mf.isSymlink())
			dataset.addProperty("fileType", "symlink");
		else if (mf.isDirectory())
			dataset.addProperty("fileType", "dir");
		else
			dataset.addProperty("fileType", "file");
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls()
				.setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
				.create();
		return gson.toJson(dataset);
	}
}
