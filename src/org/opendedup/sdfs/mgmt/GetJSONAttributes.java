package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GetJSONAttributes {

	public static String getResult(String file) throws IOException  {
		File f = new File(file);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if (f.isDirectory()) {
			try {
				File[] files = f.listFiles();
				JsonObject fo = new JsonObject();
				JsonArray datasets = new JsonArray();
				for (int i = 0; i < files.length; i++) {
					MetaDataDedupFile mf = MetaFileStore.getMF(files[i]
							.getPath());
					datasets.add(mf.toJSON(false));
				}
				fo.add("files", datasets);
				Gson gson = new GsonBuilder()
						.setPrettyPrinting()
						.serializeNulls()
						.setFieldNamingPolicy(
								FieldNamingPolicy.UPPER_CAMEL_CASE).create();
				return gson.toJson(fo);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		} else {

			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(file);
				JsonObject fo = new JsonObject();
				JsonArray datasets = new JsonArray();
				datasets.add(mf.toJSON(false));
				fo.add("files", datasets);
				Gson gson = new GsonBuilder()
						.setPrettyPrinting()
						.serializeNulls()
						.setFieldNamingPolicy(
								FieldNamingPolicy.UPPER_CAMEL_CASE).create();
				return gson.toJson(fo);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		}
	}

}
