package org.opendedup.sdfs.mgmt;

import java.io.File;



import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GetJSONAttributes {
	
	private transient static LoadingCache<String, Iterator<Path>> listFiles = CacheBuilder.newBuilder().maximumSize(1024).expireAfterAccess(5, TimeUnit.MINUTES).build(
			new CacheLoader<String, Iterator<Path>>() {
	             public Iterator<Path> load(String key) { // no checked exception
	               return null;
	             }
	           });

	public static String getResult(String file,String uuid,int num) throws IOException  {
		File f = new File(file);
		if(num == -1 || num > 100) {
			num = 100;
		}
		
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if (f.isDirectory()) {
			try {
				if(uuid == null) {
					Path path = Paths.get(file);
			        DirectoryStream<Path> dirStream = Files.newDirectoryStream(path);
			        uuid = UUID.randomUUID().toString();
			        listFiles.put(uuid, dirStream.iterator());
				}
				Iterator<Path> lst = listFiles.getIfPresent(uuid);
				if(lst == null)
					throw new IOException("uuid does not exist");
				int ct = 0;
				JsonObject fo = new JsonObject();
				JsonArray datasets = new JsonArray();
				while(lst.hasNext()) {
					MetaDataDedupFile mf = MetaFileStore.getMF(lst.next().toFile()
							.getPath());
					datasets.add(mf.toJSON(false));
					ct++;
					if(ct >= num) {
						break;
					}
				}
				fo.add("files", datasets);
				fo.addProperty("truncated", lst.hasNext());
				fo.addProperty("nextmarker", uuid);
				if(!lst.hasNext())
					listFiles.invalidate(uuid);
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
