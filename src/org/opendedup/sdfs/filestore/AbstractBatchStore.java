package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.opendedup.collections.DataArchivedException;


public interface AbstractBatchStore {
	public boolean fileExists(long id) throws IOException;

	public boolean checkAccess();
	
	public int getCheckInterval();

	public void writeHashBlobArchive(HashBlobArchive arc, long id)
			throws IOException;

	public void checkoutObject(long id, int claims) throws IOException;

	public void getBytes(long id,File f) throws IOException, DataArchivedException;
	
	public byte [] getBytes(long id,int from,int to) throws IOException, DataArchivedException;

	public Map<String, Long> getHashMap(long id) throws IOException;

	public boolean checkAccess(String username, String password,
			Properties props) throws Exception;
	

	public boolean objectClaimed(String key) throws IOException;

	public Iterator<String> getNextObjectList(String prefix) throws IOException;

	public StringResult getStringResult(String key) throws IOException,
			InterruptedException;

	public boolean isLocalData();
	
	public boolean isClustered();
	
	public int getMetaDataVersion();
}
