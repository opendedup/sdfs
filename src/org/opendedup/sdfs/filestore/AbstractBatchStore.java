package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.opendedup.collections.DataArchivedException;

public interface AbstractBatchStore {
	public boolean fileExists(long id) throws IOException;
	public boolean checkAccess();
	public void writeHashBlobArchive(HashBlobArchive arc,long id) throws IOException;
	public byte [] getBytes(long id) throws IOException, DataArchivedException;
	public Map<String,Integer> getHashMap(long id) throws IOException;
	public boolean checkAccess(String username,String password,Properties props)throws Exception;
	public Iterator<String> getNextObjectList()throws IOException;
	public StringResult getStringResult(String key) throws IOException, InterruptedException;
	public boolean isLocalData();
}
