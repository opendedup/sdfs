package org.opendedup.collections;

import java.io.IOException;
import java.io.SyncFailedException;

import org.opendedup.collections.SimpleByteArrayLongMap.KeyValuePair;

public interface SimpleMapInterface {

	int getVersion();

	String getPath();

	void iterInit();

	int getCurrentSize();

	KeyValuePair next() throws IOException, MapClosedException;

	int getMaxSz();

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 */
	boolean containsKey(byte[] key) throws MapClosedException;

	void vanish();

	boolean put(byte[] key, long value) throws MapClosedException;

	long get(byte[] key) throws MapClosedException;

	void close();

	void sync() throws SyncFailedException, IOException;

}