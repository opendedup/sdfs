package org.opendedup.sdfs.filestore;

import java.io.Serializable;

/**
 * 
 * @author Sam Silverberg A HashChunk is used by the chunk store as a container
 *         object for the actual chunk of data and meta-data for the dedup chunk
 *         of data.
 * 
 * @see H2HashStore
 * @see TCHashStore
 * 
 */
public class HashChunk implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -719101848617839457L;
	// The name of the hash chunk. This is the md5 or sha hash
	private byte[] name;
	// the start position to read or write from the byte array. This always 0
	private long start;
	// the length of the the data with the byte array
	private int len;
	// the data
	private byte[] data;
	// whether or not the data is compressed
	private boolean compressed;

	/**
	 * Instantiates the HashChunk
	 * 
	 * @param name
	 *            The name of the hash chunk. This is the md5 or sha hash
	 * @param start
	 *            the start position to read or write from the byte array. This
	 *            always 0
	 * @param len
	 *            the length of the the data with the byte array
	 * @param data
	 *            the data
	 * @param compressed
	 *            whether or not the data is compressed
	 */
	public HashChunk(byte[] name, long start, int len, byte[] data,
			boolean compressed) {
		this.name = name;
		this.start = start;
		this.len = len;
		this.data = data;
		this.compressed = compressed;
	}

	/**
	 * 
	 * @returns true if the data is compressed
	 */
	public boolean isCompressed() {
		return this.compressed;
	}

	/**
	 * 
	 * @return the dedup data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * 
	 * @param data
	 *            the dedup data
	 */
	public void setData(byte[] data) {
		this.data = data;
	}

	/**
	 * 
	 * @return the md5 or sha hash for the data
	 */
	public byte[] getName() {
		return name;
	}

	/**
	 * 
	 * @param name
	 *            the md5 or sha hash for the data
	 */
	public void setName(byte[] name) {
		this.name = name;
	}

	/**
	 * 
	 * @return the start position within the array.
	 */
	public long getStart() {
		return start;
	}

	/**
	 * 
	 * @param start
	 *            sets the start position within the array
	 */
	public void setStart(long start) {
		this.start = start;
	}

	/**
	 * 
	 * @return the lenth of the data within the array
	 */
	public int getLen() {
		return len;
	}

	/**
	 * 
	 * @param len
	 *            sets the length of data within the array.
	 */
	public void setLen(int len) {
		this.len = len;
	}

	public String toString() {
		return name + " start=" + this.start + " len=" + this.len;
	}

}
