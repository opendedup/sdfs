package org.opendedup.sdfs.filestore;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.opendedup.util.StringUtils;

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
public class HashChunk implements Externalizable {
	/**
	 * 
	 */
	// The name of the hash chunk. This is the md5 or sha hash
	private byte[] name;
	// the data
	private byte[] data;
	// whether or not the data is compressed
	private boolean compressed;

	public HashChunk() {

	}

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
	public HashChunk(byte[] name, byte[] data, boolean compressed) {
		this.name = name;
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

	@Override
	public String toString() {
		return StringUtils.getHexString(name);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.compressed = in.readBoolean();
		short hl = in.readShort();
		this.name = new byte[hl];
		in.readFully(name);
		this.data = new byte[in.readInt()];
		in.readFully(data);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(compressed);
		out.writeShort((short) this.name.length);
		out.write(this.name);
		out.writeInt(data.length);
		out.write(data);
	}

}
