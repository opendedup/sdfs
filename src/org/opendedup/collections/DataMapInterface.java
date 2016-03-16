package org.opendedup.collections;

import java.io.IOException;

import org.opendedup.sdfs.io.FileClosedException;

public interface DataMapInterface {

	public abstract void iterInit() throws IOException;

	public abstract long getIterPos();

	public abstract long nextKey() throws IOException;

	public abstract byte[] nextValue() throws IOException;

	public abstract boolean isClosed();

	public abstract void put(long pos, byte[] data) throws IOException,
			FileClosedException;

	public abstract void put(long pos, byte[] data, int length)
			throws IOException, FileClosedException;

	public abstract void putIfNull(long pos, byte[] data) throws IOException,
			FileClosedException;

	public abstract void trim(long pos, int len) throws IOException,
			FileClosedException;

	public abstract void truncate(long length) throws IOException,
			FileClosedException;

	public abstract byte getVersion();

	public abstract byte[] getFree();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	public abstract void remove(long pos) throws IOException,
			FileClosedException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#get(long)
	 */
	public abstract byte[] get(long pos) throws IOException,
			FileClosedException;

	public abstract void sync() throws IOException;

	public abstract void vanish() throws IOException;

	public abstract void copy(String destFilePath) throws IOException;

	public abstract long size();

	public abstract void close();

}