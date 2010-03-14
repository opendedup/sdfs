package org.opendedup.collections;

import java.io.IOException;

public interface AbstractMap {

	public abstract boolean isClosed();

	public abstract void put(long pos, byte[] data) throws IOException;

	public abstract void remove(long pos) throws IOException;

	public abstract byte[] get(long pos) throws IOException;

	public abstract void sync() throws IOException;

	public abstract void vanish() throws IOException;

	public abstract void close();

}