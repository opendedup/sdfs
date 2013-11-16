package org.opendedup.collections;

import java.io.IOException;

public interface AbstractMap {

	public abstract boolean isClosed();

	public abstract void sync() throws IOException;

	public abstract void vanish() throws IOException;

	public abstract void vanish(boolean propigateEvent) throws IOException;

	public abstract void close();

}