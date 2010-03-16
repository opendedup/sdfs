package org.opendedup.sdfs.filestore;

import java.io.IOException;

public interface AbstractChunkStoreListener {
	public abstract int getID();

	public abstract void chunkMovedEvent(ChunkEvent e) throws IOException;

	public abstract void chunkRemovedEvent(ChunkEvent e) throws IOException;
}
