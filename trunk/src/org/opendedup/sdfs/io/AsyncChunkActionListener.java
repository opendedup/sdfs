package org.opendedup.sdfs.io;

import org.opendedup.sdfs.io.WritableCacheBuffer.Shard;

public abstract class AsyncChunkActionListener {
	public abstract void commandException(Exception e);

	public abstract void commandResponse(Shard result);

}
