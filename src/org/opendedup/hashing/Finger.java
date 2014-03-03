package org.opendedup.hashing;

import org.opendedup.sdfs.io.AsyncChunkWriteActionListener;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class Finger implements Runnable {
	public byte[] chunk;
	public byte[] hash;
	public byte[] hl;
	public int start;
	public int len;
	public int ap;
	public boolean dedup;
	public AsyncChunkWriteActionListener l;

	@Override
	public void run() {
		try {
			this.hl = HCServiceProxy.writeChunk(hash, chunk, chunk.length,
					chunk.length, dedup);
			l.commandResponse(this);
		} catch (Throwable e) {
			l.commandException(this, e);
		}
	}
}
