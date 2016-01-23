package org.opendedup.sdfs.network;

import java.io.IOException;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.servers.HCServer;

public class HashClientPool extends GenericObjectPool{


	public HashClientPool(HCServer server, String name, int size, byte id)
			throws IOException {
		super(new HashClientPoolFactory(server,id));
		this.setMaxIdle(size); // Maximum idle threads.
	    this.setMaxActive(size); // Maximum active threads.
	    this.setMinEvictableIdleTimeMillis(30000); //Evictor runs every 30 secs.
	    this.setTestOnBorrow(true); // Check if the thread is still valid.
	    this.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
		SDFSLogger.getLog().info("Server id=" + id + " name=" + name + " hn=" + server.getHostName() + " port=" +server.getPort() + " size=" + size);
	}

	

}
