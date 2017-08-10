/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.network;

import java.io.IOException;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.servers.HCServer;

@SuppressWarnings("rawtypes")
public class HashClientPool extends GenericObjectPool {

	@SuppressWarnings("unchecked")
	public HashClientPool(HCServer server, String name, int size, byte id)
			throws IOException {
		super(new HashClientPoolFactory(server, id));
		this.setMaxIdle(size); // Maximum idle threads.
		this.setMaxActive(size); // Maximum active threads.
		this.setMinEvictableIdleTimeMillis(30000); // Evictor runs every 30
													// secs.
		this.setTestOnBorrow(true); // Check if the thread is still valid.
		this.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
		SDFSLogger.getLog().info(
				"Server id=" + id + " name=" + name + " hn="
						+ server.getHostName() + " port=" + server.getPort()
						+ " size=" + size);
	}

}
