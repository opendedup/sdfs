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

import org.apache.commons.pool.PoolableObjectFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServer;

@SuppressWarnings("rawtypes")
public class HashClientPoolFactory implements PoolableObjectFactory {
	private HCServer server;
	private byte id;

	public HashClientPoolFactory(HCServer server, byte id) {
		this.server = server;
		this.id = id;
	}

	@Override
	public void activateObject(Object arg0) throws Exception {
		HashClient hc = (HashClient) arg0;
		if (hc.isClosed()) {
			hc.openConnection();
		}

	}

	@Override
	public void destroyObject(Object arg0) throws Exception {
		HashClient hc = (HashClient) arg0;
		hc.close();

	}

	@Override
	public Object makeObject() throws Exception {
		HashClient hc = new HashClient(this.server, "server", Main.DSEPassword,
				this.id);
		hc.openConnection();
		return hc;
	}

	@Override
	public void passivateObject(Object arg0) throws Exception {

	}

	@Override
	public boolean validateObject(Object arg0) {
		HashClient hc = (HashClient) arg0;
		return !hc.isClosed();
	}

}
