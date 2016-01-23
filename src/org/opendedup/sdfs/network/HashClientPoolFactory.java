package org.opendedup.sdfs.network;

import org.apache.commons.pool.PoolableObjectFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServer;

public class HashClientPoolFactory implements PoolableObjectFactory {
	private HCServer server;
	private byte id;
	
	public HashClientPoolFactory(HCServer server,byte id) {
		this.server =server;
		this.id = id;
	}

	@Override
	public void activateObject(Object arg0) throws Exception {
		HashClient hc = (HashClient)arg0;
		if (hc.isClosed()) {
			hc.openConnection();
		}

	}

	@Override
	public void destroyObject(Object arg0) throws Exception {
		HashClient hc = (HashClient)arg0;
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
		HashClient hc = (HashClient)arg0;
		return !hc.isClosed();
	}

}
