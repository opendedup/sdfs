package org.opendedup.sdfs.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.servers.HCServer;

public class HashClientPool {

	private int port;
	private HCServer server;
	private int poolSize;
	private ArrayList<HashClient> passiveObjects = new ArrayList<HashClient>();
	private ArrayList<HashClient> activeObjects = new ArrayList<HashClient>();
	private ReentrantLock plock = new ReentrantLock();
	private ReentrantLock alock = new ReentrantLock();

	public HashClientPool(HCServer server, String name, int size)
			throws IOException {
		this.server = server;
		this.poolSize = size;
		this.populatePool();
	}

	public void populatePool() throws IOException {
		for (int i = 0; i < poolSize; i++) {
			try {
				plock.lock();
				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				plock.unlock();
				e.printStackTrace();
				throw new IOException("Unable to get object out of pool "
						+ e.toString());

			} finally {
				if (plock.isLocked())
					plock.unlock();
			}
		}
	}

	public void activateObject(HashClient hc) {
		if (hc.isClosed()) {
			hc.openConnection();
		}
	}

	public boolean validateObject(HashClient hc) {
		return false;
	}

	public HashClient borrowObject() throws IOException {
		HashClient hc = null;
		try {
			plock.lock();
			if (this.passiveObjects.size() > 0) {

				hc = this.passiveObjects.remove(0);
			}
		} catch (Exception e) {
			plock.unlock();
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			plock.unlock();
		}
		if (hc == null) {
			hc = makeObject();
		}
		if (hc.isClosed())
			hc.openConnection();
		try {
			this.alock.lock();
			this.activeObjects.add(hc);
		} catch (Exception e) {
			alock.unlock();
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			alock.unlock();
		}
		return hc;
	}

	public void returnObject(HashClient hc) throws IOException {
		try {
			alock.lock();
			this.activeObjects.remove(hc);
		} catch (Exception e) {
			alock.unlock();
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			alock.unlock();
		}
		try {
			plock.lock();
			this.passiveObjects.add(hc);
		} catch (Exception e) {
			plock.unlock();
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			plock.unlock();
		}
	}

	public HashClient makeObject() {
		HashClient hc = new HashClient(this.server, "server");
		hc.openConnection();
		return hc;
	}

	public void destroyObject(HashClient hc) {
		hc.close();
	}

}
