package org.opendedup.sdfs.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServer;

public class HashClientPool {

	private HCServer server;
	private int poolSize;
	private LinkedBlockingQueue<HashClient> passiveObjects = null;
	private ArrayList<HashClient> activeObjects = new ArrayList<HashClient>();
	private ReentrantLock alock = new ReentrantLock();

	public HashClientPool(HCServer server, String name, int size)
			throws IOException {
		this.server = server;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<HashClient>(this.poolSize);
		this.populatePool();
	}

	public void populatePool() throws IOException {
		for (int i = 0; i < poolSize; i++) {
			try {

				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to get object out of pool ",
						e);
				throw new IOException(e.toString());

			} finally {
			}
		}
	}

	public void activateObject(HashClient hc) throws IOException {
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
			hc = this.passiveObjects.take();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
		}
		if (hc == null) {
			hc = this.makeObject();
		}
		if (hc.isClosed())
			hc.openConnection();
		this.alock.lock();
		try {
			this.activeObjects.add(hc);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
			alock.unlock();
		}
		return hc;
	}

	public void returnObject(HashClient hc) throws IOException {
		alock.lock();
		try {

			this.activeObjects.remove(hc);
		} catch (Exception e) {
			e.printStackTrace();
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
			alock.unlock();
		}
		try {
			if (this.passiveObjects.size() < this.poolSize)
				this.passiveObjects.put(hc);
			else
				hc.close();
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
		}
	}

	public HashClient makeObject() throws IOException {
		HashClient hc = new HashClient(this.server, "server",Main.upStreamPassword);
		hc.openConnection();
		return hc;
	}

	public void destroyObject(HashClient hc) {
		hc.close();
	}

}
