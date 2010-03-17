package org.opendedup.util;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.opendedup.sdfs.servers.HCServer;

public class HashFunctionPool {

	private int poolSize;
	private ArrayList<MessageDigest> passiveObjects = new ArrayList<MessageDigest>();
	private ArrayList<MessageDigest> activeObjects = new ArrayList<MessageDigest>();
	private ReentrantLock plock = new ReentrantLock();
	private ReentrantLock alock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");
	static {
		Security.addProvider(new BouncyCastleProvider());
	}

	public HashFunctionPool(int size) {
		this.poolSize = size;
		this.populatePool();
	}

	public void populatePool() {
		for (int i = 0; i < poolSize; i++) {
			try {
				plock.lock();
				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				plock.unlock();
				e.printStackTrace();
				log.log(Level.SEVERE,
						"unable to instancial Hash Function pool", e);

			} finally {
				if (plock.isLocked())
					plock.unlock();
			}
		}
	}

	public void activateObject(MessageDigest hc) {

	}

	public boolean validateObject(MessageDigest hc) {
		return false;
	}

	public MessageDigest borrowObject() throws IOException {
		MessageDigest hc = null;
		try {
			plock.lock();
			if (this.passiveObjects.size() > 0) {
				hc = this.passiveObjects.remove(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			plock.unlock();
		}
		if (hc == null) {
			try {
				hc = makeObject();
			} catch (NoSuchAlgorithmException e) {
				throw new IOException(e);
			} catch (NoSuchProviderException e) {
				throw new IOException(e);
			}
		}
		try {
			this.alock.lock();
			this.activeObjects.add(hc);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Unable to get object out of pool "
					+ e.toString());

		} finally {
			alock.unlock();
		}
		return hc;
	}

	public void returnObject(MessageDigest hc) throws IOException {
		try {
			hc.reset();
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

	public MessageDigest makeObject() throws NoSuchAlgorithmException,
			NoSuchProviderException {
		MessageDigest hc = MessageDigest.getInstance("Tiger", "BC");
		return hc;
	}

	public void destroyObject(MessageDigest hc) {
		hc.reset();
		hc = null;
	}

}
