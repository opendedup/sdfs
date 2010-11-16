package org.opendedup.hashing;

import java.io.IOException;


import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class HashFunctionPool {

	private int poolSize;
	private ConcurrentLinkedQueue<AbstractHashEngine> passiveObjects = new ConcurrentLinkedQueue<AbstractHashEngine>();
	
	public HashFunctionPool(int size) {
		this.poolSize = size;
		this.populatePool();
	}
	
	public void populatePool() {
		for (int i = 0; i < poolSize; i++) {
			try {
				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				e.printStackTrace();
				SDFSLogger.getLog().fatal(
						"unable to instancial Hash Function pool", e);

			} finally {
				
			}
		}
	}
	
	public AbstractHashEngine borrowObject() throws IOException {
		AbstractHashEngine hc = null;
		hc = this.passiveObjects.poll();
		if (hc == null) {
			try {
				hc = makeObject();
			} catch (NoSuchAlgorithmException e) {
				throw new IOException(e);
			} catch (NoSuchProviderException e) {
				throw new IOException(e);
			}
		}
		return hc;
	}
	
	public void returnObject(AbstractHashEngine hc) throws IOException {
		this.passiveObjects.add(hc);
	}
	
	public AbstractHashEngine makeObject() throws NoSuchAlgorithmException,
			NoSuchProviderException {
		AbstractHashEngine hc = null;
		if(Main.hashLength == 16) {
			hc = new Tiger16HashEngine();
		}else {
			hc = new TigerHashEngine();
		}
		return hc;
	}
	
	public void destroyObject(AbstractHashEngine hc) {
		hc.destroy();
	}

}
