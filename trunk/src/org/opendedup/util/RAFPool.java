package org.opendedup.util;

import java.io.FileNotFoundException;



import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.LinkedTransferQueue;

import org.opendedup.util.SDFSLogger;

public class RAFPool {

	private int poolSize = 1;
	
	private LinkedTransferQueue<RandomAccessFile> passiveObjects = new LinkedTransferQueue<RandomAccessFile>();
	private String fileName = null;
	private boolean closed=false;
	
	public RAFPool(String fileName) {
		this.fileName = fileName;
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
	
	public RandomAccessFile borrowObject() throws IOException {
		if(this.closed)
			throw new IOException("RAF Pool closed for " + this.fileName);
		RandomAccessFile hc = null;
		hc = this.passiveObjects.poll();
		if (hc == null) {
			try {
				hc = makeObject();
			} catch (FileNotFoundException e) {
				throw new IOException(e);
			} 
		}
		return hc;
	}
	
	public void returnObject(RandomAccessFile raf) throws IOException {
		if(this.closed)
			raf.close();
		else 
			this.passiveObjects.add(raf);
	}
	
	public RandomAccessFile makeObject() throws FileNotFoundException {
		RandomAccessFile hc = null;
		hc = new RandomAccessFile(fileName,"rw");
		return hc;
	}
	
	public void destroyObject(RandomAccessFile raf) {
		try {
			raf.close();
		} catch (Exception e) {
		}
		raf = null;
	}
	
	public void close() {
		this.closed = true;
		while(this.passiveObjects.peek() != null) {
			this.destroyObject(this.passiveObjects.poll());
		}
	}

}
