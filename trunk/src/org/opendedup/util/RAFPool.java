package org.opendedup.util;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opendedup.util.SDFSLogger;

public class RAFPool {

	private int poolSize = 1;
	
	private ConcurrentLinkedQueue<FileChannel> passiveObjects = new ConcurrentLinkedQueue<FileChannel>();
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
	
	public FileChannel borrowObject() throws IOException {
		if(this.closed)
			throw new IOException("RAF Pool closed for " + this.fileName);
		FileChannel hc = null;
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
	
	public void returnObject(FileChannel raf) throws IOException {
		if(this.closed)
			raf.close();
		else 
			this.passiveObjects.add(raf);
	}
	
	public FileChannel makeObject() throws FileNotFoundException {
		RandomAccessFile hc = null;
		hc = new RandomAccessFile(fileName,"rw");
		return hc.getChannel();
	}
	
	public void destroyObject(FileChannel raf) {
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
