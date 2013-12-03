package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;

public class RAFPool {

	private File f;
	private int poolSize;
	private LinkedBlockingQueue<RandomAccessFile> passiveObjects = null;
	private ArrayList<RandomAccessFile> activeObjects = new ArrayList<RandomAccessFile>();
	private ReentrantLock alock = new ReentrantLock();

	public RAFPool(File f, int size) throws IOException {
		this.f = f;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<RandomAccessFile>(
				this.poolSize);
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

	public RandomAccessFile borrowObject() throws IOException {
		RandomAccessFile hc = null;
		try {
			hc = this.passiveObjects.poll();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
		}
		if (hc == null) {
			hc = this.makeObject();
		}
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

	public void returnObject(RandomAccessFile hc) throws IOException {
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

	public RandomAccessFile makeObject() throws IOException {
		RandomAccessFile hc = new RandomAccessFile(this.f, "rw");
		return hc;
	}

	public void destroyObject(RandomAccessFile hc) throws IOException {
		hc.close();
	}

	public void close() throws IOException, InterruptedException {
		if (this.activeObjects.size() > 0)
			throw new IOException("Cannot close because writes still occuring");
		RandomAccessFile hc = passiveObjects.poll();
		while (hc != null) {
			hc.close();
			hc = passiveObjects.poll();
		}
	}

}
