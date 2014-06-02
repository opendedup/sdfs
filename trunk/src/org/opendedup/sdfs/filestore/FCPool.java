package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

//import org.apache.lucene.store.NativePosixUtil;
import org.opendedup.logging.SDFSLogger;

public class FCPool {

	private File f;
	private int poolSize;
	private LinkedBlockingQueue<FileChannel> passiveObjects = null;
	private ArrayList<FileChannel> activeObjects = new ArrayList<FileChannel>();
	private ReentrantLock alock = new ReentrantLock();

	public FCPool(File f, int size) throws IOException {
		this.f = f;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<FileChannel>(this.poolSize);
		this.populatePool();
	}

	public void populatePool() throws IOException {
		for (int i = 0; i < poolSize; i++) {
			try {

				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				throw new IOException(e);

			} finally {
			}
		}
	}

	public FileChannel borrowObject() throws IOException {
		FileChannel hc = null;
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
			throw new IOException(e);

		} finally {
			alock.unlock();
		}
		return hc;
	}

	public void returnObject(FileChannel hc) throws IOException {
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
			boolean inserted = this.passiveObjects.offer(hc);
			if (!inserted)
				hc.close();
		} catch (Exception e) {
			throw new IOException(e);

		} finally {
		}
	}

	public FileChannel makeObject() throws IOException {
		@SuppressWarnings("resource")
		RandomAccessFile rf = new RandomAccessFile(this.f, "rw");
		// NativePosixUtil.advise(rf.getFD(), 0, 0, NativePosixUtil.DONTNEED);
		FileChannel hc = rf.getChannel();
		return hc;
	}

	public void destroyObject(FileChannel hc) throws IOException {
		hc.close();
	}

	@SuppressWarnings("resource")
	public void close() throws IOException, InterruptedException {
		if (this.activeObjects.size() > 0)
			throw new IOException("Cannot close because writes still occuring");
		FileChannel hc = passiveObjects.poll();
		while (hc != null) {
			hc.close();
			hc = passiveObjects.poll();
		}
		hc = new RandomAccessFile(this.f, "rw").getChannel();
		hc.force(true);
		hc.close();
	}

}
