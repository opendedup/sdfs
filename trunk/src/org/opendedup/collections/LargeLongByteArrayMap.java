package org.opendedup.collections;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class LargeLongByteArrayMap implements AbstractMap {

	private long fileSize = 0;
	private int arraySize = 0;
	// RandomAccessFile bdbf = null;
	String fileName;
	boolean closed = true;
	private ReentrantLock hashlock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");

	public LargeLongByteArrayMap(String fileName, long initialSize,
			int arraySize) throws IOException {
		if (initialSize > 0)
			this.fileSize = initialSize;
		this.arraySize = arraySize;
		this.fileName = fileName;
		this.openFile();
		this.closed = false;
	}

	private void openFile() throws IOException {
		File f = new File(this.fileName);
		if (!f.exists())
			f.getParentFile().mkdirs();
		RandomAccessFile bdbf = new RandomAccessFile(fileName, "rw");
		if (this.fileSize > 0)
			bdbf.setLength(this.fileSize);
		bdbf.close();
		bdbf = null;
	}

	public void setLength(long length) throws IOException {
		RandomAccessFile bdbf = new RandomAccessFile(fileName, "rw");
		bdbf.setLength(length);
		bdbf.close();
		bdbf = null;
	}

	public void lockCollection() {
		this.hashlock.lock();
	}

	public void unlockCollection() {
		this.hashlock.unlock();
	}

	@Override
	public void close() {
		this.hashlock.lock();
		this.hashlock.unlock();
		this.closed = true;
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(fileName, "rw");
		} catch (Exception e) {
		}
		try {
			bdbf.getFD().sync();
		} catch (Exception e) {
		}
		try {
			bdbf.close();
		} catch (IOException e) {
		}
		System.gc();
	}

	@Override
	public byte[] get(long pos) throws IOException {
		return this.get(pos, true);
	}

	public byte[] get(long pos, boolean checkForLock) throws IOException {
		if (checkForLock) {
			this.hashlock.lock();
			this.hashlock.unlock();
		}
		byte[] b = new byte[this.arraySize];
		RandomAccessFile rf = new RandomAccessFile(this.fileName, "rw");
		rf.seek(pos);
		rf.read(b);
		rf.close();
		return b;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public void put(long pos, byte[] data) throws IOException {
		this.hashlock.lock();
		this.hashlock.unlock();
		if (data.length != this.arraySize)
			throw new IOException(" size mismatch " + data.length
					+ " does not equal " + this.arraySize);
		RandomAccessFile rf = new RandomAccessFile(this.fileName, "rw");
		rf.seek(pos);
		rf.write(data);
		rf.close();
		rf = null;
	}

	public void remove(long pos) throws IOException {
		this.remove(pos, true);
	}

	public void remove(long pos, boolean checkForLock) throws IOException {
		if (checkForLock) {
			this.hashlock.lock();
			this.hashlock.unlock();
		}
		RandomAccessFile rf = new RandomAccessFile(this.fileName, "rw");
		rf.seek(pos);
		rf.write(new byte[this.arraySize]);
		rf.close();
		rf = null;
	}

	@Override
	public void sync() throws IOException {
		RandomAccessFile bdbf = new RandomAccessFile(fileName, "rw");
		bdbf.getFD().sync();
		bdbf.close();
		bdbf = null;

	}

	@Override
	public void vanish() throws IOException {
		RandomAccessFile bdbf = new RandomAccessFile(fileName, "rw");
		bdbf.setLength(0);
		bdbf.close();
		bdbf = null;
	}

	public void copy(String destFilePath) throws IOException {
		this.hashlock.lock();
		try {
			this.sync();
			File f = new File(destFilePath);
			if (f.exists())
				f.delete();
			else
				f.getParentFile().mkdirs();
			Path p = Paths.get(this.fileName);
			Path dest = Paths.get(destFilePath);
			p.copyTo(dest);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	public void move(String destFilePath) throws IOException {
		this.hashlock.lock();
		try {
			this.sync();
			File f = new File(destFilePath);
			if (f.exists())
				f.delete();
			else
				f.getParentFile().mkdirs();
			Path p = Paths.get(this.fileName);
			Path dest = Paths.get(destFilePath);
			p.moveTo(dest);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	public void optimize() throws IOException {
		this.hashlock.lock();
		try {
			log.info("optimizing file [" + this.fileName + "]");
			File f = new File(this.fileName + ".new");
			f.delete();
			RandomAccessFile rf = new RandomAccessFile(this.fileName, "rw");
			RandomAccessFile nrf = new RandomAccessFile(this.fileName + ".new",
					"rw");

			nrf.setLength(rf.length());
			byte[] FREE = new byte[arraySize];
			Arrays.fill(FREE, (byte) 0);
			long mData = 0;
			for (long pos = 0; pos < rf.length(); pos = pos + this.arraySize) {
				try {
					byte[] b = new byte[this.arraySize];
					rf.seek(pos);
					rf.read(b);
					if (!Arrays.equals(FREE, b)) {
						nrf.seek(pos);
						nrf.write(b);
						mData = mData + b.length;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			rf.close();
			rf = null;
			nrf.close();
			nrf = null;
			File orig = new File(this.fileName);
			orig.delete();
			File newF = new File(this.fileName + ".new");
			newF.renameTo(orig);
			log.info("optimizing file [" + this.fileName + "] migrated ["
					+ mData + "] bytes of data to new file");
		} catch (IOException e) {
			throw e;
		} finally {
			this.hashlock.unlock();
		}
	}

}
