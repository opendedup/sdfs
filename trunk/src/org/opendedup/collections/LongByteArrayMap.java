package org.opendedup.collections;

import java.io.File;


import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;

public class LongByteArrayMap implements AbstractMap {

	private static Logger log = Logger.getLogger("sdfs");
	// RandomAccessFile bdbf = null;
	MappedByteBuffer bdb = null;
	int arrayLength = 0;
	String filePath = null;
	private ReentrantLock hashlock = new ReentrantLock();
	private boolean closed = true;
	public byte[] FREE = new byte[16];
	public int iterPos = 0;
	public String fileParams = "rw";

	public LongByteArrayMap(int arrayLength, String filePath)
			throws IOException {
		this.arrayLength = arrayLength;
		this.filePath = filePath;
		FREE = new byte[arrayLength];
		Arrays.fill(FREE, (byte) 0);
		this.openFile();
		new SyncThread(this);
	}

	public LongByteArrayMap(int arrayLength, String filePath, String fileParams)
			throws IOException {
		this.fileParams = fileParams;
		this.arrayLength = arrayLength;
		this.filePath = filePath;
		FREE = new byte[arrayLength];
		Arrays.fill(FREE, (byte) 0);
		this.openFile();
		new SyncThread(this);
	}

	public void iterInit() {
		this.iterPos = 0;
	}

	public long nextKey() throws IOException {
		File f = new File(this.filePath);
		long pos = (long) iterPos * (long) Main.CHUNK_LENGTH;
		long fLen = ((f.length() * (long) Main.CHUNK_LENGTH) / arrayLength);
		if (iterPos == 0)
			log.info("fLen = " + fLen);
		while (pos <= fLen) {
			try {
				try {
					this.hashlock.lock();
					pos = (long) iterPos * (long) Main.CHUNK_LENGTH;
					iterPos++;
				} catch (Exception e1) {
				} finally {
					this.hashlock.unlock();
				}
				byte[] b = this.get(pos);
				if (b != null)
					return pos;

			} catch (Exception e) {

			} finally {

			}

		}
		if (pos == fLen)
			log.info("length end " + pos);

		return -1;
	}

	public byte[] nextValue() throws IOException {
		int pos = iterPos * this.arrayLength;
		byte[] val = null;
		File f = new File(this.filePath);
		while (pos < f.length()) {
			val = new byte[this.arrayLength];
			try {
				this.hashlock.lock();
				pos = iterPos * this.arrayLength;
				iterPos++;
				this.bdb.position(pos);
				this.bdb.get(val);
				if (!Arrays.equals(FREE, val))
					return val;
			} catch (Exception e) {
			} finally {
				this.hashlock.unlock();
			}
		}
		return null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#isClosed()
	 */
	public boolean isClosed() {
		return this.closed;
	}

	private void openFile() throws IOException {
		if (this.closed) {
			this.hashlock.lock();
			RandomAccessFile bdbf = null;
			try {
				File f = new File(filePath);
				boolean fileExists = f.exists();
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}
				bdbf = new RandomAccessFile(filePath, this.fileParams);
				log.finer("opening [" + this.filePath + "]");
				if (!fileExists)
					bdbf.setLength(32768);
				// initiall allocate 32k
				if (this.fileParams.equalsIgnoreCase("r")) {
					this.bdb = bdbf.getChannel().map(MapMode.READ_ONLY, 0,
							bdbf.length());
				} else {
					this.bdb = bdbf.getChannel().map(MapMode.READ_WRITE, 0,
							bdbf.length());
				}
				this.bdb.load();
				this.closed = false;
			} catch (IOException e) {
				throw e;
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					bdbf.close();
				} catch (Exception e) {
				}
				this.hashlock.unlock();
			}
		}
	}

	private int calcMapFilePos(long fpos) throws IOException {
		long pos = (fpos / Main.CHUNK_LENGTH) * FREE.length;
		if (pos > Integer.MAX_VALUE)
			throw new IOException(
					"Requested file position "
							+ fpos
							+ " is larger than the maximum length of a file for this file system "
							+ (Integer.MAX_VALUE * Main.CHUNK_LENGTH)
							/ FREE.length);
		return (int) pos;
	}

	private void setMapFileLength(int len) throws IOException {
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(filePath, this.fileParams);
			if (len > bdbf.length()) {
				bdbf.setLength(len);
				this.bdb = bdbf.getChannel().map(MapMode.READ_WRITE, 0, len);
				this.bdb.load();
			}
		} catch (IOException e) {
			throw e;
		} finally {
			bdbf.close();
		}

	}

	private int getMapFilePosition(long pos) throws IOException {
		int propLen = this.calcMapFilePos(pos);
		File f = new File(filePath);
		if ((propLen + 262144) > f.length())
			this.setMapFileLength(propLen + 1048576);
		return propLen;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#put(long, byte[])
	 */
	public void put(long pos, byte[] data) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}
		if (data.length != this.arrayLength)
			throw new IOException("data length " + data.length
					+ " does not equal " + this.arrayLength);
		this.hashlock.lock();
		int fpos = 0;
		try {
			fpos = this.getMapFilePosition(pos);
			this.bdb.position(fpos);
			this.bdb.put(data);
		} catch (BufferOverflowException e) {
			log.severe("trying to write at " + fpos + " but file length is "
					+ this.bdb.capacity());
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}
	
	public void truncate(long length) throws IOException {
		this.hashlock.lock();
		long pos = (length / Main.CHUNK_LENGTH) * FREE.length;
		this.setMapFileLength((int)pos + 1048576);
		this.hashlock.unlock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	public void remove(long pos) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}

		this.hashlock.lock();
		try {
			int fpos = this.getMapFilePosition(pos);
			this.bdb.position(fpos);
			this.bdb.put(FREE);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#get(long)
	 */
	public byte[] get(long pos) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}

		this.hashlock.lock();
		int fpos = 0;
		try {
			fpos = this.getMapFilePosition(pos);
			byte[] b = new byte[this.arrayLength];
			this.bdb.position(fpos);
			this.bdb.get(b);
			if (Arrays.equals(b, this.FREE))
				return null;
			return b;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (Exception e) {
			log.severe("error getting data at " + fpos);
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#sync()
	 */
	public void sync() throws IOException {
		this.bdb.force();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#vanish()
	 */
	public void vanish() throws IOException {
		this.hashlock.lock();
		try {
			if (!this.isClosed())
				this.close();
			File f = new File(this.filePath);
			f.delete();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	public void copy(String destFilePath) throws IOException {
		this.hashlock.lock();
		FileChannel srcC = null;
		FileChannel dstC = null;
		try {
			this.sync();

			File dest = new File(destFilePath);
			File src = new File(this.filePath);

			if (dest.exists())
				dest.delete();
			else
				dest.getParentFile().mkdirs();
			srcC = (FileChannel) Paths.get(src.getPath()).newByteChannel();
			dstC = (FileChannel) Paths.get(dest.getPath()).newByteChannel(
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.SPARSE);
			srcC.transferTo(0, src.length(), dstC);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				srcC.close();
			} catch (Exception e) {
			}
			try {
				dstC.close();
			} catch (Exception e) {
			}
			this.hashlock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#close()
	 */
	public void close() {
		this.hashlock.lock();
		try {
			if (!this.isClosed()) {
				this.closed = true;
				try {
					bdb.force();
					bdb = null;
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.gc();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.hashlock.unlock();
		}

	}
}
