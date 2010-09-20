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

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class LongByteArrayMap implements AbstractMap {

	// RandomAccessFile bdbf = null;
	MappedByteBuffer bdb = null;
	int arrayLength = 0;
	String filePath = null;
	private ReentrantLock hashlock = new ReentrantLock();
	private boolean closed = true;
	public byte[] FREE = new byte[16];
	public int iterPos = 0;
	public String fileParams = "rw";
	private long startMap = 0;
	private int maxReadBufferSize = 200 * 1024 * 1024;
	private int eI = 1024 * 1024;
	private long endPos = maxReadBufferSize;
	File dbFile = null;
	//private boolean smallMemory = false;
	public LongByteArrayMap(int arrayLength, String filePath)
			throws IOException {
		if(Runtime.getRuntime().maxMemory() < 1610612736) {
			//smallMemory = true;
			this.maxReadBufferSize = 50 * 1024 * 1024;
		}	
		this.arrayLength = arrayLength;
		this.filePath = filePath;
		FREE = new byte[arrayLength];
		Arrays.fill(FREE, (byte) 0);
		this.openFile();
		new SyncThread(this);
	}

	public LongByteArrayMap(int arrayLength, String filePath, String fileParams)
			throws IOException {
		if(Runtime.getRuntime().maxMemory() < 1610612736) {
			//smallMemory = true;
			this.maxReadBufferSize = 50 * 1024 * 1024;
		}	
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
			SDFSLogger.getLog().info("fLen = " + fLen);
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
			SDFSLogger.getLog().info("length end " + pos);

		return -1;
	}

	public byte[] nextValue() throws IOException {
		long pos = iterPos * this.arrayLength;
		byte[] val = null;
		File f = new File(this.filePath);
		while (pos < f.length()) {
			val = new byte[this.arrayLength];
			try {
				try {
					this.hashlock.lock();
					pos = (long) iterPos * (long) Main.CHUNK_LENGTH;
					iterPos++;
				} catch (Exception e1) {
				} finally {
					this.hashlock.unlock();
				}
				val = this.get(pos);
				if (val != null)
					return val;
			} catch (Exception e) {
			} finally {
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
				dbFile = new File(filePath);
				boolean fileExists = dbFile.exists();
				if (!dbFile.getParentFile().exists()) {
					dbFile.getParentFile().mkdirs();
				}
				bdbf = new RandomAccessFile(filePath, this.fileParams);
				SDFSLogger.getLog().debug("opening [" + this.filePath + "]");
				if (!fileExists) {
					bdbf.setLength(eI);
				}
				if (bdbf.length() < this.endPos) {
					this.endPos = bdbf.length();
				}
				// initiall allocate 32k
				if (this.fileParams.equalsIgnoreCase("r")) {
					this.bdb = bdbf.getChannel().map(MapMode.READ_ONLY, 0,
							this.endPos);
				} else {
					this.bdb = bdbf.getChannel().map(MapMode.READ_WRITE, 0,
							this.endPos);
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

	private long calcMapFilePos(long fpos) throws IOException {
		long pos = (fpos / Main.CHUNK_LENGTH) * FREE.length;
		/*
		 * if (pos > Integer.MAX_VALUE) throw new IOException(
		 * "Requested file position " + fpos +
		 * " is larger than the maximum length of a file for this file system "
		 * + (Integer.MAX_VALUE * Main.CHUNK_LENGTH) / FREE.length);
		 */
		return pos;
	}

	private void setMapFileLength(long start, int len) throws IOException {
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(filePath, this.fileParams);
			if (bdbf.length() < (start + len))
				bdbf.setLength(start + len);
			this.bdb = null;
			this.bdb = bdbf.getChannel().map(MapMode.READ_WRITE, start, len);
			this.bdb.load();
		} catch (IOException e) {
			SDFSLogger.getLog().fatal(
					"unable to write data to expand file at " + start
							+ " to length " + (start + len), e);
			throw new IOException("unable to expand memory map");
		} finally {
			bdbf.close();
		}

	}

	private int getMapFilePosition(long pos) throws IOException {
		long propLen = this.calcMapFilePos(pos);
		int bPos = (int) (propLen - startMap);
		if (propLen < this.startMap) {
			this.startMap = propLen;
			int mlen = (int) (this.endPos - this.startMap);
			if (mlen > this.maxReadBufferSize) {
				mlen = this.maxReadBufferSize;
			} else if ((dbFile.length() -propLen) <= this.maxReadBufferSize) {
				mlen = (int) (dbFile.length()-propLen);
			}
			this.endPos = this.startMap + mlen;
			this.setMapFileLength(startMap, mlen);
			bPos = 0;
			SDFSLogger.getLog()
					.debug(
							"expanded buffer to " + startMap + " end is "
									+ this.endPos);
		}else if(propLen >=(this.endPos - FREE.length)) {
			int mlen = (int) (propLen - this.startMap);
			if (mlen >= this.maxReadBufferSize) {
				this.startMap = propLen;
				mlen = eI;
				this.endPos = this.startMap + mlen;
				this.setMapFileLength(startMap, mlen);
				bPos = 0;
			} else {
				mlen = mlen + bPos + eI;
				this.endPos = this.startMap + mlen;
				this.setMapFileLength(startMap, mlen);
			}
		}
		return bPos;
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
			SDFSLogger.getLog().fatal(
					"trying to write at " + fpos + " but file length is "
							+ this.bdb.capacity());
			throw e;
		} catch (Exception e) {
			// System.exit(-1);
			throw new IOException(e);
		} finally {
			this.hashlock.unlock();
		}
	}

	public void truncate(long length) throws IOException {
		this.hashlock.lock();
		int mlen = eI;
		if (length >= this.maxReadBufferSize) {
			this.startMap = length - this.maxReadBufferSize;
			mlen = this.maxReadBufferSize;
		} else {
			this.startMap = 0;
			mlen = (int) length;
		}
		if (mlen == 0)
			mlen = eI;
		this.setMapFileLength(this.startMap, mlen);
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
			SDFSLogger.getLog().fatal(
					"error getting data at " + fpos + " buffer capacity="
							+ this.bdb.capacity(), e);
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
