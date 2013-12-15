package org.opendedup.collections;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.OSValidator;

import sun.nio.ch.FileChannelImpl;

public class LongByteArrayMap implements AbstractMap {
	private static ArrayList<LongByteArrayMapListener> mapListener = new ArrayList<LongByteArrayMapListener>();
	private static final byte swversion = 1;
	// RandomAccessFile bdbf = null;
	private static final int _arrayLength = 1 + HashFunctionPool.hashLength + 1 + 8;
	private static final int _v1arrayLength = 1 + HashFunctionPool.hashLength + 1 + 1+8+8;
	private static final int _v1offset = 64;
	private static final short magicnumber = 6442;
	String filePath = null;
	private ReentrantLock hashlock = new ReentrantLock();
	private boolean closed = true;
	public static byte[] _FREE = new byte[_arrayLength];
	public static byte[] _V1FREE = new byte[_v1arrayLength];
	public long iterPos = 0;
	FileChannel bdbc = null;
	// private int maxReadBufferSize = Integer.MAX_VALUE;
	// private int eI = 1024 * 1024;
	// private long endPos = maxReadBufferSize;
	File dbFile = null;
	Path bdbf = null;
	// FileChannel iterbdb = null;
	FileChannelImpl pbdb = null;
	RandomAccessFile rf = null;
	private int offset = _v1offset;
	private int arrayLength = _v1arrayLength;
	public byte version = swversion;
	public byte [] FREE;
	long flen = 0;

	static {
		_FREE = new byte[_arrayLength];
		_V1FREE = new byte[_v1arrayLength];
		Arrays.fill(_FREE, (byte) 0);
		Arrays.fill(_V1FREE, (byte) 0);
	}

	public static void addMapListener(LongByteArrayMapListener l) {
		mapListener.add(l);
	}

	public static void removeMapListener(LongByteArrayMapListener l) {
		mapListener.remove(l);
	}

	public static ArrayList<LongByteArrayMapListener> getMapListeners() {
		return mapListener;
	}

	// private boolean smallMemory = false;
	public LongByteArrayMap(String filePath) throws IOException {
		this.filePath = filePath;
		this.openFile();

	}

	public void iterInit() throws IOException {
		iterlock.lock();
		try {
			this.iterPos = 0;

		} finally {
			iterlock.unlock();
		}
	}

	public long getIterFPos() {
		return (this.iterPos * arrayLength) + this.offset;
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public long nextKey() throws IOException {
		iterlock.lock();
		try {
			long _cpos = getIterFPos();
			while (_cpos < flen) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					long pos = iterPos * Main.CHUNK_LENGTH;
					pbdb.read(buf, _cpos);
					byte[] val = buf.array();
					iterPos++;
					if (!Arrays.equals(val, FREE)) {
						return pos;
					}
				} catch (Exception e1) {
					SDFSLogger.getLog().debug(
							"unable to iterate through key at " + iterPos
									* arrayLength, e1);
				} finally {
					iterPos++;
					_cpos = getIterFPos();
				}
			}
			if ((iterPos * arrayLength)+ this.offset != flen)
				throw new IOException("did not reach end of file for ["
						+ this.filePath + "] len=" + iterPos * arrayLength
						+ " file len =" + flen);

			return -1;
		} finally {
			iterlock.unlock();
		}
	}

	public byte[] nextValue() throws IOException {
		iterlock.lock();
		try {
			long _cpos = getIterFPos();
			while (_cpos < flen) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					pbdb.read(buf, _cpos);
					byte[] val = buf.array();
					if (!Arrays.equals(val, FREE)) {
						return val;
					}
				} finally {
					iterPos++;
					_cpos = (iterPos * arrayLength)+ this.offset;
				}
			}
			if (getIterFPos() < pbdb.size()) {
				this.hashlock.lock();
				try {
					flen = this.pbdb.size();
				} finally {
					this.hashlock.unlock();
				}
				return this.nextValue();
			}
			return null;
		} finally {
			iterlock.unlock();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.closed;
	}
	
	private void intVersion () {
		if(version == 0) {
			this.FREE = _FREE;
			this.offset = 0;
			this.arrayLength = _arrayLength;
		} if(version == 1) {
			this.FREE = _V1FREE;
			this.offset = 64;
			this.arrayLength = _v1arrayLength;
		}
	}

	private void openFile() throws IOException {
		if (this.closed) {
			this.hashlock.lock();
			bdbf = Paths.get(filePath);
			try {
				dbFile = new File(filePath);
				boolean fileExists = dbFile.exists();
				SDFSLogger.getLog().debug("opening [" + this.filePath + "]");
				if (!fileExists) {
					if (!dbFile.getParentFile().exists()) {
						dbFile.getParentFile().mkdirs();
					}
					FileChannel bdb = (FileChannel) Files.newByteChannel(bdbf,
							StandardOpenOption.CREATE,
							StandardOpenOption.WRITE, StandardOpenOption.READ,
							StandardOpenOption.SPARSE);
					ByteBuffer buf = ByteBuffer.allocate(3);
					buf.putShort(magicnumber);
					buf.put(swversion);
					bdb.position(0);
					bdb.write(buf);
					bdb.position(1024);
					bdb.close();
					flen = 0;
				} else {
					
					flen = dbFile.length();
				}
				rf = new RandomAccessFile(filePath, "rw");
				pbdb = (FileChannelImpl) Files.newByteChannel(bdbf,
						StandardOpenOption.CREATE, StandardOpenOption.WRITE,
						StandardOpenOption.READ, StandardOpenOption.SPARSE);
				ByteBuffer buf = ByteBuffer.allocate(3);
				pbdb.read(buf);
				if(buf.getShort() == magicnumber) {
					this.version = buf.get();
				} else {
					this.version = 0;
				}
				this.intVersion();
				// initiall allocate 32k
				this.closed = false;
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to open file " + filePath, e);
				throw new IOException(e);
			} finally {
				this.hashlock.unlock();
			}
		}
	}

	private long getMapFilePosition(long pos) throws IOException {
		long propLen = ((pos / Main.CHUNK_LENGTH) * FREE.length) + this.offset;
		return propLen;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#put(long, byte[])
	 */

	public void put(long pos, byte[] data) throws IOException {
		put(pos, data, true);
	}

	public void put(long pos, byte[] data, boolean propigateEvent)
			throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}
		if (data.length != arrayLength)
			throw new IOException("data length " + data.length
					+ " does not equal " + arrayLength);
		long fpos = 0;
		fpos = this.getMapFilePosition(pos);

		//
		this.hashlock.lock();
		if (fpos > flen)
			flen = fpos;
		this.hashlock.unlock();
		// rf.seek(fpos);
		// rf.write(data);
		pbdb.write(ByteBuffer.wrap(data), fpos);
	}

	public void truncate(long length) throws IOException {
		truncate(length, true);
	}

	public void truncate(long length, boolean propigateEvent)
			throws IOException {
		this.hashlock.lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(length);
			_bdb = (FileChannel) Files.newByteChannel(bdbf,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ, StandardOpenOption.SPARSE);
			_bdb.truncate(fpos);
		} catch (Exception e) {
			// System.exit(-1);
			throw new IOException(e);
		} finally {
			try {
				_bdb.close();
			} catch (Exception e) {
			}
			this.flen = fpos;
			this.hashlock.unlock();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	public void remove(long pos) throws IOException {
		remove(pos, true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	public void remove(long pos, boolean propigateEvent) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}
		this.hashlock.lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(pos);
			_bdb = (FileChannel) Files.newByteChannel(bdbf,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ, StandardOpenOption.SPARSE);
			_bdb.write(ByteBuffer.wrap(FREE), fpos);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				_bdb.close();
			} catch (Exception e) {
			}
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

		long fpos = 0;
		try {
			fpos = this.getMapFilePosition(pos);

			if (fpos > flen)
				return null;
			byte[] buf = new byte[arrayLength];
			pbdb.read(ByteBuffer.wrap(buf), fpos);
			if (Arrays.equals(buf, FREE))
				return null;
			return buf;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"error getting data at " + fpos + " buffer capacity="
							+ dbFile.length(), e);
			throw new IOException(e);
		} finally {

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#sync()
	 */
	@Override
	public void sync() throws IOException {
		/*
		 * FileChannel _bdb = null; try { _bdb = (FileChannel)
		 * bdbf.newByteChannel(StandardOpenOption.WRITE,
		 * StandardOpenOption.READ, StandardOpenOption.SPARSE);
		 * _bdb.force(true); } catch (IOException e) {
		 * 
		 * } finally { try { _bdb.close(); } catch (Exception e) { } }
		 */
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#vanish()
	 */
	@Override
	public void vanish() throws IOException {
		vanish(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#vanish()
	 */
	@Override
	public void vanish(boolean propigateEvent) throws IOException {
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
		copy(destFilePath, true);
	}

	public void copy(String destFilePath, boolean propigateEvent)
			throws IOException {
		this.hashlock.lock();
		FileChannel srcC = null;
		FileChannel dstC = null;
		try {
			this.sync();
			SDFSLogger.getLog().debug("copying to " + destFilePath);
			File dest = new File(destFilePath);
			File src = new File(this.filePath);
			if (dest.exists())
				dest.delete();
			else
				dest.getParentFile().mkdirs();
			if (OSValidator.isWindows()) {
				srcC = (FileChannel) Files.newByteChannel(
						Paths.get(src.getPath()), StandardOpenOption.READ,
						StandardOpenOption.SPARSE);
				dstC = (FileChannel) Files.newByteChannel(
						Paths.get(src.getPath()), StandardOpenOption.CREATE,
						StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
				srcC.transferTo(0, src.length(), dstC);
			} else {
				SDFSLogger.getLog().debug("snapping on unix/linux volume");
				String cpCmd = "cp --sparse=always " + src.getPath() + " "
						+ dest.getPath();
				SDFSLogger.getLog().debug(cpCmd);
				Process p = Runtime.getRuntime().exec(cpCmd);
				int exitValue = p.waitFor();
				if (exitValue != 0) {
					throw new IOException("unable to copy " + src.getPath()
							+ " to  " + dest.getPath() + " exit value was "
							+ exitValue);
				}
				SDFSLogger.getLog().debug("copy exit value is " + p.waitFor());
			}
			SDFSLogger.getLog()
					.debug("snapped map to [" + dest.getPath() + "]");
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
	@Override
	public void close() {
		this.hashlock.lock();
		dbFile = null;
		if (!this.isClosed()) {
			this.closed = true;
		}
		try {
			pbdb.force(true);
			pbdb.close();
		} catch (Exception e) {
		} finally {
			this.hashlock.unlock();
		}
		try {
			this.rf.close();
		} catch (Exception e) {
		}
	}

}
