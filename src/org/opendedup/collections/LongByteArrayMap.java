package org.opendedup.collections;

import java.io.File;

import java.io.RandomAccessFile;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.util.OSValidator;
import org.opendedup.util.SDFSLogger;

import sun.nio.ch.FileChannelImpl;

public class LongByteArrayMap implements AbstractMap {

	// RandomAccessFile bdbf = null;
	private static final int arrayLength = 1 + Main.hashLength + 1 + 8;
	String filePath = null;
	private ReentrantLock hashlock = new ReentrantLock();
	private boolean closed = true;
	public static byte[] FREE = new byte[Main.hashLength];
	public long iterPos = 0;
	FileChannel bdbc = null;
	// private int maxReadBufferSize = Integer.MAX_VALUE;
	// private int eI = 1024 * 1024;
	// private long endPos = maxReadBufferSize;
	File dbFile = null;
	Path bdbf = null;
	FileChannel iterbdb = null;
	RandomAccessFile rf = null;
	long flen = 0;

	static {
		FREE = new byte[arrayLength];
		Arrays.fill(FREE, (byte) 0);
	}

	// private boolean smallMemory = false;
	public LongByteArrayMap(String filePath) throws IOException {
		this.filePath = filePath;
		this.openFile();
		new SyncThread(this);

	}

	public LongByteArrayMap(String filePath, String fileParams)
			throws IOException {
		this.filePath = filePath;
		this.openFile();
		new SyncThread(this);
	}

	public void iterInit() throws IOException {
		iterlock.lock();
		try {
			this.iterPos = 0;
			iterbdb = (FileChannel) Files
					.newByteChannel(bdbf,StandardOpenOption.READ);
		} finally {
			iterlock.unlock();
		}
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public long nextKey() throws IOException {
		iterlock.lock();
		try {
			while (this.iterbdb.position() < flen) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					long pos = iterPos * Main.CHUNK_LENGTH;
					iterbdb.position(iterPos * arrayLength);
					iterbdb.read(buf);
					byte[] val = buf.array();
					iterPos++;
					if (!Arrays.equals(val, FREE)) {
						return pos;
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
			if ((iterPos * arrayLength) != flen)
				throw new IOException("did not reach end of file for ["
						+ this.filePath + "] len=" + iterPos * arrayLength
						+ " file len =" + flen);

			try {
				iterbdb.close();
				iterbdb = null;
			} catch (Exception e) {
			}
			return -1;
		} finally {
			iterlock.unlock();
		}
	}

	public byte[] nextValue() throws IOException {
		iterlock.lock();
		try {
			long _cpos = (long)(iterPos * (long)arrayLength);
			while (_cpos < flen) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					iterbdb.read(buf, _cpos);
					byte[] val = buf.array();
					if (!Arrays.equals(val, FREE)) {
						return val;
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					iterPos++;
					_cpos = (long)(iterPos * (long)arrayLength);
				}
			}
			if ((iterPos * arrayLength) != iterbdb.size()) {
				SDFSLogger.getLog().warn(
						"did not reach end of file for [" + this.filePath
								+ "] len=" + iterPos * arrayLength
								+ " file len =" + iterbdb.size());
				this.hashlock.lock();
				try {
					flen = this.iterbdb.size();
				} finally {
					this.hashlock.unlock();
				}
				return this.nextValue();
			}
			try {
				iterbdb.close();
				iterbdb = null;
			} catch (Exception e) {
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
	public boolean isClosed() {
		return this.closed;
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
					bdb.position(1024);
					bdb.close();

					flen = 0;
				} else {

					flen = dbFile.length();
				}
				rf = new RandomAccessFile(filePath, "rw");
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

	private long getMapFilePosition(long pos) throws IOException {
		long propLen = this.calcMapFilePos(pos);
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
		if (data.length != arrayLength)
			throw new IOException("data length " + data.length
					+ " does not equal " + arrayLength);
		long fpos = 0;
		try {
			fpos = this.getMapFilePosition(pos);

			//
			this.hashlock.lock();
			if (fpos > flen)
				flen = fpos;

			rf.seek(fpos);
			rf.write(data);
			//pbdb.write(ByteBuffer.wrap(data), fpos);
		} catch (BufferOverflowException e) {
			SDFSLogger.getLog().fatal(
					"trying to write at " + fpos + " but file length is "
							+ dbFile.length());
			throw e;
		} catch (Exception e) {
			// System.exit(-1);
			throw new IOException(e);
		} finally {
			try {
				this.hashlock.unlock();
			} catch (Exception e) {
			}
		}
	}

	public void truncate(long length) throws IOException {
		this.hashlock.lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(length);
			_bdb = (FileChannel) Files.newByteChannel(bdbf,StandardOpenOption.CREATE,
					StandardOpenOption.WRITE, StandardOpenOption.READ,
					StandardOpenOption.SPARSE);
			_bdb.truncate(fpos);
		} catch (Exception e) {
			// System.exit(-1);
			throw new IOException(e);
		} finally {
			try {
				_bdb.close();
			} catch (Exception e) {
			}
			this.hashlock.unlock();
		}
		this.flen = fpos;
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
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(pos);
			_bdb = (FileChannel) Files.newByteChannel(bdbf,StandardOpenOption.CREATE,
					StandardOpenOption.WRITE, StandardOpenOption.READ,
					StandardOpenOption.SPARSE);
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
			this.hashlock.lock();
			try {
				rf.seek(fpos);
				rf.read(buf);
				//pbdb.read(ByteBuffer.wrap(buf), fpos);
			} finally {
				this.hashlock.unlock();
			}
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
			if (OSValidator.isWindows()) {
				SDFSLogger.getLog().info("Snapping on windows volume");
				srcC = (FileChannel) Files.newByteChannel(Paths.get(src.getPath()),
						StandardOpenOption.READ, StandardOpenOption.SPARSE);
				dstC = (FileChannel) Files.newByteChannel(Paths.get(src.getPath()),
						StandardOpenOption.CREATE, StandardOpenOption.WRITE,
						StandardOpenOption.SPARSE);
				srcC.transferTo(0, src.length(), dstC);
			} else {
				SDFSLogger.getLog().debug("snapping on unix/linux volume");
				Process p = Runtime.getRuntime().exec(
						"cp --sparse=always " + src.getPath() + " "
								+ dest.getPath());
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
	public void close() {
		this.hashlock.lock();
		dbFile = null;
		if (!this.isClosed()) {
			this.closed = true;
		}
		try {
			this.rf.close();
		} catch (Exception e) {
		} finally {
			this.hashlock.unlock();
		}
	}
}
