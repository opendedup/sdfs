package org.opendedup.collections;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.util.RAFPool;
import org.opendedup.util.SDFSLogger;

public class LongByteArrayMap implements AbstractMap {

	// RandomAccessFile bdbf = null;
	private static final int arrayLength = 1 + Main.hashLength + 1 + 8;
	String filePath = null;
	private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	private boolean closed = true;
	public static byte[] FREE = new byte[Main.hashLength];
	public int iterPos = 0;
	FileChannel bdbc = null;
	// private int maxReadBufferSize = Integer.MAX_VALUE;
	// private int eI = 1024 * 1024;
	// private long endPos = maxReadBufferSize;
	File dbFile = null;
	Path bdbf = null;
	FileChannel iterbdb = null;
	FileChannel pbdb = null;
	BitSet map = null;
	File ewahFile = null;
	long flen = 0;
	RAFPool rafPool = null;

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
		this.iterPos = 0;
		iterbdb = (FileChannel) bdbf.newByteChannel(StandardOpenOption.CREATE,
				StandardOpenOption.WRITE, StandardOpenOption.READ,
				StandardOpenOption.SPARSE);
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public long nextKey() throws IOException {
		iterlock.lock();
		try {
			File f = new File(this.filePath);
			while (this.iterbdb.position() < f.length()) {
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
			if ((iterPos * arrayLength) != f.length())
				throw new IOException("did not reach end of file for ["
						+ f.getPath() + "] len=" + iterPos * arrayLength
						+ " file len =" + f.length());

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
			File f = new File(this.filePath);
			while ((iterPos * arrayLength) < f.length()) {
				try {
					if (map == null || map.get(iterPos)) {
						ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
						iterbdb.position(iterPos * arrayLength);
						iterbdb.read(buf);
						byte[] val = buf.array();
						if (!Arrays.equals(val, FREE)) {
							return val;
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					iterPos++;
				}
			}
			if ((iterPos * arrayLength) != f.length())
				throw new IOException("did not reach end of file for ["
						+ f.getPath() + "] len=" + iterPos * arrayLength
						+ " file len =" + f.length());
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
			this.hashlock.writeLock().lock();
			bdbf = Paths.get(filePath);
			try {
				dbFile = new File(filePath);
				String ewahFilePath = filePath.substring(0,
						filePath.length() - 4) + ".ewa";
				ewahFile = new File(ewahFilePath);
				boolean fileExists = dbFile.exists();
				SDFSLogger.getLog().debug("opening [" + this.filePath + "]");
				if (!fileExists) {
					if (!dbFile.getParentFile().exists()) {
						dbFile.getParentFile().mkdirs();
					}
					FileChannel bdb = (FileChannel) bdbf.newByteChannel(
							StandardOpenOption.CREATE,
							StandardOpenOption.WRITE, StandardOpenOption.READ,
							StandardOpenOption.SPARSE);
					bdb.position(1024);
					bdb.close();
					
					flen = 0;
					this.map = new BitSet();
					SDFSLogger.getLog().info("creating map file");
				} else {
					if (ewahFile.exists()) {
						ObjectInputStream in = null;
						try {
							in = new ObjectInputStream(new FileInputStream(
									ewahFile));
							this.map = (BitSet) in.readObject();
						} catch (Exception e) {
							SDFSLogger.getLog().warn(
									"unable to read ewa file "
											+ ewahFile.getPath(), e);
							this.map = null;
						} finally {
							if (in != null)
								in.close();
						}
					}
					flen = dbFile.length();
				}
				rafPool = new RAFPool(filePath);
				pbdb = (FileChannel) bdbf.newByteChannel(
						StandardOpenOption.CREATE, StandardOpenOption.WRITE,
						StandardOpenOption.READ, StandardOpenOption.SPARSE);
				// initiall allocate 32k
				this.closed = false;
			} catch (IOException e) {
				SDFSLogger.getLog().error("unable to open file " + filePath);
				throw e;
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				this.hashlock.writeLock().unlock();
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

	private int calcBitMapPos(long fpos) throws IOException {
		int pos = (int) (fpos / Main.CHUNK_LENGTH);
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

			// _bdb.lock(fpos, data.length, false);
			this.hashlock.writeLock().lock();
			if (map != null)
				map.set(this.calcBitMapPos(pos));
			else {
				if (fpos > flen)
					flen = fpos;
			}
			pbdb.write(ByteBuffer.wrap(data), fpos);
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
				this.hashlock.writeLock().unlock();
			} catch (Exception e) {
			}
		}
	}

	public void truncate(long length) throws IOException {
		this.hashlock.writeLock().lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(length);
			_bdb = (FileChannel) bdbf.newByteChannel(StandardOpenOption.CREATE,
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
			this.hashlock.writeLock().unlock();
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
		this.hashlock.writeLock().lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(pos);
			_bdb = (FileChannel) bdbf.newByteChannel(StandardOpenOption.CREATE,
					StandardOpenOption.WRITE, StandardOpenOption.READ,
					StandardOpenOption.SPARSE);
			_bdb.write(ByteBuffer.wrap(FREE), fpos);
			if (map != null)
				map.clear(this.calcBitMapPos(pos));
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				_bdb.close();
			} catch (Exception e) {
			}
			this.hashlock.writeLock().unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#get(long)
	 * 
	 */
	public byte[] get(long pos) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.filePath + "] is close");
		}
		this.hashlock.readLock().lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(pos);
			if (map != null && !map.get(this.calcBitMapPos(pos)))
				return null;
			else if (map == null && fpos > flen)
				return null;
			_bdb = rafPool.borrowObject();
			ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
			_bdb.position(fpos);
			_bdb.read(buf);
			byte[] b = buf.array();
			if (Arrays.equals(b, FREE))
				return null;
			return b;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"error getting data at " + fpos + " buffer capacity="
							+ dbFile.length(), e);
			throw new IOException(e);
		} finally {
			try {
				if (_bdb != null)
					rafPool.returnObject(_bdb);
			} catch (Exception e) {
			}
			this.hashlock.readLock().unlock();
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
		this.hashlock.writeLock().lock();
		try {
			if (!this.isClosed())
				this.close();
			File f = new File(this.filePath);
			f.delete();
			this.ewahFile.delete();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.writeLock().unlock();
		}
	}

	public void copy(String destFilePath) throws IOException {
		this.hashlock.readLock().lock();
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
			srcC = (FileChannel) Paths.get(src.getPath()).newByteChannel(
					StandardOpenOption.READ, StandardOpenOption.SPARSE);
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
			this.hashlock.readLock().unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#close()
	 */
	public void close() {
		SDFSLogger.getLog().info("Closing file");
		this.hashlock.writeLock().lock();
		if (map != null) {
			ObjectOutputStream out = null;
			try {
				out = new ObjectOutputStream(
						new FileOutputStream(this.ewahFile));
				out.writeObject(this.map);
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"unable to write ewah file " + this.ewahFile.getPath(),
						e);
			} finally {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
		this.rafPool.close();
		dbFile = null;
		if (!this.isClosed()) {
			this.closed = true;
		}
		try {
			pbdb.close();
		} catch (Exception e) {
		} finally {
			this.hashlock.writeLock().unlock();
		}
	}
}
