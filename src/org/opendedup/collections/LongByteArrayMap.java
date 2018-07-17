/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
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
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.io.FileClosedException;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.OSValidator;

import com.google.common.primitives.Longs;

public class LongByteArrayMap implements DataMapInterface {
	private static final int _arrayLength = (1 + HashFunctionPool.hashLength + 1 + 8)
			* HashFunctionPool.max_hash_cluster;
	public static final int _v1arrayLength = 4
			+ ((HashFunctionPool.hashLength + 8) * HashFunctionPool.max_hash_cluster);
	public static final int MAX_ELEMENTS_PER_AR = HashFunctionPool.max_hash_cluster * 2;
	private static final int _v2arrayLength = 1 + 4 + 4 + 4 + (HashLocPair.BAL * MAX_ELEMENTS_PER_AR);
	private static final int _v1offset = 64;
	private static final int _v2offset = 256;
	private static final short magicnumber = 6442;
	String filePath = null;
	private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	private boolean closed = true;
	public static byte[] _FREE = new byte[_arrayLength];
	public static byte[] _V1FREE = new byte[_v1arrayLength];
	public static byte[] _V2FREE = new byte[_v2arrayLength];
	public AtomicLong iterPos = new AtomicLong();
	FileChannel bdbc = null;
	// private int maxReadBufferSize = Integer.MAX_VALUE;
	// private int eI = 1024 * 1024;
	// private long endPos = maxReadBufferSize;
	File dbFile = null;
	Path bdbf = null;
	// FileChannel iterbdb = null;
	FileChannel pbdb = null;
	RandomAccessFile rf = null;
	private int offset = 0;
	private int arrayLength = _v1arrayLength;
	private byte version = Main.MAPVERSION;
	private byte[] FREE;
	private AtomicInteger opens = new AtomicInteger();
	private static ConcurrentHashMap<String, LongByteArrayMap> mp = new ConcurrentHashMap<String, LongByteArrayMap>();
	private static ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
	private static ReentrantLock iLock = new ReentrantLock(true);
	private String lookupFilter = null;

	static {
		SDFSLogger.getLog().info("File Map Version is = " + Main.MAPVERSION);
		_FREE = new byte[_arrayLength];
		_V1FREE = new byte[_v1arrayLength];
		_V2FREE = new byte[_v2arrayLength];
		Arrays.fill(_FREE, (byte) 0);
		Arrays.fill(_V1FREE, (byte) 0);
		Arrays.fill(_V2FREE, (byte) 0);
	}

	private static ReentrantLock getLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			if (l == null) {
				l = new ReentrantLock(true);
				activeTasks.put(st, l);
			}
			return l;
		} finally {
			iLock.unlock();
		}
	}

	private static void removeLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			try {

				if (l != null && !l.hasQueuedThreads()) {
					activeTasks.remove(st);
				}
			} finally {
				if (l != null)
					l.unlock();
			}
		} finally {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("hmpa size=" + activeTasks.size());
			iLock.unlock();
		}
	}

	private LongByteArrayMap(String filePath, String lookupFilter) throws IOException {
		this.filePath = filePath;
		this.lookupFilter = lookupFilter;
		this.openFile();
	}
	
	private static AtomicLong openFiles = new AtomicLong();

	public static LongByteArrayMap getMap(String GUID, String lookupFilter) throws IOException {
		File mapFile = new File(Main.dedupDBStore + File.separator + GUID.substring(0, 2) + File.separator + GUID
				+ File.separator + GUID + ".map");
		ReentrantLock l = getLock(GUID);
		l.lock();
		try {
			LongByteArrayMap map = mp.get(mapFile.getPath());
			if (map != null) {
				map.openFile();
				return map;
			} else {
				File zmapFile = new File(Main.dedupDBStore + File.separator + GUID.substring(0, 2) + File.separator
						+ GUID + File.separator + GUID + ".map.lz4");
				if (zmapFile.exists()) {
					try {
					map = new LongByteArrayMap(zmapFile.getPath(), lookupFilter);
					}catch(java.io.EOFException e) {
						if(mapFile.exists()) {
							map = new LongByteArrayMap(mapFile.getPath(), lookupFilter);
							zmapFile.delete();
						}
					}
				}
				else
					map = new LongByteArrayMap(mapFile.getPath(), lookupFilter);
				mp.put(mapFile.getPath(), map);
				return map;
			}
		} finally {
			removeLock(GUID);
		}
	}

	private void addOpen() {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.opens.incrementAndGet();
		} finally {
			l.unlock();
		}
	}

	public byte getVersion() {
		return this.version;
	}

	public byte[] getFree() {
		return this.FREE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#iterInit()
	 */
	@Override
	public void iterInit() throws IOException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.iterPos.set(0);

		} finally {
			l.unlock();
		}
	}

	private long getInternalIterFPos() {
		return (this.iterPos.get() * arrayLength) + this.offset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#getIterPos()
	 */
	@Override
	public long getIterPos() {
		return (this.iterPos.get() * arrayLength);
	}

	public void setLogicalIterPos(long pos) throws IOException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {
			long ipos = this.getMapFilePosition(pos);
			this.iterPos.set(ipos / arrayLength);
		}finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#nextKey()
	 */
	@Override
	public long nextKey() throws IOException, FileClosedException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}
			long _cpos = getInternalIterFPos();
			while (_cpos < this.dbFile.length()) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					long pos = iterPos.get() * Main.CHUNK_LENGTH;
					pbdb.read(buf, _cpos);
					byte[] val = buf.array();
					iterPos.incrementAndGet();
					if (!Arrays.equals(val, FREE)) {
						return pos;
					}
				} catch (Exception e1) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("unable to iterate through key at " + iterPos.get() * arrayLength,
								e1);
				} finally {
					iterPos.incrementAndGet();
					_cpos = getInternalIterFPos();
				}
			}
			if ((iterPos.get() * arrayLength) + this.offset != this.dbFile.length())
				throw new IOException("did not reach end of file for [" + this.filePath + "] len="
						+ iterPos.get() * arrayLength + " file len =" + this.dbFile.length());

			return -1;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#nextValue()
	 */
	byte[][] nbufs = null;
	final static int NP = 64;
	ByteBuffer nbuf = null;

	@Override
	public SparseDataChunk nextValue(boolean index) throws IOException, FileClosedException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}
			long _cpos = getInternalIterFPos();
			while (_cpos < this.dbFile.length()) {
				try {
					if (nbuf == null) {
						nbuf = ByteBuffer.allocate(arrayLength);
					}
					nbuf.position(0);
					pbdb.read(nbuf, _cpos);
					// SDFSLogger.getLog().info("al=" + al + " cpos=" +_cpos
					// + " flen=" + flen + " fz=" +this.dbFile.length() + " nbfs=" +
					// nbuf.position());
					nbuf.position(0);
					// SDFSLogger.getLog().info("arl=" + arrayLength + "
					// nbufsz=" + nbuf.capacity() + " rem="+ nbuf.remaining());
					byte[] val = new byte[arrayLength];
					nbuf.get(val);
					if (!Arrays.equals(val, FREE)) {
						SparseDataChunk ck = new SparseDataChunk(val, this.version);
						if (index) {
							for (HashLocPair p : ck.getFingers().values()) {
								DedupFileStore.addRef(p.hash, Longs.fromByteArray(p.hashloc), 1, lookupFilter);
							}
						}
						return ck;
					}
				} finally {
					iterPos.incrementAndGet();
					_cpos = (iterPos.get() * arrayLength) + this.offset;
				}
			}
			return null;

		} finally {
			l.unlock();
		}

	}

	public LongKeyValue nextKeyValue(boolean index) throws IOException, FileClosedException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}
			long _cpos = getInternalIterFPos();
			//SDFSLogger.getLog().info("cpos=" + _cpos + " fl=" +this.dbFile.length());
			while (_cpos < this.dbFile.length()) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					pbdb.read(buf, _cpos);
					byte[] val = buf.array();
					if (!Arrays.equals(val, FREE)) {
						SparseDataChunk ck = new SparseDataChunk(val, this.version);
						if (index) {
							for (HashLocPair p : ck.getFingers().values()) {
								DedupFileStore.addRef(p.hash, Longs.fromByteArray(p.hashloc), 1, lookupFilter);
							}
						}
						return new LongKeyValue(iterPos.get() * Main.CHUNK_LENGTH, ck);
					}
				} finally {
					iterPos.incrementAndGet();
					_cpos = (iterPos.get() * arrayLength) + this.offset;
				}
			}
			return null;
		} finally {
			l.unlock();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.closed;
	}

	private void intVersion() {
		if (version == 0) {
			this.FREE = _FREE;
			this.offset = 0;
			this.arrayLength = _arrayLength;
		}
		if (version == 1) {
			this.FREE = _V1FREE;
			this.offset = _v1offset;
			this.arrayLength = _v1arrayLength;
		}
		if (version == 2) {
			this.FREE = _V2FREE;
			this.offset = _v2offset;
			this.arrayLength = _v2arrayLength;
		}
		if (version == 3) {
			this.FREE = _V2FREE;
			this.offset = _v2offset;
			this.arrayLength = _v2arrayLength;
		}
	}

	private void openFile() throws IOException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {

			if (this.closed) {

				File fp = new File(filePath);
				if (fp.exists() && filePath.endsWith(".lz4")) {
					String nfp = filePath.substring(0, filePath.length() - 4);
					CompressionUtils.decompressFile(new File(filePath), new File(nfp));
					if (!new File(nfp).exists())
						throw new IOException("File could not be decompressed " + nfp);
					if (!new File(filePath).delete())
						throw new IOException("unable to delete" + filePath);
					filePath = nfp;
				}
				try {
					bdbf = Paths.get(filePath);

					dbFile = new File(filePath);
					boolean fileExists = dbFile.exists();
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("opening [" + this.filePath + "]");
					if (!fileExists) {
						if (!dbFile.getParentFile().exists()) {
							dbFile.getParentFile().mkdirs();
						}
						FileChannel bdb = (FileChannel) Files.newByteChannel(bdbf, StandardOpenOption.CREATE,
								StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.SPARSE);
						if (version > 0) {
							// SDFSLogger.getLog().info("Writing version " +
							// this.version);
							ByteBuffer buf = ByteBuffer.allocate(3);
							buf.putShort(magicnumber);
							buf.put(this.version);
							buf.position(0);
							bdb.position(0);
							bdb.write(buf);
						}
						bdb.position(1024);
						bdb.close();

					}
					rf = new RandomAccessFile(filePath, "rw");

					pbdb = rf.getChannel();
					ByteBuffer buf = ByteBuffer.allocate(3);
					pbdb.position(0);
					pbdb.read(buf);
					buf.position(0);
					if (buf.getShort() == magicnumber) {
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
				}
			}
			this.addOpen();
			mp.put(this.filePath, this);
		} finally {
			l.unlock();
		}
	}

	private long getMapFilePosition(long pos) throws IOException {
		long propLen = ((pos / Main.CHUNK_LENGTH) * FREE.length) + this.offset;
		return propLen;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#put(long, byte[])
	 */
	@Override
	public void put(long pos, SparseDataChunk data) throws IOException, FileClosedException {
		long fpos = 0;
		fpos = this.getMapFilePosition(pos);

		//
		Lock l = this.hashlock.readLock();
		l.lock();
		try {

			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}
			/*
			 * if (data.length != arrayLength) throw new IOException("data length " +
			 * data.length + " does not equal " + arrayLength);
			 */
			/*
			 * if (Main.refCount) { SparseDataChunk ck = this.get(pos); if (ck != null) {
			 * for (HashLocPair p : ck.getFingers().values()) {
			 * DedupFileStore.removeRef(p.hash, Longs.fromByteArray(p.hashloc)); } }
			 */
			/*
			 * for (HashLocPair p : data.getFingers().values()) {
			 * DedupFileStore.addRef(p.hash, Longs.fromByteArray(p.hashloc)); }
			 */
			// }
			// rf.seek(fpos);
			// rf.write(data);
			pbdb.write(ByteBuffer.wrap(data.getBytes()), fpos);
		} finally {
			l.unlock();
		}
	}

	@Override
	public void putIfNull(long pos, SparseDataChunk data) throws IOException, FileClosedException {
		SparseDataChunk ck = this.get(pos);
		if (ck == null) {
			this.put(pos, data);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#trim(long, int)
	 */
	@Override
	public synchronized void trim(long pos, int len) throws FileClosedException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {
			double spos = Math.ceil(((double) pos / (double) Main.CHUNK_LENGTH));
			long ep = pos + len;
			double epos = Math.floor(((double) ep / (double) Main.CHUNK_LENGTH));
			long ls = ((long) spos * (long) FREE.length) + (long) this.offset;
			long es = ((long) epos * (long) FREE.length) + (long) this.offset;
			if (es <= ls)
				return;
			else {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("will trim from " + ls + " to " + es);
				FileChannel _bdb = null;
				ByteBuffer buff = ByteBuffer.wrap(this.FREE);
				try {
					_bdb = (FileChannel) Files.newByteChannel(bdbf, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
							StandardOpenOption.READ, StandardOpenOption.SPARSE);
					_bdb.position(ls);
					long _pos = ls;
					while (_bdb.position() < es) {
						_pos = _bdb.position();
						if (Main.refCount) {
							byte[] val = new byte[arrayLength];
							ByteBuffer _bz = ByteBuffer.wrap(val);
							_bdb.read(_bz);
							if (!Arrays.equals(val, FREE)) {
								SparseDataChunk ck = new SparseDataChunk(val, this.version);
								for (HashLocPair p : ck.getFingers().values()) {
									DedupFileStore.removeRef(p.hash, Longs.fromByteArray(p.hashloc), 1, lookupFilter);
								}
							}
						}
						buff.position(0);
						_bdb.position(_pos);
						_bdb.write(buff);
					}
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("trimed from " + ls + " to " + _bdb.position());
				}

				catch (Exception e) {
					SDFSLogger.getLog().error("error while trim from " + ls + " to " + es, e);
				} finally {
					try {
						_bdb.close();
					} catch (Exception e) {
					}
				}

			}
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#truncate(long)
	 */
	@Override
	public void truncate(long length) throws IOException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		long fpos = 0;
		FileChannel _bdb = null;
		try {
			fpos = this.getMapFilePosition(length);
			if (Main.refCount) {
				this.iterInit();
				LongKeyValue kv = this.nextKeyValue(false);
				while (kv != null && kv.getKey() < fpos) {
					SparseDataChunk ck = kv.getValue();
					for (HashLocPair p : ck.getFingers().values()) {
						DedupFileStore.removeRef(p.hash, Longs.fromByteArray(p.hashloc), 1, lookupFilter);
					}
					kv = this.nextKeyValue(false);
				}
			}
			_bdb = (FileChannel) Files.newByteChannel(bdbf, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
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
			l.unlock();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#remove(long)
	 */
	@Override
	public void remove(long pos) throws IOException, FileClosedException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		FileChannel _bdb = null;
		try {
			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}

			long fpos = 0;

			fpos = this.getMapFilePosition(pos);
			_bdb = (FileChannel) Files.newByteChannel(bdbf, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ, StandardOpenOption.SPARSE);
			_bdb.write(ByteBuffer.wrap(FREE), fpos);
		} catch (FileClosedException e) {
			throw e;

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				_bdb.close();
			} catch (Exception e) {
			}
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#get(long)
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#get(long)
	 */
	@Override
	public SparseDataChunk get(long pos) throws IOException, FileClosedException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		long fpos = 0;
		try {
			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}

			fpos = this.getMapFilePosition(pos);

			if (fpos > this.dbFile.length())
				return null;
			byte[] buf = null;
			if (version > 1) {
				ByteBuffer bf = ByteBuffer.allocate(5);
				pbdb.read(bf, fpos);
				bf.position(1);
				buf = new byte[bf.getInt()];
				if (buf.length == 0)
					return null;
			} else
				buf = new byte[arrayLength];
			pbdb.read(ByteBuffer.wrap(buf), fpos);
			if (Arrays.equals(buf, FREE))
				return null;
			return new SparseDataChunk(buf, this.version);
		} catch (FileClosedException e) {
			throw e;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting data at " + fpos + " buffer capacity=" + dbFile.length(), e);
			throw new IOException(e);
		} finally {
			l.unlock();

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#sync()
	 */
	@Override
	public void sync() throws IOException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			this.pbdb.force(false);
		} finally {
			l.unlock();
		}

		/*
		 * FileChannel _bdb = null; try { _bdb = (FileChannel)
		 * bdbf.newByteChannel(StandardOpenOption.WRITE, StandardOpenOption.READ,
		 * StandardOpenOption.SPARSE); _bdb.force(true); } catch (IOException e) {
		 * 
		 * } finally { try { _bdb.close(); } catch (Exception e) { } }
		 */
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#vanish()
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#vanish()
	 */
	@Override
	public void vanish(boolean index) throws IOException {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		try {
			AtomicLong rmct = new AtomicLong();
			if (index) {
				this.iterInit();
				SparseDataChunk ck = this.nextValue(false);
				while (ck != null) {
					for (HashLocPair p : ck.getFingers().values()) {
						boolean rm = DedupFileStore.removeRef(p.hash, Longs.fromByteArray(p.hashloc), 1, lookupFilter);
						if (!rm) {
							rmct.incrementAndGet();
						}
					}
					ck = this.nextValue(false);
				}
			}
			if (!this.isClosed())
				this.forceClose();
			File f = new File(this.filePath);
			f.delete();
			File cf = new File(this.filePath + ".lz4");
			cf.delete();
			f.getParentFile().delete();
			if (rmct.get() > 0) {
				SDFSLogger.getLog().warn("unable to remove orphaned reference total=" + rmct.get());
			}

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			l.unlock();
		}
	}

	public void index() throws IOException, FileClosedException {
		this.iterInit();
		SparseDataChunk ck = this.nextValue(true);
		while (ck != null) {
			ck = this.nextValue(true);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#copy(java.lang.String)
	 */
	@Override
	public void copy(String destFilePath, boolean index) throws IOException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		FileChannel srcC = null;
		FileChannel dstC = null;
		try {
			this.sync();
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("copying to " + destFilePath);
			File dest = new File(destFilePath);
			File src = new File(this.filePath);
			if (dest.exists())
				dest.delete();
			else
				dest.getParentFile().mkdirs();
			if (OSValidator.isWindows()) {
				srcC = (FileChannel) Files.newByteChannel(Paths.get(src.getPath()), StandardOpenOption.READ,
						StandardOpenOption.SPARSE);
				dstC = (FileChannel) Files.newByteChannel(Paths.get(src.getPath()), StandardOpenOption.CREATE,
						StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
				srcC.transferTo(0, src.length(), dstC);
			} else {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("snapping on unix/linux volume");
				String cpCmd = "cp --sparse=always --reflink=auto " + src.getPath() + " " + dest.getPath();
				SDFSLogger.getLog().debug(cpCmd);
				Process p = Runtime.getRuntime().exec(cpCmd);
				int exitValue = p.waitFor();
				if (exitValue != 0) {
					throw new IOException("unable to copy " + src.getPath() + " to  " + dest.getPath()
							+ " exit value was " + exitValue);
				}
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("copy exit value is " + p.waitFor());
			}
			if (index) {
				LongByteArrayMap m = new LongByteArrayMap(dest.getPath(), this.lookupFilter);
				m.index();
				m.close();
			} else if (Main.COMPRESS_METADATA) {
				CompressionUtils.compressFile(dest, new File(dest.getPath() + ".lz4"));
			}
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("snapped map to [" + dest.getPath() + "]");
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
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#size()
	 */
	@Override
	public long size() {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			long sz = (this.dbFile.length() - this.offset) / this.arrayLength;
			return sz;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#close()
	 */
	@Override
	public void close() throws IOException {

		WriteLock l = this.hashlock.writeLock();
		l.lock();

		try {
			if (!this.closed) {
				int op = this.opens.decrementAndGet();
				SDFSLogger.getLog().info("Opens for " + this.filePath + " = " + op );
				if (op <= 0) {
					SDFSLogger.getLog().debug("closing " + this.filePath);
					this.opens.set(0);

					dbFile = null;
					this.closed = true;
					try {
						pbdb.force(true);
						pbdb.close();
					} catch (Exception e) {
					}
					try {
						this.rf.close();
					} catch (Exception e) {
					}
					if (Main.COMPRESS_METADATA) {
						File df = new File(this.filePath);
						File cf = new File(this.filePath + ".lz4");
						if (!df.exists()) {
							throw new IOException(df.getPath() + " does not exist");
						} else if (cf.exists() && df.exists()) {
							throw new IOException("both " + df.getPath() + " exists and " + cf.getPath());
						} else {
							CompressionUtils.compressFile(df, cf);
							df.delete();
						}
					}
					mp.remove(this.filePath);
					openFiles.decrementAndGet();
				}
				else {
					SDFSLogger.getLog().debug("not closing " + this.filePath + " opens=" + this.opens.get());
				}
			}
		} finally {
			l.unlock();
		}
	}
	
	public void forceClose() throws IOException {

		WriteLock l = this.hashlock.writeLock();
		l.lock();

		try {
			this.opens.set(0);
			this.close();
		} finally {
			l.unlock();
		}
	}

	@Override
	public void put(long pos, SparseDataChunk data, int length) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * public static DataMapInterface convertToV1(LongByteArrayMap map,SDFSEvent
	 * evt) throws IOException { LongByteArrayMap m = new
	 * LongByteArrayMap(map.filePath +"-new",(byte)1); File of = map.dbFile; File nf
	 * = new File(map.filePath +"-new"); map.hashlock.lock(); try { map.iterInit();
	 * evt.maxCt = map.size(); byte [] val = map.nextValue(); while(val != null) {
	 * evt.curCt++; SparseDataChunk ck = new SparseDataChunk(val); SparseDataChunk
	 * _ck = new SparseDataChunk(ck.getDoop(), ck.getHash(), ck.isLocalData(),
	 * ck.getHashLoc(),m.version); long fpose = (map.getIterPos() /map.arrayLength)*
	 * Main.CHUNK_LENGTH; m.put(fpose, _ck.getBytes()); val = map.nextValue();
	 * 
	 * } m.close(); map.close(); of.delete(); Files.move(nf.toPath(), of.toPath());
	 * m = new LongByteArrayMap(of.getPath(),(byte)1);
	 * evt.endEvent("Complete migration."); } catch(IOException e) {
	 * evt.endEvent("Unable to complete migration because : " +e.getMessage(),
	 * SDFSEvent.ERROR); throw e; } finally { try { m.close(); }catch(Exception e) {
	 * SDFSLogger.getLog().debug("unable to close map file", e); }
	 * 
	 * nf.delete(); map.hashlock.unlock(); } return m; }
	 */
}
