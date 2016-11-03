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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.io.FileClosedException;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.util.OSValidator;

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
	public long iterPos = 0;
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
	long flen = 0;

	static {
		SDFSLogger.getLog().info("File Map Version is = " + Main.MAPVERSION);
		_FREE = new byte[_arrayLength];
		_V1FREE = new byte[_v1arrayLength];
		_V2FREE = new byte[_v2arrayLength];
		Arrays.fill(_FREE, (byte) 0);
		Arrays.fill(_V1FREE, (byte) 0);
		Arrays.fill(_V2FREE, (byte) 0);
	}

	public LongByteArrayMap(String filePath) throws IOException {
		this.filePath = filePath;
		this.openFile();

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
			this.iterPos = 0;

		} finally {
			l.unlock();
		}
	}

	private long getInternalIterFPos() {
		return (this.iterPos * arrayLength) + this.offset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#getIterPos()
	 */
	@Override
	public long getIterPos() {
		return (this.iterPos * arrayLength);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.DataMap#nextKey()
	 */
	@Override
	public long nextKey() throws IOException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			long _cpos = getInternalIterFPos();
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
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("unable to iterate through key at " + iterPos * arrayLength, e1);
				} finally {
					iterPos++;
					_cpos = getInternalIterFPos();
				}
			}
			if ((iterPos * arrayLength) + this.offset != flen)
				throw new IOException("did not reach end of file for [" + this.filePath + "] len="
						+ iterPos * arrayLength + " file len =" + flen);

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
	public SparseDataChunk nextValue(boolean index) throws IOException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {

			for (;;) {
				try {
					if (nbuf == null || !nbuf.hasRemaining()) {
						long _cpos = getInternalIterFPos();
						int al = arrayLength * NP;
						if (_cpos >= flen)
							return null;
						nbuf = ByteBuffer.allocate(al);
						pbdb.read(nbuf, _cpos);
						// SDFSLogger.getLog().info("al=" + al + " cpos=" +_cpos
						// + " flen=" + flen + " fz=" +pbdb.size() + " nbfs=" +
						// nbuf.position());
						nbuf.position(0);
					}
					// SDFSLogger.getLog().info("arl=" + arrayLength + "
					// nbufsz=" + nbuf.capacity() + " rem="+ nbuf.remaining());
					byte[] val = new byte[arrayLength];
					nbuf.get(val);
					if (!Arrays.equals(val, FREE)) {
						SparseDataChunk ck = new SparseDataChunk(val, this.version);
						if (index) {
							for (HashLocPair p : ck.getFingers()) {
								DedupFileStore.cp.put(p.hash);
							}
						}
						return ck;
					}
				} finally {
					iterPos++;
				}
			}

		} finally {
			l.unlock();
		}

	}

	public LongKeyValue nextKeyValue(boolean index) throws IOException {
		ReadLock l = this.hashlock.readLock();
		l.lock();
		try {
			long _cpos = getInternalIterFPos();
			while (_cpos < flen) {
				try {
					ByteBuffer buf = ByteBuffer.wrap(new byte[arrayLength]);
					pbdb.read(buf, _cpos);
					byte[] val = buf.array();
					if (!Arrays.equals(val, FREE)) {
						SparseDataChunk ck = new SparseDataChunk(val, this.version);
						if (index) {
							for (HashLocPair p : ck.getFingers()) {
								DedupFileStore.cp.put(p.hash);
							}
						}
						return new LongKeyValue(iterPos * Main.CHUNK_LENGTH, ck);
					}
				} finally {
					iterPos++;
					_cpos = (iterPos * arrayLength) + this.offset;
				}
			}
			if (getInternalIterFPos() < pbdb.size()) {

				flen = this.pbdb.size();

				return this.nextKeyValue(index);
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
		if (this.closed) {
			WriteLock l = this.hashlock.writeLock();
			l.lock();
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
					flen = 0;
				} else {

					flen = dbFile.length();
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
			} finally {
				l.unlock();
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
	 * @see org.opendedup.collections.DataMap#put(long, byte[])
	 */
	@Override
	public void put(long pos, SparseDataChunk data) throws IOException, FileClosedException {
		long fpos = 0;
		fpos = this.getMapFilePosition(pos);

		//
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {

			if (this.isClosed()) {
				throw new FileClosedException("hashtable [" + this.filePath + "] is close");
			}
			/*
			 * if (data.length != arrayLength) throw new
			 * IOException("data length " + data.length + " does not equal " +
			 * arrayLength);
			 */
			;
			if (fpos > flen)
				flen = fpos;
			l.unlock();
			l = this.hashlock.readLock();
			l.lock();
			if (Main.refCount) {
				SparseDataChunk ck = this.get(pos);
				if (ck != null) {
					for (HashLocPair p : ck.getFingers()) {
						DedupFileStore.cp.remove(p.hash);
					}
				}
				for (HashLocPair p : data.getFingers()) {
					DedupFileStore.cp.remove(p.hash);
				}
			}
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
						if(Main.refCount) {
						byte[] val = new byte[arrayLength];
						ByteBuffer _bz = ByteBuffer.wrap(val);
						_bdb.read(_bz);
						if (!Arrays.equals(val, FREE)) {
							SparseDataChunk ck = new SparseDataChunk(val, this.version);
								for (HashLocPair p : ck.getFingers()) {
									DedupFileStore.cp.remove(p.hash);
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
			if(Main.refCount) {
				this.iterInit();
				LongKeyValue kv = this.nextKeyValue(false);
				while(kv != null && kv.getKey() < fpos) {
					SparseDataChunk ck = kv.getValue();
					for(HashLocPair p : ck.getFingers()) {
						DedupFileStore.cp.remove(p.hash);
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
			this.flen = fpos;
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

			if (fpos > flen)
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
			if (index) {
				this.iterInit();
				SparseDataChunk ck = this.nextValue(false);
				while (ck != null) {
					for (HashLocPair p : ck.getFingers()) {
						DedupFileStore.cp.remove(p.hash);
					}
					ck = this.nextValue(false);
				}
			}
			if (!this.isClosed())
				this.close();
			File f = new File(this.filePath);
			f.delete();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			l.unlock();
		}
	}

	public void index() throws IOException {
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
				LongByteArrayMap m = new LongByteArrayMap(dest.getPath());
				m.index();
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
	public void close() {
		WriteLock l = this.hashlock.writeLock();
		l.lock();
		dbFile = null;
		if (!this.isClosed()) {
			this.closed = true;
		}
		try {
			pbdb.force(true);
			pbdb.close();
		} catch (Exception e) {
		}
		try {
			this.rf.close();
		} catch (Exception e) {
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
	 * LongByteArrayMap(map.filePath +"-new",(byte)1); File of = map.dbFile;
	 * File nf = new File(map.filePath +"-new"); map.hashlock.lock(); try {
	 * map.iterInit(); evt.maxCt = map.size(); byte [] val = map.nextValue();
	 * while(val != null) { evt.curCt++; SparseDataChunk ck = new
	 * SparseDataChunk(val); SparseDataChunk _ck = new
	 * SparseDataChunk(ck.getDoop(), ck.getHash(), ck.isLocalData(),
	 * ck.getHashLoc(),m.version); long fpose = (map.getIterPos()
	 * /map.arrayLength)* Main.CHUNK_LENGTH; m.put(fpose, _ck.getBytes()); val =
	 * map.nextValue();
	 * 
	 * } m.close(); map.close(); of.delete(); Files.move(nf.toPath(),
	 * of.toPath()); m = new LongByteArrayMap(of.getPath(),(byte)1);
	 * evt.endEvent("Complete migration."); } catch(IOException e) {
	 * evt.endEvent("Unable to complete migration because : " +e.getMessage(),
	 * SDFSEvent.ERROR); throw e; } finally { try { m.close(); }catch(Exception
	 * e) { SDFSLogger.getLog().debug("unable to close map file", e); }
	 * 
	 * nf.delete(); map.hashlock.unlock(); } return m; }
	 */
}
