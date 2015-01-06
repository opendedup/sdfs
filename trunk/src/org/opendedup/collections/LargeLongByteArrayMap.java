package org.opendedup.collections;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.OSValidator;

public class LargeLongByteArrayMap implements AbstractMap {

	private long fileSize = 0;
	private int arraySize = 0;
	// RandomAccessFile bdbf = null;
	String fileName;
	boolean closed = true;
	boolean dirty = false;
	private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();

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
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(fileName, "rw");

			if (this.fileSize > 0)
				bdbf.setLength(this.fileSize);

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (bdbf != null)
				bdbf.close();
			bdbf = null;
		}
	}

	public void setLength(long length) throws IOException {
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(fileName, "rw");
			bdbf.setLength(length);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (bdbf != null)
				bdbf.close();
			bdbf = null;
		}
	}

	public void lockCollection() {
		this.hashlock.writeLock().lock();
	}

	public void unlockCollection() {
		this.hashlock.writeLock().unlock();
	}

	@Override
	public void close() {
		this.hashlock.writeLock().lock();
		try {
			this.closed = true;
			if (this.dirty) {
				RandomAccessFile bdbf = null;
				try {
					bdbf = new RandomAccessFile(fileName, "rw");
					bdbf.getFD().sync();
				} catch (Exception e) {
				} finally {
					try {
						bdbf.close();
					} catch (IOException e) {

					}
					this.dirty = false;
				}
			}

		} finally {
			this.hashlock.writeLock().unlock();
		}
	}

	public byte[] get(long pos) throws IOException {
		return this.get(pos, true);
	}

	public byte[] get(long pos, boolean checkForLock) throws IOException {
		if (checkForLock) {
			this.hashlock.readLock().lock();
		}
		byte[] b = new byte[this.arraySize];
		RandomAccessFile rf = null;
		try {
			rf = new RandomAccessFile(this.fileName, "rw");
			rf.seek(pos);
			rf.read(b);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			rf.close();
			rf = null;
			if (checkForLock) {
				this.hashlock.readLock().unlock();
			}
		}
		return b;
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	public void put(long pos, byte[] data) throws IOException {
		this.hashlock.writeLock().lock();
		try {
			this.dirty = true;
			if (data.length != this.arraySize)
				throw new IOException(" size mismatch " + data.length
						+ " does not equal " + this.arraySize);

			RandomAccessFile rf = null;
			try {
				rf = new RandomAccessFile(this.fileName, "rw");
				rf.seek(pos);
				rf.write(data);
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				rf.close();
				rf = null;
			}
		} finally {
			this.hashlock.writeLock().unlock();

		}
	}

	public void remove(long pos) throws IOException {
		this.remove(pos, true);
	}

	public void remove(long pos, boolean checkForLock) throws IOException {
		if (checkForLock) {
			this.hashlock.writeLock().lock();
		}
		this.dirty = true;
		RandomAccessFile rf = null;
		try {
			rf = new RandomAccessFile(this.fileName, "rw");
			rf.seek(pos);
			rf.write(new byte[this.arraySize]);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try{
			rf.close();
			rf = null;
			}catch(Exception e) {}
			if (checkForLock) {
				this.hashlock.writeLock().lock();
			}
			
		}
	}

	@Override
	public void sync() throws IOException {
		this.hashlock.writeLock().lock();
		try {
			if (this.dirty) {
				RandomAccessFile bdbf = new RandomAccessFile(fileName, "rw");
				bdbf.getFD().sync();
				bdbf.close();
				bdbf = null;
				this.dirty = false;
			}
		} finally {
			this.hashlock.writeLock().unlock();
		}

	}

	@Override
	public void vanish() throws IOException {
		this.close();
		RandomAccessFile bdbf = null;
		try {
			bdbf = new RandomAccessFile(fileName, "rw");
			bdbf.setLength(0);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			bdbf.close();
			bdbf = null;
		}
	}

	public void copy(String destFilePath) throws IOException {
		this.hashlock.writeLock().lock();
		FileChannel srcC = null;
		FileChannel dstC = null;
		try {

			this.sync();
			File dest = new File(destFilePath);
			File src = new File(this.fileName);
			if (dest.exists())
				dest.delete();
			else
				dest.getParentFile().mkdirs();
			if (OSValidator.isWindows()) {
				srcC = (FileChannel) Files.newByteChannel(
						Paths.get(src.getPath()), StandardOpenOption.READ,
						StandardOpenOption.SPARSE);
				dstC = (FileChannel) Files.newByteChannel(
						Paths.get(dest.getPath()), StandardOpenOption.CREATE,
						StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
				srcC.transferTo(0, src.length(), dstC);
			} else {
				Process p = Runtime.getRuntime().exec(
						"cp --sparse=always " + src.getPath() + " "
								+ dest.getPath());
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"copy exit value is " + p.waitFor());
			}

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
			this.hashlock.writeLock().unlock();
		}
	}

	public void move(String destFilePath) throws IOException {
		this.hashlock.writeLock().lock();
		try {
			this.sync();
			File f = new File(destFilePath);
			if (f.exists())
				f.delete();
			else
				f.getParentFile().mkdirs();
			Path p = Paths.get(this.fileName);
			Path dest = Paths.get(destFilePath);
			Files.move(p, dest);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.hashlock.writeLock().unlock();
		}
	}

	public void optimize() throws IOException {
		this.hashlock.writeLock().lock();
		this.dirty = true;
		RandomAccessFile rf = null;
		RandomAccessFile nrf = null;
		try {
			SDFSLogger.getLog().info("optimizing file [" + this.fileName + "]");
			File f = new File(this.fileName + ".new");
			f.delete();
			rf = new RandomAccessFile(this.fileName, "rw");
			nrf = new RandomAccessFile(this.fileName + ".new", "rw");

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
			SDFSLogger.getLog().info(
					"optimizing file [" + this.fileName + "] migrated ["
							+ mData + "] bytes of data to new file");
		} catch (IOException e) {
			throw e;
		} finally {
			if (rf != null) {
				rf.close();
				rf = null;
			}
			if (nrf != null) {
				nrf.close();
				nrf = null;
			}
			this.hashlock.writeLock().unlock();
		}
	}

}
