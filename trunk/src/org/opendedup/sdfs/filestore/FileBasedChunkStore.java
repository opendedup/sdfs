package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.microsoft.windowsazure.services.core.storage.StorageException;

public class FileBasedChunkStore implements AbstractChunkStore {
	private String name;
	private boolean closed = false;
	boolean compress = false;
	boolean encrypt = false;
	private long currentLength = 0L;
	private static final int pageSize = Main.chunkStorePageSize;
	private static File chunk_location = new File(Main.chunkStore);

	public FileBasedChunkStore() {

	}

	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long bytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void close() {

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	private String getHashPath(byte[] hash) throws IOException {
		String hashString = this.getHashName(hash);
		String dir = chunk_location + File.separator
				+ hashString.substring(0, 2) + File.separator
				+ hashString.substring(2, 2) + File.separator + hashString;
		File f = new File(dir).getParentFile();
		if (!f.exists())
			f.mkdirs();
		return dir;
	}

	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		Path p = Paths.get(this.getHashPath(hash));
		File f = p.toFile();
		FileChannel fc = null;
		ByteBuffer buff = null;
		try {

			if (!f.exists())
				throw new IOException("blob does not exist " + f.toPath());
			else {
				buff = ByteBuffer.wrap(new byte[(int) f.length()]);
				fc = (FileChannel) Files.newByteChannel(p,
						StandardOpenOption.READ);
				fc.read(buff);
				byte[] data = buff.array();
				if (Main.chunkStoreEncryptionEnabled) {
					data = EncryptUtils.decrypt(data);
				}
				if (Main.cloudCompress) {
					data = CompressionUtils.decompressSnappy(data);
				}
				return data;
			}
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to fetch block [" + hash + "]", e);
			throw new IOException(e);
		} finally {
			fc.close();
			buff = null;
		}
	}

	public String getName() {
		return this.name;
	}

	private static ReentrantLock reservePositionlock = new ReentrantLock();

	public long reserveWritePosition(int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		reservePositionlock.lock();
		long pos = this.currentLength;
		this.currentLength = this.currentLength + pageSize;
		reservePositionlock.unlock();
		return pos;

	}

	public void setName(String name) {

	}

	public long size() {
		// TODO Auto-generated method stub
		return this.currentLength;
	}

	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		Path p = Paths.get(this.getHashPath(hash));
		FileChannel fc = null;
		ByteBuffer buff = null;
		try {

			fc = (FileChannel) Files.newByteChannel(p,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ);

			if (Main.cloudCompress)
				chunk = CompressionUtils.compressSnappy(chunk);
			if (Main.chunkStoreEncryptionEnabled)
				chunk = EncryptUtils.encrypt(chunk);
			buff = ByteBuffer.wrap(chunk);
			fc.write(buff);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			fc.close();
			buff = null;
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		Path p = Paths.get(this.getHashPath(hash));
		File f = p.toFile();
		try {
			if (f.exists())
				f.delete();
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"Unable to delete object " + p.getFileName(), e);
		} finally {
		}
	}

	public void deleteBucket() throws StorageException {
		//
	}

	private String getHashName(byte[] hash) throws IOException {
		if (Main.chunkStoreEncryptionEnabled) {
			byte[] encH = EncryptUtils.encrypt(hash);
			return StringUtils.getHexString(encH);
		} else {
			return StringUtils.getHexString(hash);
		}
	}

	@Override
	public void addChunkStoreListener(AbstractChunkStoreListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void claimChunk(byte[] hash, long start) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean moveChunk(byte[] hash, long origLoc, long newLoc)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void init(Element config) throws IOException {
		init();

	}

	public void init() throws IOException {
		try {

			this.compress = Main.cloudCompress;
			this.encrypt = Main.chunkStoreEncryptionEnabled;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void setSize(long size) {
		// TODO Auto-generated method stub

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void iterationInit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void compact() throws IOException {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) throws IOException,
			NoSuchAlgorithmException, NoSuchProviderException {
		Main.cloudAccessKey = args[0];
		Main.cloudSecretKey = args[1];
		Main.cloudBucket = args[2];
		Main.cloudCompress = true;
		Main.chunkStoreEncryptionEnabled = true;
		Main.chunkStoreEncryptionKey = PassPhrase.getNext();

		FileBasedChunkStore store = new FileBasedChunkStore();
		store.init();
		String testTxt = "this is a test";
		byte[] hash = HashFunctionPool.getHashEngine().getHash(
				testTxt.getBytes());
		store.deleteChunk(hash, 0, 0);
		store.writeChunk(hash, testTxt.getBytes(), 0, 0);
		System.out.println(new String(store.getChunk(hash, 0, 0)));
	}

}
