package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;

/**
 * 
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 * 
 */
public class MAzureChunkStore implements AbstractChunkStore {
	CloudStorageAccount account;
	CloudBlobClient serviceClient;
	CloudBlobContainer container;
	private String name;
	private boolean closed = false;
	boolean compress = false;
	boolean encrypt = false;
	private long currentLength = 0L;
	private static final int pageSize = Main.chunkStorePageSize;

	
	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey,
			String awsSecretKey, String bucketName) {
		return false;
	}

	public MAzureChunkStore() {

	}

	@Override
	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() {

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		String hashString = this.getHashName(hash);
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(hashString);
			if (!blob.exists())
				throw new IOException("blob does not exist " + hashString);
			else {
				ByteArrayOutputStream out = new ByteArrayOutputStream(
						(int) blob.getProperties().getLength());
				blob.download(out);
				byte[] data = out.toByteArray();
				blob.downloadAttributes();
				HashMap<String, String> metaData = blob.getMetadata();
				
				if (metaData.containsKey("encrypt") 
						&& metaData.get("encrypt").equalsIgnoreCase("true")) {
					data = EncryptUtils.decrypt(data);
				}
				if (metaData.containsKey("compress")
						&& metaData.get("compress").equalsIgnoreCase("true")) {
					data = CompressionUtils.decompressZLIB(data);
					
				}
				return data;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDFSLogger.getLog()
					.error("unable to fetch block [" + hash + "]", e);
			throw new IOException(e);
		}

	}

	@Override
	public String getName() {
		return this.name;
	}

	private static ReentrantLock reservePositionlock = new ReentrantLock();

	@Override
	public long reserveWritePosition(int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		reservePositionlock.lock();
		long pos = this.currentLength;
		this.currentLength = this.currentLength + pageSize;
		reservePositionlock.unlock();
		return pos;

	}

	@Override
	public void setName(String name) {

	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return this.currentLength;
	}

	@Override
	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		try {
			String hashString = this.getHashName(hash);
			CloudBlockBlob blob = container.getBlockBlobReference(hashString);
			if (!blob.exists()) {
				HashMap<String, String> metaData = new HashMap<String, String>();
				
				if (Main.cloudCompress) {
					chunk = CompressionUtils.compressZLIB(chunk);
					metaData.put("compress", "true");
				} else {
					metaData.put("compress", "false");
				}
				if (Main.chunkStoreEncryptionEnabled) {
					chunk = EncryptUtils.encrypt(chunk);
					metaData.put("encrypt", "true");
				} else {
					metaData.put("encrypt", "false");
				}
				blob.setMetadata(metaData);
				ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
				blob.upload(s3IS, chunk.length);
				blob.uploadMetadata();

			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash);
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(hashString);
			if (blob.exists())
				blob.delete();
		} catch (Exception e) {
			SDFSLogger.getLog()
					.warn("Unable to delete object " + hashString, e);
		} finally {
		}
	}

	public void deleteBucket() throws StorageException {
		container.deleteIfExists();
		this.close();
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
		try{
		String storageConnectionString = "DefaultEndpointsProtocol=http;"
				+ "AccountName=" + Main.cloudAccessKey + ";" + "AccountKey="
				+ Main.cloudSecretKey;
		account = CloudStorageAccount.parse(storageConnectionString);
		serviceClient = account.createCloudBlobClient();
		this.name = Main.cloudBucket;
		container = serviceClient.getContainerReference("gettingstarted");
		container.createIfNotExist();
		this.compress = Main.cloudCompress;
		this.encrypt = Main.chunkStoreEncryptionEnabled;
		}catch(Exception e) {
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

		MAzureChunkStore store = new MAzureChunkStore();
		store.init();
		String testTxt = "this is a test";
		byte[] hash = HashFunctionPool.getHashEngine().getHash(testTxt.getBytes());
		store.deleteChunk(hash, 0, 0);
		store.writeChunk(hash, testTxt.getBytes(), 0, 0);
		System.out.println(new String(store.getChunk(hash, 0, 0)));
	}

}
