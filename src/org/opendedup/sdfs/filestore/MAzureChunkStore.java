package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
	boolean compress = false;
	boolean encrypt = false;
	private long currentLength = 0L;
	private int cacheSize = 104857600 / Main.CHUNK_LENGTH;

	LoadingCache<String, byte[]> chunks = CacheBuilder.newBuilder()
			.maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<String, byte[]>() {
				public byte[] load(String hashString) throws IOException {
					SDFSLogger.getLog().debug("getting hash " + hashString);
					try {
						CloudBlockBlob blob = container
								.getBlockBlobReference(hashString);
						if (!blob.exists())
							throw new IOException("blob does not exist "
									+ hashString);
						else {
							ByteArrayOutputStream out = new ByteArrayOutputStream(
									(int) blob.getProperties().getLength());
							blob.download(out);
							byte[] data = out.toByteArray();
							blob.downloadAttributes();
							HashMap<String, String> metaData = blob
									.getMetadata();

							if (metaData.containsKey("encrypt")
									&& metaData.get("encrypt")
											.equalsIgnoreCase("true")) {
								data = EncryptUtils.decrypt(data);
							}
							if (metaData.containsKey("compress")
									&& metaData.get("compress")
											.equalsIgnoreCase("true")) {
								data = CompressionUtils.decompressZLIB(data);

							}
							if (metaData.containsKey("scompress")
									&& metaData.get("scompress")
											.equalsIgnoreCase("true")) {
								data = CompressionUtils.decompressSnappy(data);
							}
							return data;
						}
					} catch (Exception e) {
						SDFSLogger.getLog()
								.error("unable to fetch block [" + hashString
										+ "]", e);
						throw new IOException(e);
					}
				}
			});

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
		return 0;
	}

	@Override
	public long bytesWritten() {
		return 0;
	}

	@Override
	public void close() {

	}

	public void expandFile(long length) throws IOException {

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		try {

			String hashString = this.getHashName(hash);
			SDFSLogger.getLog().debug("getting hash " + hashString);
			byte[] _bz = this.chunks.get(hashString);
			byte[] bz = Arrays.clone(_bz);
			return bz;
		} catch (ExecutionException e) {
			SDFSLogger.getLog().error("Unable to get block at " + start, e);
			throw new IOException(e);
		}

	}

	@Override
	public String getName() {
		return this.name;
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
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		String hashString = this.getHashName(hash);
		SDFSLogger.getLog().debug("writing data for " + hashString);
		try {

			CloudBlockBlob blob = container.getBlockBlobReference(hashString);
			if (!blob.exists()) {
				HashMap<String, String> metaData = new HashMap<String, String>();

				if (Main.cloudCompress) {
					chunk = CompressionUtils.compressSnappy(chunk);
					metaData.put("scompress", "true");
				} else {
					metaData.put("scompress", "false");
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
			return 0;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write hash " + hashString, e);
			throw new IOException(e);
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash);
		try {
			this.chunks.invalidate(hashString);
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
	public void init(Element config) throws IOException {
		init();

	}

	public void init() throws IOException {
		try {
			String storageConnectionString = "DefaultEndpointsProtocol=http;"
					+ "AccountName=" + Main.cloudAccessKey + ";"
					+ "AccountKey=" + Main.cloudSecretKey;
			account = CloudStorageAccount.parse(storageConnectionString);
			serviceClient = account.createCloudBlobClient();
			this.name = Main.cloudBucket;
			container = serviceClient.getContainerReference(this.name);
			container.createIfNotExist();
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
	public void iterationInit() {

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
		byte[] hash = HashFunctionPool.getHashEngine().getHash(
				testTxt.getBytes());
		store.deleteChunk(hash, 0, 0);
		store.writeChunk(hash, testTxt.getBytes(), 0);
		System.out.println(new String(store.getChunk(hash, 0, 0)));
	}

	@Override
	public long getFreeBlocks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
