package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

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
import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
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
public class CopyOfMAzureChunkStore implements AbstractChunkStore {
	CloudStorageAccount account;
	CloudBlobClient serviceClient;
	CloudBlobContainer container;
	private String name;
	boolean compress = false;
	boolean encrypt = false;
	private AtomicLong currentLength = new AtomicLong(0);
	private AtomicLong compressedLength = new AtomicLong(0);
	private int cacheSize = 104857600 / Main.CHUNK_LENGTH;

	LoadingCache<String, byte[]> chunks = CacheBuilder.newBuilder()
			.maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<String, byte[]>() {
				public byte[] load(String hashString) throws IOException {
					SDFSLogger.getLog().debug("getting hash " + hashString);
					try {
						CloudBlockBlob blob = container
								.getBlockBlobReference(hashString);

							ByteArrayOutputStream out = new ByteArrayOutputStream(
									(int) blob.getProperties().getLength());
							blob.download(out);
							byte[] data = out.toByteArray();
							blob.downloadAttributes();
							HashMap<String, String> metaData = blob
									.getMetadata();
							int size = 0;
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
							if (metaData.containsKey("lz4Compress")
									&& metaData.get("lz4Compress")
											.equalsIgnoreCase("true")) {
								size = Integer.parseInt(metaData.get("size"));
								data = CompressionUtils.decompressLz4(data,
										size);
							}

							return data;
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

	public CopyOfMAzureChunkStore() {

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
		try {
			HashMap<String, String> md = container.getMetadata();
			md.put("currentlength", Long.toString(this.currentLength.get()));
			md.put("compressedlength",
					Long.toString(this.compressedLength.get()));
			container.setMetadata(md);
			container.uploadMetadata();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error closing container", e);
		}
	}

	public void expandFile(long length) throws IOException {

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		try {

			String hashString = this.getHashName(hash,
					Main.chunkStoreEncryptionEnabled);
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
		return this.currentLength.get();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		String hashString = this.getHashName(hash,
				Main.chunkStoreEncryptionEnabled);
		int cl = chunk.length;
		try {

				CloudBlockBlob blob = container.getBlockBlobReference(hashString);
				HashMap<String, String> metaData = new HashMap<String, String>();

				if (Main.compress) {
					chunk = CompressionUtils.compressLz4(chunk);
					metaData.put("lz4Compress", "true");
				} else {
					metaData.put("lz4Compress", "false");
				}
				if (Main.chunkStoreEncryptionEnabled) {
					chunk = EncryptUtils.encrypt(chunk);
					metaData.put("encrypt", "true");
				} else {
					metaData.put("encrypt", "false");
				}
				metaData.put("size", Integer.toString(cl));
				metaData.put("compressedsize", Integer.toString(chunk.length));
				blob.setMetadata(metaData);
				ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
				blob.upload(s3IS, chunk.length);
				blob.uploadMetadata();
			return 0;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write hash " + hashString, e);
			throw new IOException(e);
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash,
				Main.chunkStoreEncryptionEnabled);
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

	private String getHashName(byte[] hash, boolean enc) throws IOException {
		if (enc) {
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
			HashMap<String, String> md = container.getMetadata();
			long sz = 0;
			long cl = 0;
			if (md.containsKey("currentlength")) {
				sz = Long.parseLong(md.get("currentlength"));
				if (sz < 0)
					sz = 0;
			}
			if (md.containsKey("compressedlength")) {
				cl = Long.parseLong(md.get("compressedlength"));
				if (cl < 0)
					cl = 0;
			}
			this.currentLength.set(sz);
			this.compressedLength.set(cl);
			md.put("currentlength", "-1");
			md.put("compressedlength", "-1");
			container.setMetadata(md);
			container.uploadMetadata();
			this.compress = Main.compress;
			this.encrypt = Main.chunkStoreEncryptionEnabled;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	Iterator<ListBlobItem> iter = null;

	@Override
	public void iterationInit() {
		iter = container.listBlobs().iterator();
	}

	public static void main(String[] args) throws IOException,
			NoSuchAlgorithmException, NoSuchProviderException {
		Main.cloudAccessKey = args[0];
		Main.cloudSecretKey = args[1];
		Main.cloudBucket = args[2];
		Main.compress = true;
		Main.chunkStoreEncryptionEnabled = true;
		Main.chunkStoreEncryptionKey = PassPhrase.getNext();

		CopyOfMAzureChunkStore store = new CopyOfMAzureChunkStore();
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

	private byte[] getHashBytes(String hashStr, boolean enc) {
		if (enc) {
			byte[] encH = StringUtils.getHexBytes(hashStr);
			return EncryptUtils.decrypt(encH);
		} else {
			return StringUtils.getHexBytes(hashStr);
		}
	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		try {
			if (!iter.hasNext())
				return null;
			else {
				CloudBlob bi = (CloudBlob) iter.next();
				HashMap<String, String> md = bi.getMetadata();
				boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
				byte[] nm = this.getHashBytes(bi.getName(), encrypt);
				int sz = Integer.parseInt(md.get("size"));
				int cl = Integer.parseInt(md.get("compressedsize"));
				ChunkData chk = new ChunkData(nm, 0);
				chk.cLen = sz;
				this.currentLength.addAndGet(chk.cLen);
				this.compressedLength.addAndGet(cl);
				return chk;
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public long maxSize() {
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		return this.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

}
