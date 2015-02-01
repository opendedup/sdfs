package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.bouncycastle.util.Arrays;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * 
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 * 
 */
public class S3ChunkStore implements AbstractChunkStore {
	private static AWSCredentials awsCredentials = null;
	private String name;
	private S3ServicePool pool = null;
	private AtomicLong currentLength = new AtomicLong(0);
	private AtomicLong compressedLength = new AtomicLong(0);
	private int cacheSize = 10485760 / Main.CHUNK_LENGTH;

	LoadingCache<String, byte[]> chunks = CacheBuilder.newBuilder()
			.maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<String, byte[]>() {
				public byte[] load(String hashString) throws IOException {

					RestS3Service s3Service = null;
					try {
						s3Service = pool.borrowObject();
						S3Object obj = s3Service.getObject(name, hashString);
						boolean encrypt = false;
						boolean compress = false;
						boolean lz4compress = false;
						int size = Integer.parseInt((String) obj
								.getMetadata("size"));
						int csz = size;
						if (obj.containsMetadata("encrypt")) {
							encrypt = Boolean.parseBoolean((String) obj
									.getMetadata("encrypt"));
						}
						if (obj.containsMetadata("compress")) {
							compress = Boolean.parseBoolean((String) obj
									.getMetadata("compress"));
						} else if (obj.containsMetadata("lz4compress")) {
							csz = Integer.parseInt((String) obj
									.getMetadata("compressedsize"));
							lz4compress = Boolean.parseBoolean((String) obj
									.getMetadata("lz4compress"));
						}
						int cl = (int) obj.getContentLength();
						if (cl != csz) {
							SDFSLogger.getLog().warn(
									"Possible data mismatch size=" + csz
											+ " does not equal content length"
											+ cl);
						}

						byte[] data = new byte[cl];
						DataInputStream in = new DataInputStream(obj
								.getDataInputStream());
						in.readFully(data);
						obj.closeDataInputStream();
						if (encrypt)
							data = EncryptUtils.decrypt(data);
						if (compress)
							data = CompressionUtils.decompressZLIB(data);
						else if (lz4compress) {
							data = CompressionUtils.decompressLz4(data, size);
						}
						return data;
					} catch (Exception e) {
						SDFSLogger.getLog()
								.error("unable to fetch block [" + hashString
										+ "]", e);
						throw new IOException("unable to read " + hashString);
					} finally {
						pool.returnObject(s3Service);
					}
				}
			});

	static {
		try {
			awsCredentials = new AWSCredentials(Main.cloudAccessKey,
					Main.cloudSecretKey);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		AWSCredentials creds = null;
		try {
			creds = new AWSCredentials(awsAccessKey, awsSecretKey);
			RestS3Service s3Service = new RestS3Service(creds);
			s3Service.listAllBuckets();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			return false;
		}
	}

	public static boolean checkBucketUnique(String awsAccessKey,
			String awsSecretKey, String bucketName) {
		AWSCredentials creds = null;
		try {
			creds = new AWSCredentials(awsAccessKey, awsSecretKey);
			RestS3Service s3Service = new RestS3Service(creds);
			S3Bucket s3Bucket = s3Service.getBucket(bucketName);
			if (s3Bucket == null) {
				return true;
				//s3Bucket = s3Service.createBucket(bucketName);
			}
			return false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to create aws bucket", e);
			return false;
		}
	}

	public S3ChunkStore() {

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
		RestS3Service s3Service = null;
		try {

			s3Service = pool.borrowObject();
			S3Object obj = s3Service.getObject(this.name, "bucketinfo");
			obj.removeMetadata("currentsize");
			obj.addMetadata("currentsize",
					Long.toString(this.currentLength.get()));
			obj.removeMetadata("currentcompressedsize");
			obj.addMetadata("currentcompressedsize",
					Long.toString(this.compressedLength.get()));
			s3Service.updateObjectMetadata(this.name, obj);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while closing bucket " + this.name,
					e);
		} finally {
			try {
				pool.returnObject(s3Service);
			} catch (IOException e) {
				SDFSLogger.getLog().warn(
						"error while closing bucket " + this.name, e);
			}
		}

		try {
			pool.close();
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while closing bucket " + this.name,
					e);
		}

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		try {
			String hashString = this.getHashName(hash,
					Main.chunkStoreEncryptionEnabled);
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
		S3Object s3Object = new S3Object(hashString);
		s3Object.addMetadata("size", Integer.toString(chunk.length));
		this.currentLength.addAndGet(chunk.length);
		if (Main.compress) {
			chunk = CompressionUtils.compressLz4(chunk);
			s3Object.addMetadata("lz4compress", "true");
		} else {
			s3Object.addMetadata("lz4compress", "false");
		}
		if (Main.chunkStoreEncryptionEnabled) {
			chunk = EncryptUtils.encrypt(chunk);
			s3Object.addMetadata("encrypt", "true");
		} else {
			s3Object.addMetadata("encrypt", "false");
		}
		s3Object.addMetadata("compressedsize", Integer.toString(chunk.length));
		this.compressedLength.addAndGet(chunk.length);
		ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
		s3Object.setDataInputStream(s3IS);
		s3Object.setContentType("binary/octet-stream");
		s3Object.setContentLength(s3IS.available());
		RestS3Service s3Service = null;
		try {
			s3Service = pool.borrowObject();
			s3Service.putObject(this.name, s3Object);
			return 0;
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to upload " + hashString, e);
			throw new IOException(e);
		} finally {
			pool.returnObject(s3Service);
			s3IS.close();

		}
	}

	public static void main(String[] args) throws IOException {

	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash,
				Main.chunkStoreEncryptionEnabled);
		RestS3Service s3Service = null;
		try {
			this.chunks.invalidate(hashString);
			s3Service = pool.borrowObject();
			StorageObject obj = s3Service.getObjectDetails(this.name,
					hashString);
			int size = Integer.parseInt((String) obj.getMetadata("size"));
			int compressedSize = Integer.parseInt((String) obj
					.getMetadata("compressedsize"));
			this.currentLength.addAndGet(-1 * size);
			this.compressedLength.addAndGet(-1 * compressedSize);
			s3Service.deleteObject(this.name, hashString);
		} catch (Exception e) {
			SDFSLogger.getLog()
					.warn("Unable to delete object " + hashString, e);
		} finally {
			pool.returnObject(s3Service);
		}
	}

	public static void deleteBucket(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out.println("");
			System.out.print("Deleting Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new AWSCredentials(awsAccessKey,
					awsSecretKey);
			S3Service bs3Service = new RestS3Service(bawsCredentials);
			S3Object[] obj = bs3Service.listObjects(bucketName);
			for (int i = 0; i < obj.length; i++) {
				bs3Service.deleteObject(bucketName, obj[i].getKey());
				System.out.print(".");
			}
			bs3Service.deleteBucket(bucketName);
			SDFSLogger.getLog().info("Bucket [" + bucketName + "] deleted");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (ServiceException e) {
			SDFSLogger.getLog()
					.warn("Unable to delete bucket " + bucketName, e);
		}
	}

	private String getHashName(byte[] hash, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encrypt(hash);
			return StringUtils.getHexString(encH).toLowerCase();
		} else {
			return StringUtils.getHexString(hash).toLowerCase();
		}
	}

	private byte[] getHashBytes(String hashStr, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = StringUtils.getHexBytes(hashStr);
			return EncryptUtils.decrypt(encH);
		} else {
			return StringUtils.getHexBytes(hashStr);
		}
	}

	@Override
	public void init(Element config) throws IOException {
		this.name = Main.cloudBucket;
		try {
			String bucketLocation = null;
			if (config.hasAttribute("default-bucket-location")) {
				bucketLocation = config.getAttribute("default-bucket-location");
			}
			pool = new S3ServicePool(S3ChunkStore.awsCredentials,
					Main.dseIOThreads);
			RestS3Service s3Service = pool.borrowObject();

			S3Bucket s3Bucket = s3Service.getBucket(this.name);

			if (s3Bucket == null) {
				if(bucketLocation == null)
				s3Bucket = s3Service.createBucket(this.name);
				else
					s3Bucket = s3Service.createBucket(this.name,bucketLocation);
				SDFSLogger.getLog().info("created new store " + name);
				S3Object s3Object = new S3Object("bucketinfo");
				s3Object.addMetadata("currentsize", "-1");
				s3Object.addMetadata("currentcompressedsize", "-1");
				s3Service.putObject(this.name, s3Object);
			} else {

				S3Object obj = null;
				try {
					obj = s3Service.getObject(this.name, "bucketinfo");
				} catch (Exception e) {
					SDFSLogger.getLog().info(
							"unable to find bucketinfo object", e);
				}
				if (obj == null) {
					S3Object s3Object = new S3Object("bucketinfo");
					s3Object.addMetadata("currentsize", "-1");
					s3Object.addMetadata("currentcompressedsize", "-1");
					s3Service.putObject(this.name, s3Object);
				} else {
					if (obj.containsMetadata("currentsize")) {
						long cl = Long.parseLong((String) obj
								.getMetadata("currentsize"));
						if (cl >= 0) {
							this.currentLength.set(cl);
							obj.removeMetadata("currentsize");
							obj.addMetadata("currentsize",
									Long.toString(-1 * cl));
						} else
							SDFSLogger.getLog().warn(
									"The S3 objectstore DSE did not close correctly len="
											+ cl);
					} else {
						SDFSLogger
								.getLog()
								.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}

					if (obj.containsMetadata("currentcompressedsize")) {
						long cl = Long.parseLong((String) obj
								.getMetadata("currentcompressedsize"));
						if (cl >= 0) {
							this.compressedLength.set(cl);
							obj.removeMetadata("currentcompressedsize");
							obj.addMetadata("currentcompressedsize",
									Long.toString(-1 * cl));
						} else
							SDFSLogger.getLog().warn(
									"The S3 objectstore DSE did not close correctly clen="
											+ cl);
					} else {
						SDFSLogger
								.getLog()
								.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}
					s3Service.updateObjectMetadata(this.name, obj);
				}
			}
			pool.returnObject(s3Service);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		try {
			if (objPos >= obj.length) {
				StorageObjectsChunk ck = bs3Service.listObjectsChunked(
						this.getName(), null, null, 1000, lastKey);
				obj = ck.getObjects();
				objPos = 0;
			}
		} catch (Exception e) {
			throw new IOException(e);
		}

		if (objPos < obj.length) {
			boolean encrypt = false;
			lastKey = obj[objPos].getKey();
			if (lastKey.equals("bucketinfo")) {
				objPos++;
				return getNextChunck();
			}

			if (obj[objPos].containsMetadata("encrypt")) {
				encrypt = Boolean.parseBoolean((String) obj[objPos]
						.getMetadata("encrypt"));
			}
			ChunkData chk = new ChunkData(this.getHashBytes(
					obj[objPos].getKey(), encrypt), 0);
			if (obj[objPos].containsMetadata("size")) {
				chk.cLen = Integer.parseInt((String) obj[objPos]
						.getMetadata("size"));
				this.currentLength.addAndGet(chk.cLen);
			}
			if (obj[objPos].containsMetadata("compressedsize")) {
				int cl = Integer.parseInt((String) obj[objPos]
						.getMetadata("compressedsize"));
				this.compressedLength.addAndGet(cl);
			}
			objPos++;
			return chk;
		}
		return null;
	}

	StorageObject[] obj = null;
	int objPos = 0;
	S3Service bs3Service = null;
	String lastKey = null;

	@Override
	public void iterationInit() {

		try {
			bs3Service = new RestS3Service(awsCredentials);
			StorageObjectsChunk ck = bs3Service.listObjectsChunked(
					this.getName(), null, null, 1000, null);
			this.compressedLength.set(0);
			this.currentLength.set(0);
			obj = ck.getObjects();
			this.lastKey = null;
			objPos = 0;
		} catch (ServiceException e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}

	}

	@Override
	public long getFreeBlocks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long maxSize() {
		// TODO Auto-generated method stub
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		// TODO Auto-generated method stub
		return this.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sync() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setReadSpeed(int bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setWriteSpeed(int bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCacheSize(long bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getReadSpeed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getWriteSpeed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String restoreBlock(long id, byte[] hash) {
		return null;
		
	}

	@Override
	public boolean blockRestored(String id) {
		// TODO Auto-generated method stub
		return true;
	}

}
