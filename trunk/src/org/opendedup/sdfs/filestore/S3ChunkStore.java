package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.bouncycastle.util.Arrays;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.XpathUtils;
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
	private static BasicAWSCredentials awsCredentials = null;
	private String name;
	private AWSS3ServicePool pool = null;
	private AtomicLong currentLength = new AtomicLong(0);
	private AtomicLong compressedLength = new AtomicLong(0);
	private int cacheSize = 10485760 / Main.CHUNK_LENGTH;
	private static XpathUtils utils = new XpathUtils();

	LoadingCache<String, byte[]> chunks = CacheBuilder.newBuilder()
			.maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<String, byte[]>() {
				public byte[] load(String hashString) throws IOException {
					AmazonS3Client s3Service = null;
					try {
						s3Service = pool.borrowObject();
						S3Object obj = s3Service.getObject(name, hashString);
						boolean encrypt = false;
						boolean compress = false;
						boolean lz4compress = false;
						Map<String, String> md = obj.getObjectMetadata()
								.getUserMetadata();
						int size = Integer.parseInt(md.get("size"));
						// Map<String,String> md =
						// obj.getObjectMetadata().getUserMetadata();
						int csz = size;
						if (md.containsKey("encrypt")) {
							encrypt = Boolean.parseBoolean(md.get("encrypt"));
						}
						if (md.containsKey("compress")) {
							compress = Boolean.parseBoolean(md.get("compress"));
						} else if (md.containsKey("lz4compress")) {
							csz = Integer.parseInt(md.get("compressedsize"));
							lz4compress = Boolean.parseBoolean(md
									.get("lz4compress"));
						}
						int cl = (int) obj.getObjectMetadata()
								.getContentLength();
						if (cl != csz) {
							SDFSLogger.getLog().warn(
									"Possible data mismatch size=" + csz
											+ " does not equal content length"
											+ cl);
						}

						byte[] data = new byte[cl];
						DataInputStream in = new DataInputStream(obj
								.getObjectContent());
						in.readFully(data);
						obj.close();
						if (encrypt)
							data = EncryptUtils.decrypt(data);
						if (compress)
							data = CompressionUtils.decompressZLIB(data);
						else if (lz4compress) {
							data = CompressionUtils.decompressLz4(data, size);
						}
						return data;
					} catch (Throwable e) {
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
			awsCredentials = new BasicAWSCredentials(Main.cloudAccessKey,
					Main.cloudSecretKey);
			System.out.println(utils.toString());
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			System.exit(-1);
		}
	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		BasicAWSCredentials creds = null;
		try {
			creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
			AmazonS3Client s3Service = new AmazonS3Client(creds);
			s3Service.listBuckets();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			return false;
		}
	}

	public static boolean checkBucketUnique(String awsAccessKey,
			String awsSecretKey, String bucketName) {
		BasicAWSCredentials creds = null;
		try {
			creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
			AmazonS3Client s3Service = new AmazonS3Client(creds);
			boolean exists = s3Service.doesBucketExist(bucketName);
			if (!exists) {
				s3Service.createBucket(bucketName);
			}
			return true;
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
		AmazonS3Client s3Service = null;
		try {

			s3Service = pool.borrowObject();
			s3Service.deleteObject(name, "bucketinfo");
			byte[] b = new byte[1];
			ByteArrayInputStream bInput = new ByteArrayInputStream(b);
			ObjectMetadata md = new ObjectMetadata();
			md.addUserMetadata("currentsize",
					Long.toString(this.currentLength.get()));
			md.addUserMetadata("currentcompressedsize",
					Long.toString(this.compressedLength.get()));
			md.setContentLength(b.length);
			s3Service.putObject(name, "bucketinfo", bInput, md);
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
		this.name = name.toLowerCase();
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return this.currentLength.get();
	}

	private static ReentrantLock lock = new ReentrantLock();

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		try {
			String hashString = this.getHashName(hash,
					Main.chunkStoreEncryptionEnabled);
			lock.lock();
			RandomAccessFile rf = new RandomAccessFile("/tmp/hashes.txt", "rw");
			rf.seek(rf.length());
			rf.writeBytes(hashString + "\n");
			rf.close();
			lock.unlock();
			ObjectMetadata md = new ObjectMetadata();
			md.addUserMetadata("size", Integer.toString(chunk.length));
			this.currentLength.addAndGet(chunk.length);
			if (Main.compress) {
				chunk = CompressionUtils.compressLz4(chunk);
				md.addUserMetadata("lz4compress", "true");
			} else {
				md.addUserMetadata("lz4compress", "false");
			}
			if (Main.chunkStoreEncryptionEnabled) {
				chunk = EncryptUtils.encrypt(chunk);
				md.addUserMetadata("encrypt", "true");
			} else {
				md.addUserMetadata("encrypt", "false");
			}
			md.addUserMetadata("compressedsize", Integer.toString(chunk.length));
			md.setContentLength(chunk.length);
			this.compressedLength.addAndGet(chunk.length);
			ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
			md.setContentType("binary/octet-stream");
			AmazonS3Client s3Service = null;
			try {
				s3Service = pool.borrowObject();
				s3Service.putObject(name, hashString, s3IS, md);
				SDFSLogger.getLog().info(hashString);

				return 0;
			} finally {
				pool.returnObject(s3Service);
				s3IS.close();

			}
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal(
					"unable to upload "
							+ this.getHashName(hash,
									Main.chunkStoreEncryptionEnabled), e);
			throw new IOException(e);
		}
	}

	public static void main(String[] args) throws IOException {

	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash,
				Main.chunkStoreEncryptionEnabled);
		AmazonS3Client s3Service = null;
		try {
			this.chunks.invalidate(hashString);
			s3Service = pool.borrowObject();
			Map<String, String> md = s3Service.getObjectMetadata(name,
					hashString).getUserMetadata();
			int size = Integer.parseInt(md.get("size"));
			int compressedSize = Integer.parseInt(md.get("compressedsize"));
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

	private String getHashName(byte[] hash, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encrypt(hash);
			return StringUtils.getHexString(encH).toLowerCase();
		} else {
			return StringUtils.getHexString(hash).toLowerCase();
		}
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
	public void init(Element config) throws IOException {
		this.name = Main.cloudBucket.toLowerCase();
		try {

			pool = new AWSS3ServicePool(S3ChunkStore.awsCredentials, 8);
			AmazonS3Client s3Service = pool.borrowObject();

			boolean exists = s3Service.doesBucketExist(this.name);
			if (!exists) {
				s3Service.createBucket(this.name);
				SDFSLogger.getLog().info("created new store " + name);
			} else {
				try {
					s3Service.getObjectMetadata(this.name, "bucketinfo");

					Map<String, String> md = s3Service.getObjectMetadata(
							this.name, "bucketinfo").getUserMetadata();

					if (!md.isEmpty()) {
						this.currentLength.set(Long.parseLong(md
								.get("currentsize")));
						this.compressedLength.set(Long.parseLong((md
								.get("currentsize"))));
					} else {
						SDFSLogger
								.getLog()
								.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}
				} catch (Exception e) {
					SDFSLogger
							.getLog()
							.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added",
									e);
				}
			}
			pool.returnObject(s3Service);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public ChunkData getNextChunck() throws IOException {

		if (objPos < obj.size()) {
			S3ObjectSummary _obj = obj.get(objPos);
			boolean encrypt = false;
			lastKey = _obj.getKey();
			Map<String, String> md = bs3Service
					.getObjectMetadata(name, lastKey).getUserMetadata();
			if (md.containsKey("encrypt")) {
				encrypt = Boolean.parseBoolean(md.get("encrypt"));
			}
			ChunkData chk = new ChunkData(this.getHashBytes(lastKey, encrypt),
					0);
			if (md.containsKey("size")) {
				chk.cLen = Integer.parseInt(md.get("size"));
				this.currentLength.addAndGet(chk.cLen);
			}
			if (md.containsKey("compressedsize")) {
				int cl = Integer.parseInt(md.get("compressedsize"));
				this.compressedLength.addAndGet(cl);
			}
			objPos++;
			return chk;
		}
		return null;
	}

	List<S3ObjectSummary> obj = null;
	int objPos = 0;
	AmazonS3Client bs3Service = null;
	String lastKey = null;

	@Override
	public void iterationInit() {
		bs3Service = new AmazonS3Client(awsCredentials);
		obj = bs3Service.listObjects(this.name).getObjectSummaries();
		this.compressedLength.set(0);
		this.currentLength.set(0);
		this.lastKey = null;
		objPos = 0;
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

}
