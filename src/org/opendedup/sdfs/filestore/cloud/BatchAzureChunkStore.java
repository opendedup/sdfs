package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.HashBlobArchiveNoMap;
import org.opendedup.sdfs.filestore.StringResult;
import org.apache.commons.compress.utils.IOUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.filestore.cloud.utils.FileUtils;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import com.google.common.io.BaseEncoding;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.core.Base64;

import org.opendedup.collections.HashExistsException;

/**
 * 
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 * 
 */
public class BatchAzureChunkStore implements AbstractChunkStore,
		AbstractBatchStore, Runnable, AbstractCloudFileSync {
	CloudStorageAccount account;
	CloudBlobClient serviceClient = null;
	CloudBlobContainer container = null;
	private String name;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	boolean closed = false;
	boolean deleteUnclaimed = true;
	File staged_sync_location = new File(Main.chunkStore + File.separator
			+ "syncstaged");

	// private String bucketLocation = null;
	static {

	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey,
			String awsSecretKey, String bucketName) {
		return false;
	}

	public BatchAzureChunkStore() {

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
			SDFSLogger.getLog().info(
					"############ Closing Azure Container ##################");
			// container = pool.borrowObject();
			HashBlobArchive.close();
			HashMap<String, String> md = container.getMetadata();
			md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("compressedlength",
					Long.toString(HashBlobArchive.compressedLength.get()));
			container.setMetadata(md);
			container.uploadMetadata();
			SDFSLogger.getLog().info("Updated container on close");
			SDFSLogger.getLog().info(
					"############ Azure Container Closed ##################");
		} catch (Exception e) {
			SDFSLogger.getLog().error("error closing container", e);
		} finally {
			this.closed = true;
		}
		try {
			// this.serviceClient.
		} catch (Exception e) {

		}
	}

	public void expandFile(long length) throws IOException {

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len)
			throws IOException, DataArchivedException {
			return HashBlobArchive.getBlock(hash, start);

	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {

	}
	public void cacheData(byte[] hash, long start, int len)
			throws IOException, DataArchivedException {
		try {
			HashBlobArchive.cacheArchive(hash, start);
		} catch (ExecutionException e) {
			SDFSLogger.getLog().error("Unable to get block at " + start, e);
			throw new IOException(e);
		}

	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return HashBlobArchive.currentLength.get();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		try {
			return HashBlobArchive.writeBlock(hash, chunk);
		} catch (HashExistsException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error writing hash", e);
			throw new IOException(e);
		}
	}

	private ReentrantLock delLock = new ReentrantLock();

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		delLock.lock();
		try {
			// SDFSLogger.getLog().info("deleting " + del);
			if (this.deletes.containsKey(start)) {
				int sz = this.deletes.get(start) + 1;
				this.deletes.put(start, sz);
			} else
				this.deletes.put(start, 1);

		} catch (Exception e) {
			SDFSLogger.getLog().error("error putting data", e);
			throw new IOException(e);
		} finally {
			delLock.unlock();
		}

		/*
		 * String hashString = this.encHashArchiveName(start,
		 * Main.chunkStoreEncryptionEnabled); try { CloudBlockBlob blob =
		 * container.getBlockBlobReference("blocks/" +hashString);
		 * HashMap<String, String> metaData = blob.getMetadata(); int objs =
		 * Integer.parseInt(metaData.get("objects")); objs--; if(objs <= 0) {
		 * blob.delete(); blob = container.getBlockBlobReference("keys/"
		 * +hashString); blob.delete(); }else { metaData.put("objects",
		 * Integer.toString(objs)); blob.setMetadata(metaData);
		 * blob.uploadMetadata(); } } catch (Exception e) { SDFSLogger.getLog()
		 * .warn("Unable to delete object " + hashString, e); } finally {
		 * //pool.returnObject(container); }
		 */
	}

	public void deleteBucket() throws StorageException, IOException,
			InterruptedException {
		try {
			container.deleteIfExists();
		} finally {
			// pool.returnObject(container);
		}
		this.close();
	}

	@Override
	public void init(Element config) throws IOException {
		this.name = Main.cloudBucket.toLowerCase();
		this.staged_sync_location.mkdirs();
		String connectionProtocol = "https";
		if (config.hasAttribute("default-bucket-location")) {
			// bucketLocation = config.getAttribute("default-bucket-location");
		}
		if (config.hasAttribute("block-size")) {
			int sz = (int) StringUtils.parseSize(config
					.getAttribute("block-size"));
			HashBlobArchive.MAX_LEN = sz;

		}
		if (config.hasAttribute("sync-files")) {
			boolean syncf = Boolean.parseBoolean(config
					.getAttribute("sync-files"));
			if (syncf) {
				new FileReplicationService(this);
			}
		}
		if (config.hasAttribute("delete-unclaimed")) {
			this.deleteUnclaimed = Boolean.parseBoolean(config
					.getAttribute("delete-unclaimed"));
		}
		if (config.hasAttribute("upload-thread-sleep-time")) {
			int tm = Integer.parseInt(config
					.getAttribute("upload-thread-sleep-time"));
			HashBlobArchive.THREAD_SLEEP_TIME = tm;
		}
		if (config.hasAttribute("local-cache-size")) {
			long sz = StringUtils.parseSize(config
					.getAttribute("local-cache-size"));
			HashBlobArchive.setLocalCacheSize(sz);
		}
		if (config.hasAttribute("map-cache-size")) {
			int sz = Integer.parseInt(config.getAttribute("map-cache-size"));
			HashBlobArchive.MAP_CACHE_SIZE = sz;
		}
		if (config.hasAttribute("io-threads")) {
			int sz = Integer.parseInt(config.getAttribute("io-threads"));
			Main.dseIOThreads = sz;
		}
		int rsp = 0;
		int wsp = 0;
		if (config.hasAttribute("read-speed")) {
			rsp = Integer.parseInt(config.getAttribute("read-speed"));
		}
		if (config.hasAttribute("write-speed")) {
			wsp = Integer.parseInt(config.getAttribute("write-speed"));
		}
		
		//System.setProperty("http.keepalive", "true");
		
		System.setProperty("http.maxConnections",
				Integer.toString(Main.dseIOThreads * 2));
		
		if (config.getElementsByTagName("connection-props").getLength() > 0) {
			Element el = (Element) config.getElementsByTagName(
					"connection-props").item(0);
			NamedNodeMap ls = el.getAttributes();
			for (int i = 0; i < ls.getLength(); i++) {
				System.setProperty(ls.item(i).getNodeName(), ls.item(i)
						.getNodeValue());
				SDFSLogger.getLog().debug(
						"set connection value" + ls.item(i).getNodeName()
								+ " to " + ls.item(i).getNodeValue());
			}
		}
		try {
			String storageConnectionString = "DefaultEndpointsProtocol="
					+ connectionProtocol + ";" + "AccountName="
					+ Main.cloudAccessKey + ";" + "AccountKey="
					+ Main.cloudSecretKey;
			account = CloudStorageAccount.parse(storageConnectionString);
			serviceClient = account.createCloudBlobClient();
			serviceClient.getDefaultRequestOptions().setConcurrentRequestCount(
					Main.dseIOThreads * 2);
			/*
			serviceClient.getDefaultRequestOptions().setTimeoutIntervalInMs(
					10 * 1000);
			
			serviceClient.getDefaultRequestOptions().setRetryPolicyFactory(
					new RetryExponentialRetry(500, 5));
			*/
			container = serviceClient.getContainerReference(this.name);
			container.createIfNotExists();
			container.downloadAttributes();
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
			if(cl==0 || sz==0) {
			md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("compressedlength",
					Long.toString(HashBlobArchive.compressedLength.get()));
			container.setMetadata(md);
			container.uploadMetadata();
			}
			HashBlobArchive.currentLength.set(sz);
			HashBlobArchive.compressedLength.set(cl);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}
		Thread thread = new Thread(this);
		thread.start();
		HashBlobArchive.init(this);
		HashBlobArchive.setReadSpeed(rsp);
		HashBlobArchive.setWriteSpeed(wsp);
	}

	Iterator<ListBlobItem> iter = null;
	MultiDownload dl = null;

	@Override
	public void iterationInit(boolean deep) throws IOException {
		this.hid = 0;
		this.ht = null;
		iter = container.listBlobs("keys/").iterator();
		HashBlobArchive.currentLength.set(0);
		HashBlobArchive.compressedLength.set(0);
		dl = new MultiDownload(this);
		dl.iterationInit(false, "/keys");
		this.ht = null;
		this.hid = 0;
		
	}

	@Override
	public long getFreeBlocks() {
		return 0;
	}

	private StringTokenizer ht = null;
	private long hid = 0;

	@Override
	public synchronized ChunkData getNextChunck() throws IOException {
		if (ht == null || !ht.hasMoreElements()) {
			StringResult rs;
			try {
				rs = dl.getStringTokenizer();
			} catch (Exception e) {
				throw new IOException(e);
			}
			if (rs == null) {
				return null;
			}
			ht = rs.st;
			hid = rs.id;
		}
		if(!ht.hasMoreElements())
			return getNextChunck();
		else {
		String token = ht.nextToken();
		ChunkData chk = new ChunkData(BaseEncoding.base64().decode(
				token.split(":")[0]), hid);
		return chk;
		}
	}

	@Override
	public long maxSize() {
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		return HashBlobArchive.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id,
					Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/"
					+ haName);
			return blob.exists();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}

	private String[] getStrings(CloudBlockBlob blob) throws StorageException,
			IOException {
		HashMap<String, String> md = blob.getMetadata();
		byte[] nm = new byte[(int) blob.getProperties().getLength()];
		blob.downloadToByteArray(nm, 0);
		if (!md.containsKey("encrypt")) {
			blob.downloadAttributes();
			md = blob.getMetadata();
		}
		boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
		if (encrypt) {
			nm = EncryptUtils.decryptCBC(nm);
		}
		boolean compress = Boolean.parseBoolean(md.get("lz4Compress"));
		if (compress) {
			int size = Integer.parseInt(md.get("size"));
			nm = CompressionUtils.decompressLz4(nm, size);
		}
		String st = new String(nm);
		return st.split(",");

	}

	private int getClaimedObjects(CloudBlockBlob blob) throws IOException {
		try {
			String[] hs = this.getStrings(blob);
			HashMap<String, String> md = blob.getMetadata();
			boolean encrypt = false;
			if (!md.containsKey("encrypt")) {
				blob.downloadAttributes();
				md = blob.getMetadata();
			}

			encrypt = Boolean.parseBoolean(md.get("encrypt"));
			long id = EncyptUtils.decHashArchiveName(blob.getName()
					.substring(5), encrypt);
			int claims = 0;
			for (String ha : hs) {
				byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
				long cid = HCServiceProxy.getHashesMap().get(b);
				if (cid == id)
					claims++;
			}
			return claims;
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);

		byte[] chunks = arc.getBytes();
		try {
			// container = pool.borrowObject();
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/"
					+ haName);
			HashMap<String, String> metaData = new HashMap<String, String>();
			metaData.put("size", Integer.toString(arc.uncompressedLength.get()));
			if (Main.compress) {
				metaData.put("lz4Compress", "true");
			} else {
				metaData.put("lz4Compress", "false");
			}
			int csz = chunks.length;
			if (Main.chunkStoreEncryptionEnabled) {
				metaData.put("encrypt", "true");
			} else {
				metaData.put("encrypt", "false");
			}
			metaData.put("compressedsize", Integer.toString(chunks.length));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			metaData.put("objects", Integer.toString(arc.getSz()));

			blob.setMetadata(metaData);

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.reset();
			md.update(chunks);
			// Encode the md5 content using Base64 encoding
			String base64EncodedMD5content = Base64.encode(md.digest());
			// initialize blob properties and assign md5 content generated.
			BlobProperties blobProperties = blob.getProperties();
			blobProperties.setContentMD5(base64EncodedMD5content);
			ByteArrayInputStream s3IS = new ByteArrayInputStream(chunks);
			blob.upload(s3IS, chunks.length);
			s3IS.close();
			s3IS = null;
			
			// upload the metadata
			chunks = arc.getHashesString().getBytes();
			blob = container.getBlockBlobReference("keys/" + haName);
			// metaData = new HashMap<String, String>();
			int ssz = chunks.length;
			if (Main.compress) {
				chunks = CompressionUtils.compressLz4(chunks);
				metaData.put("lz4Compress", "true");
			} else {
				metaData.put("lz4Compress", "false");
			}
			if (Main.chunkStoreEncryptionEnabled) {
				chunks = EncryptUtils.encryptCBC(chunks);
				metaData.put("encrypt", "true");
			} else {
				metaData.put("encrypt", "false");
			}
			metaData.put("compressedsize", Integer.toString(chunks.length));
			// metaData.put("bsize", Integer.toString(arc.getLen()));
			// metaData.put("objects", Integer.toString(arc.getSz()));
			metaData.put("size", Integer.toString(ssz));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			metaData.put("bcompressedsize", Integer.toString(csz));
			metaData.put("objects", Integer.toString(arc.getSz()));
			md = MessageDigest.getInstance("MD5");
			md.reset();
			md.update(chunks);
			// Encode the md5 content using Base64 encoding
			base64EncodedMD5content = Base64.encode(md.digest());
			// initialize blob properties and assign md5 content generated.
			blobProperties = blob.getProperties();
			blobProperties.setContentMD5(base64EncodedMD5content);

			blob.setMetadata(metaData);
			s3IS = new ByteArrayInputStream(chunks);
			blob.upload(s3IS, chunks.length);
			s3IS.close();
			s3IS = null;
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to write archive " + arc.getID() + " with id " +id,
					e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	@Override
	public byte[] getBytes(long id) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id,
					Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/"
					+ haName);

			ByteArrayOutputStream out = new ByteArrayOutputStream((int) blob
					.getProperties().getLength());
			blob.download(out);
			HashMap<String, String> metaData = blob.getMetadata();
			byte[] data = out.toByteArray();
			if (metaData.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean(metaData.get("deleted"));
				if (del) {
					CloudBlockBlob kblob = container
							.getBlockBlobReference("keys/" + haName);
					kblob.downloadAttributes();
					metaData = kblob.getMetadata();
					int claims = this.getClaimedObjects(kblob);
					int delobj = 0;
					if (metaData.containsKey("deletedobjects")) {
						delobj = Integer.parseInt(metaData
								.get("deletedobjects")) - claims;
						if (delobj < 0)
							delobj = 0;
					}
					metaData.remove("deleted");
					metaData.put("deletedobjects", Integer.toString(delobj));
					metaData.put("suspect", "true");
					int _size = Integer.parseInt((String) metaData.get("size"));
					int _compressedSize = Integer.parseInt((String) metaData
							.get("compressedsize"));
					HashBlobArchive.currentLength.addAndGet(_size);
					HashBlobArchive.compressedLength.addAndGet(_compressedSize);
					blob.setMetadata(metaData);
					blob.uploadMetadata();
					metaData = kblob.getMetadata();
					metaData.remove("deleted");
					metaData.put("deletedobjects", Integer.toString(delobj));
					metaData.put("suspect", "true");
					kblob.setMetadata(metaData);
					kblob.uploadMetadata();

				}

			}
			return data;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch block [" + id + "]", e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	private int verifyDelete(long id) throws StorageException,
			URISyntaxException, IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		CloudBlockBlob kblob = container
				.getBlockBlobReference("keys/" + haName);
		kblob.downloadAttributes();
		HashMap<String, String> metaData = kblob.getMetadata();
		int claims = this.getClaimedObjects(kblob);
		if (claims > 0) {
			SDFSLogger.getLog().warn(
					"Reclaimed object " + id + " claims=" + claims);
			int delobj = 0;
			if (metaData.containsKey("deletedobjects")) {
				delobj = Integer.parseInt(metaData.get("deletedobjects"))
						- claims;
				if (delobj < 0)
					delobj = 0;
			}
			metaData.remove("deleted");
			metaData.put("deletedobjects", Integer.toString(delobj));
			metaData.put("suspect", "true");
			int _size = Integer.parseInt((String) metaData.get("size"));
			int _compressedSize = Integer.parseInt((String) metaData
					.get("compressedsize"));
			HashBlobArchive.currentLength.addAndGet(_size);
			HashBlobArchive.compressedLength.addAndGet(_compressedSize);
			metaData = kblob.getMetadata();
			metaData.remove("deleted");
			metaData.put("deletedobjects", Integer.toString(delobj));
			metaData.put("suspect", "true");
			kblob.setMetadata(metaData);
			kblob.uploadMetadata();
		} else {
			kblob.delete();
			kblob = container.getBlockBlobReference("blocks/" + haName);
			kblob.delete();
		}

		return claims;
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {
					HashMap<String, String> md = container.getMetadata();
					md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
					md.put("compressedlength",
							Long.toString(HashBlobArchive.compressedLength.get()));
					container.setMetadata(md);
					container.uploadMetadata();
				} catch(Exception e) {
					SDFSLogger.getLog().error("unable to update size", e);
				}
				if (this.deletes.size() > 0) {
					this.delLock.lock();
					HashMap<Long, Integer> odel = null;
					try {
						odel = this.deletes;
						this.deletes = new HashMap<Long, Integer>();
						// SDFSLogger.getLog().info("delete hash table size of "
						// + odel.size());
					} finally {
						this.delLock.unlock();
					}
					Set<Long> iter = odel.keySet();
					for (Long k : iter) {
						String hashString = EncyptUtils
								.encHashArchiveName(k.longValue(),
										Main.chunkStoreEncryptionEnabled);
						try {
							CloudBlockBlob blob = container
									.getBlockBlobReference("keys/"
											+ hashString);
							blob.downloadAttributes();
							HashMap<String, String> metaData = blob
									.getMetadata();
							int objs = Integer
									.parseInt(metaData.get("objects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							int delobj = 0;
							if (metaData.containsKey("deletedobjects"))
								delobj = Integer.parseInt((String) metaData
										.get("deletedobjects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							delobj = delobj + odel.get(k);
							if (objs <= delobj) {
								int size = Integer.parseInt((String) metaData
										.get("size"));
								int compressedSize = Integer
										.parseInt((String) metaData
												.get("compressedsize"));
								if (HashBlobArchive.compressedLength.get() > 0) {

									HashBlobArchive.compressedLength.addAndGet(-1
											* compressedSize);
								} else if (HashBlobArchive.compressedLength.get() < 0)
									HashBlobArchive.compressedLength.set(0);
								HashBlobArchive.currentLength.addAndGet(-1 * size);
								if (HashBlobArchive.currentLength.get() > 0) {
									HashBlobArchive.currentLength.addAndGet(-1 * size);
								} else if (HashBlobArchive.currentLength.get() < 0)
									HashBlobArchive.currentLength.set(0);
								HashBlobArchive.removeCache(k.longValue());
								if (this.deleteUnclaimed) {
									this.verifyDelete(k.longValue());
								} else {
									// SDFSLogger.getLog().info("deleting " +
									// hashString);
									metaData.put("deleted", "true");
									metaData.put("deletedobjects",
											Integer.toString(delobj));
									blob.setMetadata(metaData);
									blob.uploadMetadata();
									blob = container
											.getBlockBlobReference("keys/"
													+ hashString);
									blob.downloadAttributes();
									metaData = blob.getMetadata();
									metaData.put("deletedobjects",
											Integer.toString(delobj));
									metaData.put("deleted", "true");
									blob.uploadMetadata();
								}

							} else {
								// SDFSLogger.getLog().info("updating " +
								// hashString + " sz=" +objs);
								metaData.put("deletedobjects",
										Integer.toString(delobj));
								blob.setMetadata(metaData);
								blob.uploadMetadata();
								blob = container.getBlockBlobReference("keys/"
										+ hashString);
								blob.downloadAttributes();
								metaData = blob.getMetadata();
								metaData.put("deletedobjects",
										Integer.toString(delobj));
								blob.uploadMetadata();
							}
						} catch (Exception e) {
							delLock.lock();
							try {
								// SDFSLogger.getLog().info("deleting " + del);
								if (this.deletes.containsKey(k)) {
									int sz = this.deletes.get(k) + odel.get(k);
									this.deletes.put(k, sz);
								} else
									this.deletes.put(k, odel.get(k));

							} finally {
								delLock.unlock();
							}
							SDFSLogger.getLog().warn(
									"Unable to delete object " + hashString, e);
						} finally {
							// pool.returnObject(container);
						}
					}

				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				SDFSLogger.getLog().error("error in delete thread", e);
			}
		}

	}

	@Override
	public void sync() throws IOException {
		HashBlobArchive.sync();

	}

	private long getLastModified(String st) {

		try {
			CloudBlockBlob blob = container.getBlockBlobReference(st);
			blob.downloadAttributes();
			HashMap<String, String> metaData = blob.getMetadata();
			if (metaData.containsKey("lastmodified")) {
				return Long.parseLong((String) metaData.get("lastmodified"));
			} else {
				return 0;
			}
		} catch (Exception e) {
			return -1;
		} finally {

		}
	}

	@Override
	public void uploadFile(File f, String to, String pp) throws IOException {
		BufferedInputStream in = null;
		while (to.startsWith(File.separator))
			to = to.substring(1);
		String pth = pp + "/"
				+ EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		boolean isDir = false;
		boolean isSymlink = false;
		if (!OSValidator.isWindows()) {
			isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			isSymlink = Files.readAttributes(f.toPath(),
					PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
		} else {
			isDir = f.isDirectory();
		}
		if (isSymlink) {
			try {
			CloudBlockBlob blob = container.getBlockBlobReference(pth);
			HashMap<String, String> metaData = new HashMap<String,String>();
			metaData.put("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			metaData.put("lastmodified", Long.toString(f.lastModified()));
			String slp = EncyptUtils.encString(Files.readSymbolicLink(f.toPath()).toFile().getPath(), Main.chunkStoreEncryptionEnabled);
			metaData.put("symlink", slp);
			blob.setMetadata(metaData);
			blob.uploadText(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		}
		else if (isDir) {
			try {
				CloudBlockBlob blob = container.getBlockBlobReference(pth);
				HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
						Main.chunkStoreEncryptionEnabled);
				metaData.put("encrypt",
						Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				metaData.put("directory", "true");
				blob.setMetadata(metaData);
				blob.uploadText(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else {
			if (f.lastModified() == this.getLastModified(pth))
				return;
			String rnd = RandomGUID.getGuid();
			File p = new File(this.staged_sync_location, rnd);
			File z = new File(this.staged_sync_location, rnd + ".z");
			File e = new File(this.staged_sync_location, rnd + ".e");
			while (z.exists()) {
				rnd = RandomGUID.getGuid();
				p = new File(this.staged_sync_location, rnd);
				z = new File(this.staged_sync_location, rnd + ".z");
				e = new File(this.staged_sync_location, rnd + ".e");
			}
			try {
				BufferedInputStream is = new BufferedInputStream(
						new FileInputStream(f));
				BufferedOutputStream os = new BufferedOutputStream(
						new FileOutputStream(p));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				if (Main.compress) {
					CompressionUtils.compressFile(p, z);
					p.delete();
					p = z;
				}
				if (Main.chunkStoreEncryptionEnabled) {
					try {
						EncryptUtils.encryptFile(p, e);
					} catch (Exception e1) {
						throw new IOException(e1);
					}
					p.delete();
					p = e;
				}
				while (to.startsWith(File.separator))
					to = to.substring(1);
				CloudBlockBlob blob = container.getBlockBlobReference(pth);
				HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
						Main.chunkStoreEncryptionEnabled);
				metaData.put("lz4compress", Boolean.toString(Main.compress));
				metaData.put("encrypt",
						Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				blob.setMetadata(metaData);
				try (FileInputStream inputStream = new FileInputStream(p)) {
					MessageDigest digest = MessageDigest.getInstance("MD5");

					byte[] bytesBuffer = new byte[1024];
					int bytesRead = -1;

					while ((bytesRead = inputStream.read(bytesBuffer)) != -1) {
						digest.update(bytesBuffer, 0, bytesRead);
					}
					byte[] b = digest.digest();
					String base64EncodedMD5content = Base64.encode(b);

					// initialize blob properties and assign md5 content
					// generated.
					BlobProperties blobProperties = blob.getProperties();
					blobProperties.setContentMD5(base64EncodedMD5content);
				} catch (Exception ex) {
					throw new IOException("Could not generate hash from file",
							ex);
				}
				// Encode the md5 content using Base64 encoding

				in = new BufferedInputStream(new FileInputStream(p), 32768);
				blob.upload(in, p.length());

			} catch (Exception e1) {
				throw new IOException(e1);
			} finally {
				try {
					if (in != null)
						in.close();
				} finally {
					p.delete();
					z.delete();
					e.delete();
				}
			}
		}

	}

	@Override
	public void downloadFile(String nm, File to, String pp) throws IOException {
		if (to.exists())
			throw new IOException("file " + to.getPath() + " exists");
		String rnd = RandomGUID.getGuid();
		File p = new File(this.staged_sync_location, rnd);
		File z = new File(this.staged_sync_location, rnd + ".uz");
		File e = new File(this.staged_sync_location, rnd + ".de");
		while (z.exists()) {
			rnd = RandomGUID.getGuid();
			p = new File(this.staged_sync_location, rnd);
			z = new File(this.staged_sync_location, rnd + ".uz");
			e = new File(this.staged_sync_location, rnd + ".de");
		}
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(pp
					+ "/"
					+ EncyptUtils.encString(nm,
							Main.chunkStoreEncryptionEnabled));
			blob.downloadToFile(p.getPath());
			byte[] md5 = Base64.decode(blob.getProperties().getContentMD5());
			if (!FileUtils.fileValid(p, md5))
				throw new IOException("file " + p.getPath() + " is corrupt");
			HashMap<String, String> metaData = blob.getMetadata();
			boolean encrypt = false;
			boolean lz4compress = false;
			boolean snappycompress = false;
			if (metaData.containsKey("encrypt")) {
				encrypt = Boolean.parseBoolean(metaData.get("encrypt"));
			}
			if (metaData.containsKey("lz4compress")) {
				lz4compress = Boolean.parseBoolean(metaData.get("lz4compress"));
			}
			if (metaData.containsKey("symlink")) {
				if(OSValidator.isWindows())
					throw new IOException("unable to restore symlinks to windows");
				else {
					String spth = EncyptUtils.decString(metaData.get("symlink"),
							encrypt);
					Path srcP = Paths.get(spth);
					Path dstP = Paths.get(to.getPath());
					Files.createSymbolicLink(dstP, srcP);
				}
			}
			else if (metaData.containsKey("directory")) {
				to.mkdirs();
				FileUtils.setFileMetaData(to, metaData, encrypt);
				p.delete();
			} else {

				

				if (encrypt) {
					EncryptUtils.decryptFile(p, e);
					p.delete();
					p = e;
				}
				if (lz4compress) {
					CompressionUtils.decompressFile(p, z);
					p.delete();
					p = z;
				}
				if (snappycompress) {
					CompressionUtils.decompressFileSnappy(p, z);
					p.delete();
					p = z;
				}
				if (!to.getParentFile().exists()) {
					to.getParentFile().mkdirs();
				}
				if (to.exists())
					throw new IOException("file " + to.getPath() + " exists");

				BufferedInputStream is = new BufferedInputStream(
						new FileInputStream(p));
				BufferedOutputStream os = new BufferedOutputStream(
						new FileOutputStream(to));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				FileUtils.setFileMetaData(to, metaData, encrypt);
			}
		} catch (Exception e1) {
			throw new IOException(e1);
		} finally {
			p.delete();
			z.delete();
			e.delete();
		}

	}

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		String haName = EncyptUtils.encString(nm,
				Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(pp + "/"
					+ haName);
			blob.delete();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void renameFile(String from, String to, String pp)
			throws IOException {
		while (from.startsWith(File.separator))
			from = from.substring(1);
		while (to.startsWith(File.separator))
			to = to.substring(1);
		String fn = EncyptUtils.encString(from,
				Main.chunkStoreEncryptionEnabled);
		String tn = EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob sblob = container.getBlockBlobReference(pp + "/"
					+ fn);
			CloudBlockBlob tblob = container.getBlockBlobReference(pp + "/"
					+ tn);
			tblob.startCopy(sblob);
			while (tblob.getCopyState().getStatus() == CopyStatus.PENDING) {
				Thread.sleep(10);
			}
			if (tblob.getCopyState().getStatus() == CopyStatus.SUCCESS) {
				sblob.delete();
			} else {
				throw new IOException("unable to rename file " + fn
						+ " because " + tblob.getCopyState().getStatus().name()
						+ " : " + tblob.getCopyState().getStatusDescription());
			}

		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	Iterator<ListBlobItem> di = null;

	public void clearIter() {
		di = null;
	}

	public String getNextName(String pp) throws IOException {
		String pfx = pp + "/";
		if (di == null)
			di = container.listBlobs(pp + "/").iterator();
		while (di.hasNext()) {
			CloudBlob bi = (CloudBlob) di.next();
			try {
				bi.downloadAttributes();

				HashMap<String, String> md = bi.getMetadata();
				boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
				String fname = EncyptUtils.decString(
						bi.getName().substring(pfx.length()), encrypt);
				return fname;
				/*
				 * this.downloadFile(fname, new File(to.getPath() +
				 * File.separator + fname), pp);
				 */
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		di = null;
		return null;
	}

	public Map<String, Integer> getHashMap(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob kblob = container.getBlockBlobReference("keys/"
					+ haName);
			kblob.downloadAttributes();
			String[] ks = this.getStrings(kblob);
			HashMap<String, Integer> m = new HashMap<String, Integer>(ks.length);
			for (String k : ks) {
				String[] kv = k.split(":");
				m.put(kv[0], Integer.parseInt(kv[1]));
			}
			return m;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean checkAccess() {
		Exception e = null;
		for (int i = 0; i < 3; i++) {
			try {
				HashMap<String, String> md = container.getMetadata();
				if (md.containsKey("currentlength")) {
					Long.parseLong(md.get("currentlength"));
					return true;
				}
			} catch (Exception _e) {
				e = _e;
				SDFSLogger.getLog().debug(
						"unable to connect to bucket try " + i + " of 3", e);
			}
		}
		if (e != null)
			SDFSLogger.getLog().warn(
					"unable to connect to bucket try " + 3 + " of 3", e);
		return false;
	}

	@Override
	public void setReadSpeed(int kbps) {
		HashBlobArchive.setReadSpeed((double) kbps);

	}

	@Override
	public void setWriteSpeed(int kbps) {
		HashBlobArchive.setWriteSpeed((double) kbps);

	}

	@Override
	public void setCacheSize(long sz) throws IOException {
		HashBlobArchive.setCacheSize(sz);

	}

	@Override
	public int getReadSpeed() {
		return (int) HashBlobArchive.getReadSpeed();
	}

	@Override
	public int getWriteSpeed() {
		return (int) HashBlobArchive.getWriteSpeed();
	}

	@Override
	public long getCacheSize() {
		return HashBlobArchive.getCacheSize();
	}

	@Override
	public long getMaxCacheSize() {
		return HashBlobArchive.getLocalCacheSize();
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

	@Override
	public boolean checkAccess(String username, String password,
			Properties props) throws Exception {
		String storageConnectionString = "DefaultEndpointsProtocol="
				+ props.getProperty("protocol") + ";" + "AccountName="
				+ Main.cloudAccessKey + ";" + "AccountKey="
				+ Main.cloudSecretKey;
		account = CloudStorageAccount.parse(storageConnectionString);
		serviceClient = account.createCloudBlobClient();
		serviceClient.listContainers();
		return true;
	}

	public void recoverVolumeConfig(String na, File to, String parentPath,
			String accessKey, String secretKey, String bucket, Properties props)
			throws IOException {
		try {
			if (to.exists())
				throw new IOException("file exists " + to.getPath());
			String storageConnectionString = "DefaultEndpointsProtocol="
					+ "https;" + "AccountName=" + accessKey + ";"
					+ "AccountKey=" + secretKey;
			CloudStorageAccount _account;
			CloudBlobClient _serviceClient = null;
			CloudBlobContainer _container = null;
			_account = CloudStorageAccount.parse(storageConnectionString);
			_serviceClient = _account.createCloudBlobClient();
			_container = _serviceClient.getContainerReference(bucket);
			boolean encrypt = Boolean.parseBoolean(props.getProperty("encrypt",
					"false"));
			String keyStr = null;
			String ivStr = null;
			if (encrypt) {
				keyStr = props.getProperty("key");
				ivStr = props.getProperty("iv");
			}
			Iterator<ListBlobItem> _iter = _container.listBlobs("volume/")
					.iterator();
			while (_iter.hasNext()) {
				CloudBlob bi = (CloudBlob) _iter.next();
				bi.downloadAttributes();
				HashMap<String, String> md = bi.getMetadata();
				String vn = bi.getName().substring("volume/".length());
				System.out.println(vn);
				if (md.containsKey("encrypt")) {
					vn = new String(EncryptUtils.decryptCBC(BaseEncoding
							.base64Url().decode(vn), keyStr, ivStr));
				}

				if (vn.equalsIgnoreCase(na + "-volume-cfg.xml")) {
					String rnd = RandomGUID.getGuid();
					File tmpdir = com.google.common.io.Files.createTempDir();
					File p = new File(tmpdir, rnd);
					File z = new File(tmpdir, rnd + ".uz");
					File e = new File(tmpdir, rnd + ".de");
					while (z.exists()) {
						rnd = RandomGUID.getGuid();
						p = new File(tmpdir, rnd);
						z = new File(tmpdir, rnd + ".uz");
						e = new File(tmpdir, rnd + ".de");
					}
					try {
						CloudBlockBlob blob = _container
								.getBlockBlobReference(bi.getName());

						BufferedOutputStream out = new BufferedOutputStream(
								new FileOutputStream(p));
						blob.download(out);
						HashMap<String, String> metaData = blob.getMetadata();
						out.flush();
						out.close();
						boolean enc = false;
						boolean lz4compress = false;
						if (metaData.containsKey("encrypt")) {
							enc = Boolean.parseBoolean(metaData.get("encrypt"));
						}
						if (metaData.containsKey("lz4compress")) {
							lz4compress = Boolean.parseBoolean(metaData
									.get("lz4compress"));
						}
						if (enc) {
							EncryptUtils.decryptFile(p, e, keyStr, ivStr);
							p.delete();
							p = e;
						}
						if (lz4compress) {
							CompressionUtils.decompressFile(p, z);
							p.delete();
							p = z;
						}
						if (!to.getParentFile().exists()) {
							to.getParentFile().mkdirs();
						}
						if (to.exists())
							throw new IOException("file " + to.getPath()
									+ " exists");

						BufferedInputStream is = new BufferedInputStream(
								new FileInputStream(p));
						BufferedOutputStream os = new BufferedOutputStream(
								new FileOutputStream(to));
						IOUtils.copy(is, os);
						os.flush();
						os.close();
						is.close();

					} catch (Exception e1) {
						throw new IOException(e1);
					} finally {
						p.delete();
						z.delete();
						e.delete();
					}
					break;
				}

			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static void main(String[] args) throws IOException {

	}

	@Override
	public void deleteStore() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void compact() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<String> getNextObjectList() throws IOException {
		List<String> al = new ArrayList<String>();
		for(int i = 0;i<1000;i++) {
			if(iter.hasNext()) {
				CloudBlob bi = (CloudBlob) iter.next();
				try {
					al.add(bi.getName());
				} catch (URISyntaxException e) {
					throw new IOException(e);
				}
			}else return al.iterator();
		}
		return al.iterator();
	}

	@Override
	public StringResult getStringResult(String key) throws IOException,
			InterruptedException {
		try {
		CloudBlob bi = (CloudBlob) container.getBlockBlobReference(key);
		bi.downloadAttributes();
		HashMap<String, String> md = bi.getMetadata();
		byte[] nm = new byte[(int) bi.getProperties().getLength()];
		bi.downloadToByteArray(nm, 0);
		boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
		if (encrypt) {
			nm = EncryptUtils.decryptCBC(nm);
		}
		this.hid = EncyptUtils.decHashArchiveName(bi.getName()
				.substring(5), encrypt);
		boolean compress = Boolean.parseBoolean(md.get("lz4Compress"));
		if (compress) {
			int size = Integer.parseInt(md.get("size"));
			nm = CompressionUtils.decompressLz4(nm, size);
		}
		String st = new String(nm);
		ht = new StringTokenizer(st, ",");
		boolean changed = false;
		if (md.containsKey("deleted")) {
			md.remove("deleted");
			changed = true;
		}
		if (md.containsKey("deletedobjects")) {
			changed = true;
			md.remove("deletedobjects");
		}
		if (changed) {
			bi.setMetadata(md);
			bi.uploadMetadata();
			String bnm = "blocks/" + bi.getName().substring(5);
			CloudBlockBlob blob = container.getBlockBlobReference(bnm);
			blob.downloadAttributes();
			HashMap<String, String> bmd = blob.getMetadata();
			bmd.remove("deletedobjects");
			bmd.remove("deleted");
			blob.setMetadata(bmd);
			blob.uploadMetadata();
		}
		try {
			int _sz = Integer.parseInt(md.get("bsize"));
			int _cl = Integer.parseInt(md.get("compressedsize"));
			HashBlobArchive.currentLength.addAndGet(_sz);
			HashBlobArchive.compressedLength.addAndGet(_cl);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to update size", e);
		}
		StringResult rslt = new StringResult();
		rslt.id = hid;
		rslt.st = ht;
		SDFSLogger.getLog().info("st="+rslt.st);
		return rslt;
		}catch(Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchiveNoMap arc, int id) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
