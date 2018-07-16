package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;


import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.jets3t.service.utils.ServiceUtils;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.StringResult;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.cloud.azure.BlobDataIO;
import org.opendedup.sdfs.filestore.cloud.azure.BlobDataTracker;
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

import static java.lang.Math.toIntExact;

import com.google.common.io.BaseEncoding;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.RehydrationStatus;
import com.microsoft.azure.storage.blob.StandardBlobTier;
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
public class BatchAzureChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {
	CloudStorageAccount account;
	CloudBlobClient serviceClient = null;
	CloudBlobContainer container = null;
	private String name;

	OperationContext opContext = new OperationContext();
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean clustered = true;
	private int checkInterval = 15000;
	private static final int version = 1;
	private String accessKey = Main.cloudAccessKey;
	private String secretKey = Main.cloudSecretKey;
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	private boolean standAlone = true;
	private StandardBlobTier tier = null;
	private HashSet<Long> refresh = new HashSet<Long>();
	private BlobDataIO bio = null;
	private int tierInDays = 30;
	private boolean tierImmedately = false;
	private String endpoint = null;

	// private String bucketLocation = null;
	static {

	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
		return false;
	}

	public BatchAzureChunkStore() {

	}

	@Override
	public void clearCounters() {
		HashBlobArchive.setCompressedLength(0);
		HashBlobArchive.setLength(0);
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
			SDFSLogger.getLog().info("############ Closing Azure Container ##################");
			// container = pool.borrowObject();
			if (this.standAlone) {
				HashBlobArchive.close();
				HashMap<String, String> md = container.getMetadata();
				md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
				md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
				container.setMetadata(md);

				container.uploadMetadata();
				SDFSLogger.getLog().info("Updated container on close");
			}
			SDFSLogger.getLog().info("############ Azure Container Closed ##################");
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
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException, DataArchivedException {
		return HashBlobArchive.getBlock(hash, start);

	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {

	}

	public void cacheData(long start) throws IOException, DataArchivedException {
		try {
			HashBlobArchive.cacheArchive(start);
		} catch (ExecutionException e) {
			SDFSLogger.getLog().error("Unable to get block at " + start, e);
			throw new IOException(e);
		}

	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		try {
			RemoteVolumeInfo[] rv = this.getConnectedVolumes();
			long sz = 0;
			for (RemoteVolumeInfo r : rv) {
				sz += r.data;
			}
			return sz;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to get clustered compressed size", e);
		}
		// return HashBlobArchive.getCompressedLength();
		return HashBlobArchive.getLength();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len, String uuid) throws IOException {
		try {
			return HashBlobArchive.writeBlock(hash, chunk, uuid);
		} catch (HashExistsException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error writing hash", e);
			throw new IOException(e);
		}
	}

	private ReentrantLock delLock = new ReentrantLock();

	@Override
	public void deleteChunk(byte[] hash, long start, int len) throws IOException {
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
		 * container.getBlockBlobReference("blocks/" +hashString); HashMap<String,
		 * String> metaData = blob.getMetadata(); int objs =
		 * Integer.parseInt(metaData.get("objects")); objs--; if(objs <= 0) {
		 * blob.delete(); blob = container.getBlockBlobReference("keys/" +hashString);
		 * blob.delete(); }else { metaData.put("objects", Integer.toString(objs));
		 * blob.setMetadata(metaData); blob.uploadMetadata(); } } catch (Exception e) {
		 * SDFSLogger.getLog() .warn("Unable to delete object " + hashString, e); }
		 * finally { //pool.returnObject(container); }
		 */
	}

	public void deleteBucket() throws StorageException, IOException, InterruptedException {
		try {
			container.deleteIfExists();
		} finally {
			// pool.returnObject(container);
		}
		this.close();
	}

	@Override
	public void init(Element config) throws IOException {
		String storageConnectionString = null;
		if(config.hasAttribute("connection-string")) {
			storageConnectionString = config.getAttribute("connection-string");
		}
		if (config.hasAttribute("bucket-name")) {
			this.name = config.getAttribute("bucket-name").toLowerCase();
		} else {
			this.name = Main.cloudBucket.toLowerCase();
		}
		if (config.hasAttribute("backlog-size")) {
			if (config.getAttribute("backlog-size").equals("-1")) {
				HashBlobArchive.maxQueueSize = -1;
			} else if (!config.getAttribute("backlog-size").equals("0")) {
				long bsz = StringUtils.parseSize(config.getAttribute("block-size"));
				long qsz = StringUtils.parseSize(config.getAttribute("backlog-size"));
				if (qsz > 0) {
					long tsz = qsz / bsz;
					HashBlobArchive.maxQueueSize = Math.toIntExact(tsz);
				}
			}
		}else {
			HashBlobArchive.maxQueueSize = 0;
		}
		if (config.hasAttribute("access-key")) {
			this.accessKey = config.getAttribute("access-key");
		}
		if (config.hasAttribute("secret-key")) {
			this.secretKey = config.getAttribute("secret-key");
		}
		this.staged_sync_location.mkdirs();
		String connectionProtocol = "https";
		if (config.hasAttribute("default-bucket-location")) {
			// bucketLocation = config.getAttribute("default-bucket-location");
		}
		if (this.standAlone && config.hasAttribute("block-size")) {
			int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
			HashBlobArchive.MAX_LEN = sz;

		}
		if(this.standAlone && config.hasAttribute("backlog-size")) {
			if (config.getAttribute("backlog-size").equals("-1")) {
				HashBlobArchive.maxQueueSize = -1;
			} else if (!config.getAttribute("backlog-size").equals("0")) {
				long bsz = StringUtils.parseSize(config.getAttribute("block-size"));
				long qsz = StringUtils.parseSize(config.getAttribute("backlog-size"));
				if (qsz > 0) {
					long tsz = qsz / bsz;
					HashBlobArchive.maxQueueSize = Math.toIntExact(tsz);
				}
			}
		}
		if(config.hasAttribute("endpoint")) {
			this.endpoint = config.getAttribute("endpoint");
		}
		if (config.hasAttribute("user-agent-prefix")) {
			String ua = config.getAttribute("user-agent-prefix");
			HashMap<String, String> headers = new HashMap<String, String>();
			headers.put("User-Agent", ua);
			opContext.setUserHeaders(headers);

		}
		if (config.hasAttribute("connection-check-interval")) {
			this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
		}
		if (config.hasAttribute("sync-files")) {
			boolean syncf = Boolean.parseBoolean(config.getAttribute("sync-files"));
			if (syncf) {
				new FileReplicationService(this);
			}
		}
		if (config.hasAttribute("delete-unclaimed")) {
			this.deleteUnclaimed = Boolean.parseBoolean(config.getAttribute("delete-unclaimed"));
		}
		if (this.standAlone && config.hasAttribute("upload-thread-sleep-time")) {
			int tm = Integer.parseInt(config.getAttribute("upload-thread-sleep-time"));
			HashBlobArchive.THREAD_SLEEP_TIME = tm;
		}
		if (this.standAlone && config.hasAttribute("local-cache-size")) {
			long sz = StringUtils.parseSize(config.getAttribute("local-cache-size"));
			HashBlobArchive.setLocalCacheSize(sz);
		}
		if (this.standAlone && config.hasAttribute("map-cache-size")) {
			int sz = Integer.parseInt(config.getAttribute("map-cache-size"));
			HashBlobArchive.MAP_CACHE_SIZE = sz;
		}
		if (this.standAlone && config.hasAttribute("io-threads")) {
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
		if (config.hasAttribute("storage-tier")) {
			if (config.getAttribute("storage-tier").equalsIgnoreCase("hot"))
				this.tier = StandardBlobTier.HOT;
			if (config.getAttribute("storage-tier").equalsIgnoreCase("cool"))
				this.tier = StandardBlobTier.COOL;
			if (config.getAttribute("storage-tier").equalsIgnoreCase("archive"))
				this.tier = StandardBlobTier.ARCHIVE;
			if (this.tier != null) {
				SDFSLogger.getLog().info("Set Storage Tier to " + tier);
			} else {
				SDFSLogger.getLog().warn("Storage Tier " + config.getAttribute("storage-tier") + " not found");
			}
		}
		if (config.hasAttribute("azure-tier-in-days")) {
			this.tierInDays = Integer.parseInt(config.getAttribute("azure-tier-in-days"));
			if (config.hasAttribute("tier-immediately")) {
				this.tierImmedately = Boolean.parseBoolean(config.getAttribute("tier-immediately"));
			}
			Main.REFRESH_BLOBS = true;
			Main.checkArchiveOnRead = true;
			SDFSLogger.getLog().info("Azure in days = " + this.tierInDays + " tier-immediately=" + this.tierImmedately + " tier-level=" +this.tier);
		}
		
		
		// System.setProperty("http.keepalive", "true");

		System.setProperty("http.maxConnections", Integer.toString(Main.dseIOThreads * 2));

		if (config.getElementsByTagName("connection-props").getLength() > 0) {
			Element el = (Element) config.getElementsByTagName("connection-props").item(0);
			NamedNodeMap ls = el.getAttributes();
			for (int i = 0; i < ls.getLength(); i++) {
				System.setProperty(ls.item(i).getNodeName(), ls.item(i).getNodeValue());
				SDFSLogger.getLog()
						.debug("set connection value" + ls.item(i).getNodeName() + " to " + ls.item(i).getNodeValue());
			}
		}
		try {
			if(storageConnectionString == null)
				storageConnectionString = "DefaultEndpointsProtocol=" + connectionProtocol + ";" + "AccountName="
					+ this.accessKey + ";" + "AccountKey=" + this.secretKey;
			if(this.endpoint != null)
				storageConnectionString = storageConnectionString + "EndpointSuffix=" + this.endpoint;
				
			account = CloudStorageAccount.parse(storageConnectionString);
			serviceClient = account.createCloudBlobClient();
			serviceClient.getDefaultRequestOptions().setConcurrentRequestCount(Main.dseIOThreads * 2);
			if (tier != null && (tier.equals(StandardBlobTier.ARCHIVE) || tier.equals(StandardBlobTier.COOL))) {
				this.bio = new BlobDataIO(this.name + "table", this.accessKey, this.secretKey, connectionProtocol);
			}
			/*
			 * serviceClient.getDefaultRequestOptions().setTimeoutIntervalInMs( 10 * 1000);
			 * 
			 * 
			 * serviceClient.getDefaultRequestOptions().setRetryPolicyFactory( new
			 * RetryExponentialRetry(500, 5));
			 */
			container = serviceClient.getContainerReference(this.name);
			container.createIfNotExists(null, null, opContext);
			container.downloadAttributes();
			HashMap<String, String> md = container.getMetadata();
			if (md.size() == 0)
				this.clustered = true;
			else if (md.containsKey("clustered"))
				this.clustered = Boolean.parseBoolean(md.get("clustered"));
			else
				this.clustered = false;
			md.put("clustered", Boolean.toString(clustered));
			long sz = 0;
			long cl = 0;
			if (!this.clustered) {
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

				if (cl == 0 || sz == 0) {
					md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
					md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
					md.put("clustered", Boolean.toString(this.clustered));
					md.put("lastupdated", Long.toString(System.currentTimeMillis()));
					md.put("port", Integer.toString(Main.sdfsCliPort));
					container.setMetadata(md);
					container.uploadMetadata();
				}
			} else {
				String lbi = "bucketinfo/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				if (this.standAlone) {
					CloudBlockBlob blob = container.getBlockBlobReference(lbi);
					if (blob.exists()) {
						blob.downloadAttributes();
						HashMap<String, String> metaData = blob.getMetadata();
						if (metaData.containsKey("currentlength")) {
							sz = Long.parseLong(metaData.get("currentlength"));
							if (sz < 0)
								sz = 0;
						}
						if (metaData.containsKey("compressedlength")) {
							cl = Long.parseLong(metaData.get("compressedlength"));
							if (cl < 0)
								cl = 0;
						}
					}
					HashMap<String, String> metaData = new HashMap<String, String>();
					metaData.put("currentlength", Long.toString(HashBlobArchive.getLength()));
					metaData.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
					metaData.put("clustered", Boolean.toString(this.clustered));
					metaData.put("hostname", InetAddress.getLocalHost().getHostName());
					metaData.put("port", Integer.toString(Main.sdfsCliPort));
					metaData.put("lastupdated", Long.toString(System.currentTimeMillis()));
					metaData.put("bucketversion", Integer.toString(version));
					metaData.put("sdfsversion", Main.version);
					blob.setMetadata(metaData);
					blob.uploadText(Long.toString(System.currentTimeMillis()));
					blob.uploadMetadata();
					container.setMetadata(md);
					container.uploadMetadata(null, null, opContext);
					SDFSLogger.getLog().debug("set user metadata " + metaData.size());
				}

			}
			if (this.standAlone) {
				HashBlobArchive.setLength(sz);
				HashBlobArchive.setCompressedLength(cl);

			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}
		Thread thread = new Thread(this);
		thread.start();
		if (this.standAlone) {
			HashBlobArchive.init(this);
			HashBlobArchive.setReadSpeed(rsp, false);
			HashBlobArchive.setWriteSpeed(wsp, false);
		}
	}

	Iterator<ListBlobItem> iter = null;
	MultiDownload dl = null;

	@Override
	public HashMap<String, String> getUserMetaData(String object) throws IOException {
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(object);
			blob.downloadAttributes();
			return blob.getMetadata();
		} catch (Exception e) {
			SDFSLogger.getLog().info("unable to download attribute", e);
			throw new IOException(e);
		}
	}

	@Override
	public void updateBucketInfo(Map<String, String> md) throws IOException {
		try {
			String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference(lbi);
			blob.downloadAttributes();
			blob.setMetadata((HashMap<String, String>) md);
			blob.uploadText(Long.toString(System.currentTimeMillis()));
			blob.uploadMetadata(null, null, opContext);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, String> getBucketInfo() {

		try {
			String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference(lbi);
			blob.downloadAttributes();
			HashMap<String, String> md = blob.getMetadata();
			return md;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to update metadata for bucket" + Main.DSEID, e);
			return null;
		}
	}

	@Override
	public void iterationInit(boolean deep) throws IOException {
		this.hid = 0;
		this.ht = null;
		try {
			String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference(lbi);
			blob.downloadAttributes();
			HashMap<String, String> md = blob.getMetadata();
			md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
			md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
			md.put("clustered", Boolean.toString(this.clustered));
			md.put("hostname", InetAddress.getLocalHost().getHostName());
			md.put("lastupdated", Long.toString(System.currentTimeMillis()));
			md.put("bucketversion", Integer.toString(version));
			md.put("sdfsversion", Main.version);
			md.put("port", Integer.toString(Main.sdfsCliPort));
			blob = container.getBlockBlobReference(lbi + "-" + System.currentTimeMillis());
			blob.setMetadata(md);
			blob.uploadMetadata(null, null, opContext);
		} catch (Exception e) {
			SDFSLogger.getLog().info("unable to create backup of current volume info", e);
		}
		iter = container.listBlobs("keys/").iterator();
		if (this.standAlone) {
			HashBlobArchive.setLength(0);
			HashBlobArchive.setCompressedLength(0);
		}
		dl = new MultiDownload(this, "keys/");
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
		if (!ht.hasMoreElements())
			return getNextChunck();
		else {
			String token = ht.nextToken();
			ChunkData chk = new ChunkData(BaseEncoding.base64().decode(token.split(":")[0]), hid);
			return chk;
		}
	}

	@Override
	public long maxSize() {
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		try {
			RemoteVolumeInfo[] rv = this.getConnectedVolumes();
			long sz = 0;
			for (RemoteVolumeInfo r : rv) {
				sz += r.compressed;
			}
			return sz;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to get clustered compressed size", e);
		}
		return HashBlobArchive.getCompressedLength();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + haName);
			return blob.exists();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}

	private String[] getStrings(CloudBlockBlob blob) throws StorageException, IOException {
		HashMap<String, String> md = blob.getMetadata();
		byte[] nm = new byte[(int) blob.getProperties().getLength()];
		blob.downloadToByteArray(nm, 0, null, null, opContext);
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

	private String getClaimName(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// SDFSLogger.getLog().info("id="+id+ " claims/keys/" + haName + "/"
		// + EncyptUtils.encHashArchiveName(Main.DSEID,
		// Main.chunkStoreEncryptionEnabled));
		return "claims/keys/" + haName + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
	}

	private int getClaimedObjects(CloudBlockBlob blob, long id) throws IOException {
		try {
			String[] hs = this.getStrings(blob);
			HashMap<String, String> md = blob.getMetadata();
			if (!md.containsKey("encrypt")) {
				blob.downloadAttributes();
				md = blob.getMetadata();
			}

			int claims = 0;
			for (String ha : hs) {
				byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
				if (HCServiceProxy.getHashesMap().mightContainKey(b, id))
					claims++;
			}
			return claims;
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

		byte[] f = arc.getBytes();
		IOException e = null;
		for (int i = 0; i < 9; i++) {
			try {
				// container = pool.borrowObject();
				CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + haName);
				HashMap<String, String> metaData = new HashMap<String, String>();
				metaData.put("size", Integer.toString(arc.uncompressedLength.get()));
				if (Main.compress) {
					metaData.put("lz4Compress", "true");
				} else {
					metaData.put("lz4Compress", "false");
				}
				int csz = toIntExact(f.length);
				if (Main.chunkStoreEncryptionEnabled) {
					metaData.put("encrypt", "true");
				} else {
					metaData.put("encrypt", "false");
				}
				metaData.put("compressedsize", Integer.toString(csz));
				metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
				metaData.put("objects", Integer.toString(arc.getSz()));
				metaData.put("lastaccessed", Long.toString(System.currentTimeMillis()));

				blob.setMetadata(metaData);

				ByteArrayInputStream in = new ByteArrayInputStream(f);
				String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(in));
				IOUtils.closeQuietly(in);
				// initialize blob properties and assign md5 content generated.
				BlobProperties blobProperties = blob.getProperties();
				blobProperties.setContentMD5(mds);
				ByteArrayInputStream bin = new ByteArrayInputStream(f);
				blob.upload(bin, csz, null, null, opContext);
				IOUtils.closeQuietly(bin);
				if (tier != null && (tier.equals(StandardBlobTier.ARCHIVE) || tier.equals(StandardBlobTier.COOL))) {
					this.bio.updateBlobDataTracker(id,
							EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
				} else if (this.tier != null) {
					blob.uploadStandardBlobTier(this.tier);
				}
				// upload the metadata
				byte[] chunks = arc.getHashesString().getBytes();
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
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.reset();
				md.update(chunks);
				// Encode the md5 content using Base64 encoding
				String base64EncodedMD5content = Base64.encode(md.digest());
				// initialize blob properties and assign md5 content generated.
				blobProperties = blob.getProperties();
				blobProperties.setContentMD5(base64EncodedMD5content);

				blob.setMetadata(metaData);
				ByteArrayInputStream s3IS = new ByteArrayInputStream(chunks);
				blob.upload(s3IS, chunks.length, null, null, opContext);
				s3IS.close();
				s3IS = null;
				blob = container.getBlockBlobReference(this.getClaimName(id));
				blob.setMetadata(metaData);
				blob.uploadText(Long.toString(System.currentTimeMillis()));
				return;
			} catch (Throwable e1) {
				SDFSLogger.getLog().debug("unable to write archive " + arc.getID() + " with id " + id, e1);
				e = new IOException(e1);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e2) {

				}
			}
		}
		if (e != null) {
			SDFSLogger.getLog().error("unable to write block", e);
			throw e;
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException, DataArchivedException {
		Exception e = null;
		for (int i = 0; i < 9; i++) {
			try {
				if (f.exists() && !f.delete()) {
					SDFSLogger.getLog().warn("file already exists! " + f.getPath());
					File nf = new File(f.getPath() + " " + ".old");
					Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
				String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
				CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + haName);
				if (this.tier != null && this.tier.equals(StandardBlobTier.ARCHIVE)) {
					blob.downloadAttributes();
					if (blob.getProperties().getStandardBlobTier().equals(StandardBlobTier.ARCHIVE)) {
						this.restoreBlock(id, new byte [16]);
						throw new DataArchivedException(id, null);
					}
				}
				if (this.tier != null && this.tier.equals(StandardBlobTier.COOL)) {
					blob.uploadStandardBlobTier(StandardBlobTier.HOT);
				}
				blob.downloadToFile(f.getPath(), null, null, opContext);
				if (Main.REFRESH_BLOBS) {
					synchronized (this.refresh) {
						this.refresh.add(id);
					}
				}
				HashMap<String, String> metaData = blob.getMetadata();
				if (metaData.containsKey("deleted")) {
					boolean del = Boolean.parseBoolean(metaData.get("deleted"));
					if (del) {
						CloudBlockBlob kblob = container.getBlockBlobReference("keys/" + haName);
						kblob.downloadAttributes();
						metaData = kblob.getMetadata();
						int claims = this.getClaimedObjects(kblob, id);
						int delobj = 0;
						if (metaData.containsKey("deletedobjects")) {
							delobj = Integer.parseInt(metaData.get("deletedobjects")) - claims;
							if (delobj < 0)
								delobj = 0;
						}
						metaData.remove("deleted");
						metaData.put("deletedobjects", Integer.toString(delobj));
						metaData.put("suspect", "true");
						int _size = Integer.parseInt((String) metaData.get("size"));
						int _compressedSize = Integer.parseInt((String) metaData.get("compressedsize"));
						if (this.standAlone) {
							HashBlobArchive.addToLength(_size);
							HashBlobArchive.addToCompressedLength(_compressedSize);
						}
						blob.setMetadata(metaData);
						blob.uploadMetadata(null, null, opContext);
						metaData = kblob.getMetadata();
						metaData.remove("deleted");
						metaData.put("deletedobjects", Integer.toString(delobj));
						metaData.put("suspect", "true");
						kblob.setMetadata(metaData);
						kblob.uploadMetadata(null, null, opContext);
						e = null;
						break;
					}

				}

			} catch (DataArchivedException e1) {
				throw e1;
			} catch (Exception e1) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e2) {
				}
				e = e1;
			} finally {

			}
			if (e != null) {
				SDFSLogger.getLog().error(
						"unable to fetch block [" + id + "] to file " + f.getPath() + " file exists=" + f.exists(), e);
				throw new IOException(e);
			}
		}
	}

	public int verifyDelete(long id) throws IOException {
		int claims = 0;
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob kblob = container.getBlockBlobReference("keys/" + haName);
			CloudBlockBlob cblob = null;
			if (this.clustered)
				cblob = container.getBlockBlobReference(this.getClaimName(id));
			kblob.downloadAttributes();
			cblob.downloadAttributes();
			HashMap<String, String> metaData = null;
			if (clustered)
				metaData = cblob.getMetadata();
			else
				metaData = kblob.getMetadata();
			claims = this.getClaimedObjects(kblob, id);
			if (claims > 0) {
				SDFSLogger.getLog().warn("Reclaimed object " + id + " claims=" + claims);
				int delobj = 0;
				if (metaData.containsKey("deletedobjects")) {
					delobj = Integer.parseInt(metaData.get("deletedobjects")) - claims;
					if (delobj < 0)
						delobj = 0;
				}
				metaData.remove("deleted");
				metaData.put("deletedobjects", Integer.toString(delobj));
				metaData.put("suspect", "true");
				int _size = Integer.parseInt((String) metaData.get("size"));
				int _compressedSize = Integer.parseInt((String) metaData.get("compressedsize"));
				if (this.standAlone) {
					HashBlobArchive.addToLength(_size);
					HashBlobArchive.addToCompressedLength(_compressedSize);
				}
				metaData = kblob.getMetadata();
				metaData.remove("deleted");
				metaData.put("deletedobjects", Integer.toString(delobj));
				metaData.put("suspect", "true");
				if (clustered) {
					cblob.setMetadata(metaData);
					cblob.uploadMetadata();
				} else {
					kblob.setMetadata(metaData);
					kblob.uploadMetadata();
				}
			} else {
				if (clustered) {
					cblob.delete();
					try {
						bio.removeBlobDataTracker(id,
								EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
						if (Main.REFRESH_BLOBS) {
							synchronized (this.refresh) {
								this.refresh.remove(id);
							}
						}
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to remove " + id + " from tablelob", e);
						
					}
					if (!container.listBlobs("claims/keys/" + haName).iterator().hasNext()) {
						kblob.delete();
						kblob = container.getBlockBlobReference("blocks/" + haName);
						kblob.delete();
						kblob = container.getBlockBlobReference("keys/" + haName);
						kblob.delete();
						SDFSLogger.getLog().info("deleted block " + id + " name=blocks/" + haName);
					}
				} else {
					kblob.delete();
					kblob = container.getBlockBlobReference("blocks/" + haName);
					kblob.delete();
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}

		return claims;
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis() + 15 * 60 * 1000; // Start Archiving after 15 minutes
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {
					if (!this.clustered) {
						HashMap<String, String> md = container.getMetadata();
						md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
						md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
						container.setMetadata(md);
						container.uploadMetadata(null, null, opContext);
					} else {
						String lbi = "bucketinfo/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
						CloudBlockBlob blob = container.getBlockBlobReference(lbi);
						blob.downloadAttributes();
						HashMap<String, String> md = blob.getMetadata();
						md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
						md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
						md.put("clustered", Boolean.toString(this.clustered));
						md.put("hostname", InetAddress.getLocalHost().getHostName());
						md.put("lastupdated", Long.toString(System.currentTimeMillis()));
						md.put("bucketversion", Integer.toString(version));
						md.put("sdfsversion", Main.version);
						md.put("port", Integer.toString(Main.sdfsCliPort));
						blob.setMetadata(md);
						blob.uploadMetadata(null, null, opContext);
					}
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to update size", e);
				}
				if (this.tier != null
						&& (tier.equals(StandardBlobTier.ARCHIVE) || tier.equals(StandardBlobTier.COOL))) {
					HashSet<Long> orr = new HashSet<Long>();
					String dseID = EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					if (Main.REFRESH_BLOBS) {
						synchronized (this.refresh) {
							orr.addAll(this.refresh);
							this.refresh.clear();
						}
						for (Long id : orr) {
							try {
								bio.updateBlobDataTracker(id, dseID);
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to update block id " + id, e);
							}
						}
					} else {
						this.refresh.clear();
					}
					long currentTime = System.currentTimeMillis();
					if (currentTime > startTime) {
						Iterable<BlobDataTracker> tri = null;
						if (this.tierImmedately) {
							long mins = (Long.valueOf(this.tierInDays) * 60 * 1000) + 60000;
							SDFSLogger.getLog().info("Checking how many archives are " + mins + " back");
							tri = bio.getBlobDataTrackers(mins, dseID);
						} else
							tri = bio.getBlobDataTrackers(this.tierInDays, dseID);
						for (BlobDataTracker bt : tri) {
							String hashString = EncyptUtils.encHashArchiveName(Long.parseLong(bt.getRowKey()),
									Main.chunkStoreEncryptionEnabled);
							try {
								SDFSLogger.getLog().info("Moving  blocks/" + hashString + " to " + this.tier);
								CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + hashString);
								blob.downloadAttributes();
								if (!blob.getProperties().getStandardBlobTier().equals(tier)) {
									blob.uploadStandardBlobTier(this.tier);
									SDFSLogger.getLog().info("Moved  blocks/" + hashString + " to "
											+ blob.getProperties().getStandardBlobTier());
								}
								bio.removeBlobDataTracker(Long.parseLong(bt.getRowKey()), dseID);
							} catch (Throwable e) {
								SDFSLogger.getLog().warn("unable to change storage status for blocks/" + hashString, e);
							}
						}
					}
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
						String hashString = EncyptUtils.encHashArchiveName(k.longValue(),
								Main.chunkStoreEncryptionEnabled);
						try {
							CloudBlockBlob blob = null;
							if (this.clustered)
								blob = container.getBlockBlobReference(this.getClaimName(k));
							else
								blob = container.getBlockBlobReference("keys/" + hashString);
							blob.downloadAttributes();
							HashMap<String, String> metaData = blob.getMetadata();
							int objs = Integer.parseInt(metaData.get("objects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							int delobj = 0;
							if (metaData.containsKey("deletedobjects"))
								delobj = Integer.parseInt((String) metaData.get("deletedobjects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							delobj = delobj + odel.get(k);
							if (objs <= delobj) {
								int size = Integer.parseInt((String) metaData.get("size"));
								int compressedSize = Integer.parseInt((String) metaData.get("compressedsize"));
								if (this.standAlone) {
									if (HashBlobArchive.getCompressedLength() > 0) {
										HashBlobArchive.addToCompressedLength((-1 * compressedSize));
									} else if (HashBlobArchive.getCompressedLength() < 0)
										HashBlobArchive.setCompressedLength(0);

									if (HashBlobArchive.getLength() > 0) {
										HashBlobArchive.addToLength(-1 * size);
									} else if (HashBlobArchive.getLength() < 0)
										HashBlobArchive.setLength(0);
									else
										HashBlobArchive.setLength(-1 * size);
								}
								HashBlobArchive.removeCache(k.longValue());
								if (this.deleteUnclaimed) {
									SDFSLogger.getLog().info("checking to delete " +k.longValue());
									this.verifyDelete(k.longValue());
								} else {
									// SDFSLogger.getLog().info("deleting " +
									// hashString);
									metaData.put("deleted", "true");
									metaData.put("deletedobjects", Integer.toString(delobj));
									blob.setMetadata(metaData);
									blob.uploadMetadata(null, null, opContext);
								}

							} else {
								// SDFSLogger.getLog().info("updating " +
								// hashString + " sz=" +objs);
								metaData.put("deletedobjects", Integer.toString(delobj));
								blob.setMetadata(metaData);
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
							SDFSLogger.getLog().warn("Unable to delete object " + hashString, e);
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

	@Override
	public void uploadFile(File f, String to, String pp, HashMap<String, String> metaData, boolean disableComp)
			throws IOException {
		while (to.startsWith(File.separator))
			to = to.substring(1);
		to = FilenameUtils.separatorsToUnix(to);
		String pth = pp + "/" + EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		boolean isDir = false;
		boolean isSymlink = false;
		if (!OSValidator.isWindows()) {
			isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isDirectory();
			isSymlink = Files.readAttributes(f.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		} else {
			isDir = f.isDirectory();
		}
		if (isSymlink) {
			try {
				CloudBlockBlob blob = container.getBlockBlobReference(pth);
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				String slp = EncyptUtils.encString(Files.readSymbolicLink(f.toPath()).toFile().getPath(),
						Main.chunkStoreEncryptionEnabled);
				metaData.put("symlink", slp);
				blob.setMetadata(metaData);
				blob.uploadText(pth);
				if (this.isClustered())
					this.checkoutFile(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else if (isDir) {
			try {
				CloudBlockBlob blob = container.getBlockBlobReference(pth);
				HashMap<String, String> _metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.putAll(_metaData);
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				metaData.put("directory", "true");
				blob.setMetadata(metaData);
				blob.uploadText(pth);
				if (this.isClustered())
					this.checkoutFile(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else {
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
				BufferedInputStream is = null;
				if (disableComp)
					p = f;
				else {
					is = new BufferedInputStream(new FileInputStream(f));
					BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(p));
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
				}
				while (to.startsWith(File.separator))
					to = to.substring(1);
				CloudBlockBlob blob = container.getBlockBlobReference(pth);
				HashMap<String, String> _metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.putAll(_metaData);
				if (!disableComp) {
					metaData.put("lz4compress", Boolean.toString(Main.compress));
					metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
					metaData.put("lastmodified", Long.toString(f.lastModified()));
				}
				blob.setMetadata(metaData);
				// Encode the md5 content using Base64 encoding

				is = new BufferedInputStream(new FileInputStream(p));
				blob.upload(is, p.length());
				if (this.isClustered())
					this.checkoutFile(pth);
				IOUtils.closeQuietly(is);
			} catch (Exception e1) {
				throw new IOException(e1);
			} finally {
				if (!disableComp) {
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
			String fn = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
			CloudBlockBlob blob = container.getBlockBlobReference(fn);
			blob.downloadToFile(p.getPath(), null, null, opContext);
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
				if (OSValidator.isWindows())
					throw new IOException("unable to restore symlinks to windows");
				else {
					String spth = EncyptUtils.decString(metaData.get("symlink"), encrypt);
					Path srcP = Paths.get(spth);
					Path dstP = Paths.get(to.getPath());
					Files.createSymbolicLink(dstP, srcP);
				}
			} else if (metaData.containsKey("directory")) {
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

				BufferedInputStream is = new BufferedInputStream(new FileInputStream(p));
				BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(to));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				FileUtils.setFileMetaData(to, metaData, encrypt);
			}
			if (this.isClustered())
				this.checkoutFile(fn);
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
		String haName = EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);

		try {
			CloudBlockBlob blob = container.getBlockBlobReference(pp + "/" + haName);
			if (blob.exists()) {
				if (this.isClustered()) {
					String blb = "claims/" + haName + "/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					blob = container.getBlockBlobReference(blb);
					blob.delete();
					if (!container.listBlobs("claims/" + haName).iterator().hasNext()) {
						blob = container.getBlockBlobReference(pp + "/" + haName);
						blob.delete();
					}

				} else {

					blob.delete();
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void renameFile(String from, String to, String pp) throws IOException {
		while (from.startsWith(File.separator))
			from = from.substring(1);
		while (to.startsWith(File.separator))
			to = to.substring(1);
		String fn = EncyptUtils.encString(from, Main.chunkStoreEncryptionEnabled);
		String tn = EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob sblob = container.getBlockBlobReference(pp + "/" + fn);
			CloudBlockBlob tblob = container.getBlockBlobReference(pp + "/" + tn);
			tblob.startCopy(sblob);
			while (tblob.getCopyState().getStatus() == CopyStatus.PENDING) {
				Thread.sleep(10);
			}
			if (tblob.getCopyState().getStatus() == CopyStatus.SUCCESS) {
				sblob.delete();
			} else {
				throw new IOException(
						"unable to rename file " + fn + " because " + tblob.getCopyState().getStatus().name() + " : "
								+ tblob.getCopyState().getStatusDescription());
			}

		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	Iterator<ListBlobItem> di = null;

	public void clearIter() {
		di = null;
	}

	public String getNextName(String pp, long id) throws IOException {
		String pfx = pp + "/";
		if (di == null)
			di = container.listBlobs(pp + "/").iterator();
		while (di.hasNext()) {
			CloudBlob bi = (CloudBlob) di.next();
			try {
				bi.downloadAttributes();

				HashMap<String, String> md = bi.getMetadata();
				boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
				String fname = EncyptUtils.decString(bi.getName().substring(pfx.length()), encrypt);
				return fname;
				/*
				 * this.downloadFile(fname, new File(to.getPath() + File.separator + fname),
				 * pp);
				 */
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		di = null;
		return null;
	}

	public Map<String, Long> getHashMap(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob kblob = container.getBlockBlobReference("keys/" + haName);
			kblob.downloadAttributes(null, null, opContext);
			String[] ks = this.getStrings(kblob);
			HashMap<String, Long> m = new HashMap<String, Long>(ks.length);
			for (String k : ks) {
				String[] kv = k.split(":");
				m.put(kv[0], Long.parseLong(kv[1]));
			}
			return m;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean checkAccess() {
		Exception e = null;
		for (int i = 0; i < 9; i++) {
			try {
				if (!this.clustered) {
					HashMap<String, String> md = container.getMetadata();
					if (md.containsKey("currentlength")) {
						Long.parseLong(md.get("currentlength"));
						return true;
					}
				} else {
					String lbi = "bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					CloudBlockBlob blob = container.getBlockBlobReference(lbi);
					blob.downloadAttributes();
					HashMap<String, String> md = blob.getMetadata();
					Long.parseLong(md.get("currentlength"));
					return true;
				}
			} catch (Exception _e) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {

				}
				e = _e;
				SDFSLogger.getLog().debug("unable to connect to bucket try " + i + " of 3", e);
			}
		}
		if (e != null)
			SDFSLogger.getLog().warn("unable to connect to bucket try " + 3 + " of 3", e);
		return false;
	}

	@Override
	public void setReadSpeed(int kbps) {
		HashBlobArchive.setReadSpeed((double) kbps, true);

	}

	@Override
	public void setWriteSpeed(int kbps) {
		HashBlobArchive.setWriteSpeed((double) kbps, true);

	}

	@Override
	public void setCacheSize(long sz) throws IOException {
		HashBlobArchive.setCacheSize(sz, true);

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

	private WeakHashMap<Long, String> restoreRequests = new WeakHashMap<Long, String>();

	@Override
	public String restoreBlock(long id, byte[] hash) throws IOException {
		if (id == -1) {
			SDFSLogger.getLog().warn("Hash not found for " + StringUtils.getHexString(hash) + " id " + id);
			return null;
		}
		String haName = this.restoreRequests.get(new Long(id));
		if (haName == null)
			haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		else {
			return haName;
		}
		try {
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + haName);
			blob.downloadAttributes();
			if (blob.getProperties().getStandardBlobTier().equals(StandardBlobTier.ARCHIVE)) {
				blob.uploadStandardBlobTier(StandardBlobTier.HOT);
				bio.removeBlobDataTracker(id,
						EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
				this.restoreRequests.put(new Long(id), haName);
				return haName;
			} else {
				this.restoreRequests.put(new Long(id), null);
				return null;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to restore " + id,e);
			throw new IOException(e);
		}

	}

	@Override
	public boolean blockRestored(String id) {
		try {
			CloudBlockBlob blob = container.getBlockBlobReference("blocks/" + id);
			if(!blob.exists())
				return true;
			blob.downloadAttributes();
			if (blob.getProperties().getStandardBlobTier().equals(StandardBlobTier.HOT)) {
				return true;
			} else {
				if (blob.getProperties().getRehydrationStatus() == null
						|| blob.getProperties().getRehydrationStatus().equals(RehydrationStatus.UNKNOWN)) {
					SDFSLogger.getLog().warn("rehydration status unknow for " + id + " will attempt to rehydrate");
					blob.uploadStandardBlobTier(StandardBlobTier.HOT);
				}
				return false;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while checking block [" + id + "] restored", e);
			return false;
		}
	}

	@Override
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
		String storageConnectionString = "DefaultEndpointsProtocol=" + props.getProperty("protocol") + ";"
				+ "AccountName=" + Main.cloudAccessKey + ";" + "AccountKey=" + Main.cloudSecretKey;
		account = CloudStorageAccount.parse(storageConnectionString);
		serviceClient = account.createCloudBlobClient();
		serviceClient.listContainers();
		return true;
	}

	public void recoverVolumeConfig(String na, File to, String parentPath, String accessKey, String secretKey,
			String bucket, Properties props) throws IOException {
		try {
			if (to.exists())
				throw new IOException("file exists " + to.getPath());
			String storageConnectionString = "DefaultEndpointsProtocol=" + "https;" + "AccountName=" + accessKey + ";"
					+ "AccountKey=" + secretKey;
			CloudStorageAccount _account;
			CloudBlobClient _serviceClient = null;
			CloudBlobContainer _container = null;
			_account = CloudStorageAccount.parse(storageConnectionString);
			_serviceClient = _account.createCloudBlobClient();
			_container = _serviceClient.getContainerReference(bucket);
			boolean encrypt = Boolean.parseBoolean(props.getProperty("encrypt", "false"));
			String keyStr = null;
			String ivStr = null;
			if (encrypt) {
				keyStr = props.getProperty("key");
				ivStr = props.getProperty("iv");
			}
			Iterator<ListBlobItem> _iter = _container.listBlobs("volume/").iterator();
			while (_iter.hasNext()) {
				CloudBlob bi = (CloudBlob) _iter.next();
				bi.downloadAttributes();
				HashMap<String, String> md = bi.getMetadata();
				String vn = bi.getName().substring("volume/".length());
				System.out.println(vn);
				if (md.containsKey("encrypt")) {
					vn = new String(EncryptUtils.decryptCBC(BaseEncoding.base64Url().decode(vn), keyStr, ivStr));
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
						CloudBlockBlob blob = _container.getBlockBlobReference(bi.getName());

						BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(p));
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
							lz4compress = Boolean.parseBoolean(metaData.get("lz4compress"));
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
							throw new IOException("file " + to.getPath() + " exists");

						BufferedInputStream is = new BufferedInputStream(new FileInputStream(p));
						BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(to));
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

	@Override
	public void deleteStore() {
		// TODO Auto-generated method stub

	}

	@Override
	public void compact() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<String> getNextObjectList(String prefix) throws IOException {
		List<String> al = new ArrayList<String>();
		for (int i = 0; i < 1000; i++) {
			if (iter.hasNext()) {
				CloudBlob bi = (CloudBlob) iter.next();
				// SDFSLogger.getLog().info("key name= " + bi.getName());
				al.add(bi.getName());
			} else
				return al.iterator();
		}
		return al.iterator();
	}

	@Override
	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		try {
			CloudBlob bi = (CloudBlob) container.getBlockBlobReference(key);
			bi.downloadAttributes();
			HashMap<String, String> md = bi.getMetadata();
			byte[] nm = new byte[(int) bi.getProperties().getLength()];
			bi.downloadToByteArray(nm, 0, null, null, opContext);
			boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
			if (encrypt) {
				nm = EncryptUtils.decryptCBC(nm);
			}
			long sid = EncyptUtils.decHashArchiveName(bi.getName().substring(5), encrypt);
			boolean compress = Boolean.parseBoolean(md.get("lz4Compress"));
			if (compress) {
				int size = Integer.parseInt(md.get("size"));
				nm = CompressionUtils.decompressLz4(nm, size);
			}
			String st = new String(nm);
			StringTokenizer sht = new StringTokenizer(st, ",");
			CloudBlob nbi = (CloudBlob) container.getBlockBlobReference(this.getClaimName(sid));
			nbi.downloadAttributes();
			md = nbi.getMetadata();
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
				bi.uploadMetadata(null, null, opContext);
				String bnm = this.getClaimName(sid);
				try {
				
				CloudBlockBlob blob = container.getBlockBlobReference(bnm);
				blob.downloadAttributes();
				HashMap<String, String> bmd = blob.getMetadata();
				bmd.remove("deletedobjects");
				bmd.remove("deleted");
				blob.setMetadata(bmd);
				blob.uploadMetadata(null, null, opContext);
				}catch(Exception e) {
					SDFSLogger.getLog().warn("unable to update key " +bnm,e);
				}
			}
			try {
				int _sz = Integer.parseInt(md.get("bsize"));
				int _cl = Integer.parseInt(md.get("compressedsize"));
				if (this.standAlone) {
					HashBlobArchive.addToLength(_sz);
					HashBlobArchive.addToCompressedLength(_cl);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to update size", e);
			}
			StringResult rslt = new StringResult();
			rslt.id = sid;
			rslt.st = sht;
			SDFSLogger.getLog().debug("st=" + rslt.st);
			return rslt;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		try {
			CloudBlockBlob cblob = container.getBlockBlobReference(this.getClaimName(id));
			if (cblob.exists())
				return;
			else {
				String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
				CloudBlockBlob kblob = container.getBlockBlobReference("keys/" + haName);
				kblob.downloadAttributes();
				HashMap<String, String> metaData = kblob.getMetadata();
				cblob.setMetadata(metaData);
				cblob.uploadText(Long.toString(System.currentTimeMillis()));
			}
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public boolean objectClaimed(String key) throws IOException {
		if (!this.clustered)
			return true;
		try {
			CloudBlockBlob kblob = container.getBlockBlobReference("claims/" + key + "/"
					+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
			return kblob.exists();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error checking if blob is clamimed", e);
			return false;
		}
	}

	@Override
	public void checkoutFile(String name) throws IOException {
		try {
			name = FilenameUtils.separatorsToUnix(name);
			CloudBlockBlob kblob = container.getBlockBlobReference("claims/" + name + "/"
					+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
			kblob.uploadText(Long.toString(System.currentTimeMillis()));
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
		if (!this.clustered)
			return true;
		try {
			CloudBlockBlob kblob = container.getBlockBlobReference("claims/" + name + "/"
					+ EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled));
			return kblob.exists();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error checking if blob is clamimed", e);
			return false;
		}
	}

	@Override
	public int getCheckInterval() {
		return this.checkInterval;
	}

	@Override
	public boolean isClustered() {
		// TODO Auto-generated method stub
		return this.clustered;
	}

	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (this.isClustered()) {
			try {
				Iterator<ListBlobItem> it = container.listBlobs("bucketinfo/").iterator();
				ArrayList<RemoteVolumeInfo> al = new ArrayList<RemoteVolumeInfo>();
				while (it.hasNext()) {
					CloudBlob bi = (CloudBlob) it.next();
					bi.downloadAttributes();
					HashMap<String, String> md = bi.getMetadata();
					SDFSLogger.getLog().debug("keysize=" + md.size());
					for (String st : md.keySet()) {
						SDFSLogger.getLog().debug("key=" + st + " val=" + md.get(st));
					}
					RemoteVolumeInfo info = new RemoteVolumeInfo();
					info.id = EncyptUtils.decHashArchiveName(bi.getName().substring("bucketinfo/".length()),
							Main.chunkStoreEncryptionEnabled);
					info.hostname = md.get("hostname");
					try {
						info.port = Integer.parseInt(md.get("port"));
					} catch (Exception e) {
					}
					info.compressed = Long.parseLong(md.get("compressedlength"));
					info.data = Long.parseLong(md.get("currentlength"));
					info.lastupdated = Long.parseLong(md.get("lastupdated"));
					info.metaData = md;
					al.add(info);
				}
				RemoteVolumeInfo[] ids = new RemoteVolumeInfo[al.size()];
				for (int i = 0; i < al.size(); i++) {
					ids[i] = al.get(i);
				}
				return ids;

			} catch (Exception e) {
				throw new IOException(e);
			}
		} else {
			RemoteVolumeInfo info = new RemoteVolumeInfo();
			info.id = Main.DSEID;
			info.port = Main.sdfsCliPort;
			info.hostname = InetAddress.getLocalHost().getHostName();
			info.compressed = this.compressedSize();
			info.data = this.size();
			RemoteVolumeInfo[] ninfo = { info };
			return ninfo;
		}
	}

	@Override
	public byte[] getBytes(long id, int from, int to) throws IOException, DataArchivedException {
		throw new IOException("funtion not supported");
	}

	@Override
	public int getMetaDataVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void removeVolume(long volumeID) throws IOException {
		if (volumeID == Main.DSEID)
			throw new IOException("volume can not remove its self");
		String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		try {
			CloudBlockBlob cblob = container.getBlockBlobReference(lbi);
			cblob.downloadAttributes();
			HashMap<String, String> md = cblob.getMetadata();
			long dur = System.currentTimeMillis() - Long.parseLong(md.get("lastupdated"));
			if (dur < (60000 * 2)) {
				throw new IOException("Volume [" + volumeID + "] is currently mounted");
			}
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}

		String vid = EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		iter = container.listBlobs("claims/", true).iterator();
		String suffix = "/" + vid;
		String prefix = "claims/";
		while (iter.hasNext()) {
			try {
				CloudBlob bi = (CloudBlob) iter.next();
				String nm = bi.getName();
				if (nm.endsWith(suffix)) {
					bi.delete();
					String fldr = nm.substring(0, nm.length() - suffix.length());
					Iterator<ListBlobItem> _it = container.listBlobs(fldr).iterator();
					if (!_it.hasNext()) {
						String fl = fldr.substring(prefix.length());
						CloudBlockBlob blob = container.getBlockBlobReference(fl);
						blob.delete();

					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		try {
			CloudBlockBlob blob = container.getBlockBlobReference(lbi);
			blob.delete();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void timeStampData(long key) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addRefresh(long id) {
		synchronized (this.refresh) {
			if (Main.REFRESH_BLOBS)
				this.refresh.add(id);
		}

	}

	public static void main(String[] args) {
		String storageConnectionString = "DefaultEndpointsProtocol=http;" + "AccountName=" + args[0] + ";"
				+ "AccountKey=" + args[1];
		try {
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			// Create the blob client.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Get a reference to a container.
			// The container name must be lower case
			CloudBlobContainer container = blobClient.getContainerReference(args[2]);
			CloudBlockBlob kblob = container.getBlockBlobReference("blocks/MTEwMjE4ODc4MjM0MzY3NzM=");
			kblob.downloadAttributes();
			System.out.println(kblob.getProperties().getStandardBlobTier());
			System.out.println(kblob.getProperties().getRehydrationStatus());

		} catch (Exception e) {
			// Output the stack trace.
			e.printStackTrace();
		}

	}

	@Override
	public void setDseSize(long sz) {
		// TODO Auto-generated method stub
	}

	public long getAllObjSummary(String pp, long id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setCredentials(String accessKey, String secretKey) {
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	@Override
	public boolean isStandAlone() {
		return this.standAlone;
	}

	@Override
	public void setStandAlone(boolean standAlone) {
		this.standAlone = standAlone;

	}

	boolean metaStore = true;

	@Override
	public void setMetaStore(boolean metaStore) {
		this.metaStore = metaStore;

	}

	@Override
	public boolean isMetaStore(boolean metaStore) {
		return this.metaStore;
	}

	SecureRandom rand = new SecureRandom();

	private long getLongID() {
		byte[] k = new byte[7];
		rand.nextBytes(k);
		ByteBuffer bk = ByteBuffer.allocate(8);
		byte bid = 0;
		bk.put(bid);
		bk.put(k);
		bk.position(0);
		return bk.getLong();
	}

	@Override
	public long getNewArchiveID() throws IOException {

		long pid = this.getLongID();
		while (pid < 100 && this.fileExists(pid))
			pid = this.getLongID();
		return pid;
	}

}
