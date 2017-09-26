package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;


import org.jclouds.filesystem.reference.FilesystemConstants;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.ws.rs.core.MediaType;

import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;

import com.google.common.base.Supplier;

import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.StringResult;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.payloads.FilePayload;
import org.jclouds.Constants;
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

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

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
public class BatchJCloudChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {

	BlobStoreContext context = null;
	BlobStore blobStore = null;
	String bucketLocation = null;
	private String name;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean clustered = true;
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	private int checkInterval = 15000;
	private static final int version = 1;
	private boolean azureStore = false;
	private boolean accessStore = false;
	private boolean b2Store = false;
	private boolean atmosStore = false;
	private final static String mdExt = ".6442";
	private GenericObjectPool<BlobStore> bPool;

	// private String bucketLocation = null;
	static {

	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
		return false;
	}

	public BatchJCloudChunkStore() {

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
			SDFSLogger.getLog().info("############ Closing Container ##################");
			// container = pool.borrowObject();
			HashBlobArchive.close();

			Map<String, String> md = this.getMetaData(
					"bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
			md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
			md.put("hostname", InetAddress.getLocalHost().getHostName());
			if (Main.volume != null) {
				md.put("port", Integer.toString(Main.sdfsCliPort));
			}
			md.put("bucketversion", Integer.toString(version));
			md.put("sdfsversion", Main.version);
			md.put("lastupdated", Long.toString(System.currentTimeMillis()));
			this.updateObject(
					"bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled), md);

			this.context.close();
			SDFSLogger.getLog().info("Updated container on close");
			SDFSLogger.getLog().info("############ Container Closed ##################");
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

	private Map<String, String> getMetaData(String obj) throws IOException {
		if (this.accessStore || this.atmosStore || b2Store) {
			if (blobStore.blobExists(this.name, obj + mdExt)) {
				if (this.b2Store) {
					BlobStore st = null;
					try {
						st = bPool.borrowObject();

						Blob blob = st.getBlob(this.name, obj + mdExt);
						ObjectInputStream in = new ObjectInputStream(blob.getPayload().openStream());
						@SuppressWarnings("unchecked")
						Map<String, String> md = (Map<String, String>) in.readObject();
						blob.getPayload().release();
						IOUtils.closeQuietly(blob.getPayload());
						return md;
					} catch (Exception e) {
						// SDFSLogger.getLog().error("unable to borrow
						// object",e);
						throw new IOException(e);
					} finally {
						if (st != null)
							bPool.returnObject(st);
					}
				} else {
					Blob blob = blobStore.getBlob(this.name, obj + mdExt);

					ObjectInputStream in = new ObjectInputStream(blob.getPayload().openStream());
					try {
						@SuppressWarnings("unchecked")
						Map<String, String> md = (Map<String, String>) in.readObject();
						blob.getPayload().release();
						IOUtils.closeQuietly(blob.getPayload());
						return md;
					} catch (ClassNotFoundException e) {
						throw new IOException(e);
					}
				}
			} else {
				return new HashMap<String, String>();
			}
		} else {
			BlobMetadata omd = blobStore.blobMetadata(this.name, obj);

			Map<String, String> md = new HashMap<String, String>();
			if (omd != null) {
				Map<String, String> zk = omd.getUserMetadata();
				for (String key : zk.keySet()) {
					md.put(key.toLowerCase(), zk.get(key));
				}
			}
			return md;

		}
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return HashBlobArchive.currentLength.get();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len) throws IOException {
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

	public void deleteBucket() throws IOException {
		try {
			blobStore.deleteContainer(this.name);
		} finally {
			// pool.returnObject(container);
		}
		this.close();
	}

	/*
	 * @SuppressWarnings("deprecation") private void resetCurrentSize() throws
	 * IOException { PageSet<? extends StorageMetadata> bul = null; if
	 * (this.atmosStore) bul = blobStore.list(this.name,
	 * ListContainerOptions.Builder.inDirectory("bucketinfo/")); else
	 * if(this.b2Store) bul = blobStore.list(this.name,
	 * ListContainerOptions.Builder.prefix("bucketinfo/").maxResults(100)); else bul
	 * = blobStore.list(this.name,
	 * ListContainerOptions.Builder.prefix("bucketinfo/")); long rcs = 0; long rccs
	 * = 0; String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID,
	 * Main.chunkStoreEncryptionEnabled); for (StorageMetadata st : bul) { if
	 * (!st.getName().equalsIgnoreCase(lbi)) { Map<String, String> md =
	 * this.getMetaData(lbi); try { rcs += Long.parseLong(md.get("currentlength"));
	 * rccs += Long.parseLong(md.get("compressedlength")); } catch (Exception e) {
	 * 
	 * } } } this.rcurrentCompressedSize.set(rccs); this.rcurrentSize.set(rcs); }
	 */

	@Override
	public void init(Element config) throws IOException {
		SDFSLogger.getLog().info("Accessing JCloud bucket " + Main.cloudBucket.toLowerCase());
		this.name = Main.cloudBucket.toLowerCase();
		this.staged_sync_location.mkdirs();
		if (config.hasAttribute("default-bucket-location")) {
			bucketLocation = config.getAttribute("default-bucket-location");
		}
		if (config.hasAttribute("block-size")) {
			int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
			HashBlobArchive.MAX_LEN = sz;
		}
		if (config.hasAttribute("sync-files")) {
			boolean syncf = Boolean.parseBoolean(config.getAttribute("sync-files"));
			if (syncf) {
				new FileReplicationService(this);
			}
		}
		if (config.hasAttribute("connection-check-interval")) {
			this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
		}
		if (config.hasAttribute("delete-unclaimed")) {
			this.deleteUnclaimed = Boolean.parseBoolean(config.getAttribute("delete-unclaimed"));
		}
		if (config.hasAttribute("upload-thread-sleep-time")) {
			int tm = Integer.parseInt(config.getAttribute("upload-thread-sleep-time"));
			HashBlobArchive.THREAD_SLEEP_TIME = tm;
		}
		if (config.hasAttribute("local-cache-size")) {
			long sz = StringUtils.parseSize(config.getAttribute("local-cache-size"));
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
		Properties overrides = new Properties();
		if (config.getElementsByTagName("connection-props").getLength() > 0) {
			Element el = (Element) config.getElementsByTagName("connection-props").item(0);
			NamedNodeMap ls = el.getAttributes();
			for (int i = 0; i < ls.getLength(); i++) {
				overrides.put(ls.item(i).getNodeName(), ls.item(i).getNodeValue());
				SDFSLogger.getLog()
						.debug("set connection value" + ls.item(i).getNodeName() + " to " + ls.item(i).getNodeValue());
			}
		}
		try {
			String service = config.getAttribute("service-type");
			if (service.equalsIgnoreCase("azureblob"))
				this.azureStore = true;
			if (service.equalsIgnoreCase("atmos")) {
				EncyptUtils.baseEncode = true;
				this.atmosStore = true;
			}
			if (service.equalsIgnoreCase("b2")) {
				EncyptUtils.baseEncode = true;
				this.b2Store = true;
			}
			String userAgent = "SDFS/" + Main.version;
			if (config.hasAttribute("user-agent-prefix"))
				userAgent = config.getAttribute("user-agent-prefix") + " " + userAgent;
			if (service.equalsIgnoreCase("b2"))
				overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "60000");
			else
				overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "5000");
			
			overrides.setProperty(Constants.PROPERTY_USER_THREADS, "0");
			overrides.setProperty(Constants.PROPERTY_USER_AGENT, userAgent);
			overrides.setProperty(Constants.PROPERTY_MAX_CONNECTIONS_PER_CONTEXT,
					Integer.toString(Main.dseIOThreads * 2));
			overrides.setProperty(Constants.PROPERTY_MAX_CONNECTIONS_PER_HOST, 0 + "");
			// Keep retrying indefinitely
			overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, "10");
			// Do not wait between retries
			overrides.setProperty(Constants.PROPERTY_RETRY_DELAY_START, "0");
			Location region = null;
			if (service.equals("google-cloud-storage") && config.hasAttribute("auth-file")) {
				InputStream is = new FileInputStream(config.getAttribute("auth-file"));
				String creds = org.apache.commons.io.IOUtils.toString(is, "UTF-8");
				org.apache.commons.io.IOUtils.closeQuietly(is);
				Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(creds);
				context = ContextBuilder.newBuilder(service).overrides(overrides)
						.credentialsSupplier(credentialSupplier).buildView(BlobStoreContext.class);

			} else if (service.equals("google-cloud-storage")) {
				overrides.setProperty(Constants.PROPERTY_ENDPOINT, "https://storage.googleapis.com");
				overrides.setProperty(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
				overrides.setProperty(Constants.PROPERTY_STRIP_EXPECT_HEADER, "true");
				context = ContextBuilder.newBuilder("s3").overrides(overrides)
						.credentials(Main.cloudAccessKey, Main.cloudSecretKey).buildView(BlobStoreContext.class);
			} else if (service.equals("filesystem")) {
				EncyptUtils.baseEncode = true;
				SDFSLogger.getLog().info("share-path=" + config.getAttribute("share-path"));
				overrides.setProperty(FilesystemConstants.PROPERTY_BASEDIR, config.getAttribute("share-path"));
				context = ContextBuilder.newBuilder("filesystem").overrides(overrides)
						.buildView(BlobStoreContext.class);
				this.accessStore = true;
			} else {
				SDFSLogger.getLog().debug("ca=" + Main.cloudAccessKey + " cs=" + Main.cloudSecretKey);
				context = ContextBuilder.newBuilder(service).credentials(Main.cloudAccessKey, Main.cloudSecretKey)
						.overrides(overrides).buildView(BlobStoreContext.class);
			}
			blobStore = context.getBlobStore();
			if (this.b2Store) {
				GenericObjectPoolConfig bconfig = new GenericObjectPoolConfig();
				bconfig.setMaxIdle(1);
				bconfig.setMaxTotal(Main.dseIOThreads * 2);
				bconfig.setTestOnBorrow(false);
				bconfig.setTestOnReturn(false);
				GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
				cfg.setMinIdle(Main.dseIOThreads);
				this.bPool = new GenericObjectPool<BlobStore>(
						new B2ConnectionFactory(Main.cloudAccessKey, Main.cloudSecretKey, overrides));
			}

			if (!blobStore.containerExists(this.name))
				blobStore.createContainerInLocation(region, this.name);
			/*
			 * serviceClient.getDefaultRequestOptions().setTimeoutIntervalInMs( 10 * 1000);
			 * 
			 * serviceClient.getDefaultRequestOptions().setRetryPolicyFactory( new
			 * RetryExponentialRetry(500, 5));
			 */

			String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			Map<String, String> md = new HashMap<String, String>();
			if (blobStore.blobExists(this.name, lbi)) {
				md = this.getMetaData(lbi);
			}
			if (md.size() == 0)
				this.clustered = true;
			else if (md.containsKey("clustered"))
				this.clustered = Boolean.parseBoolean(md.get("clustered"));
			else
				this.clustered = false;
			md.put("clustered", Boolean.toString(clustered));
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
			if (cl == 0 || sz == 0) {
				md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
				md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
				md.put("clustered", Boolean.toString(this.clustered));
				md.put("hostname", InetAddress.getLocalHost().getHostName());
				if (Main.volume != null) {
					md.put("port", Integer.toString(Main.sdfsCliPort));
				}
				md.put("bucketversion", Integer.toString(version));
				md.put("lastupdated", Long.toString(System.currentTimeMillis()));
				md.put("sdfsversion", Main.version);

				if (this.accessStore || this.atmosStore || b2Store)
					this.updateObject("bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled), md);
				Blob b = blobStore
						.blobBuilder("bucketinfo/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled))
						.payload(Long.toString(System.currentTimeMillis())).build();
				this.writeBlob(b, false);
			} else {
				Blob b = blobStore
						.blobBuilder("bucketinfo/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled))
						.payload(Long.toString(System.currentTimeMillis())).userMetadata(md).build();
				if (this.accessStore || this.atmosStore || b2Store) {
					b = blobStore
							.blobBuilder("bucketinfo/"
									+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled))
							.payload(Long.toString(System.currentTimeMillis())).build();
					this.updateObject("bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled), md);
				}
				this.writeBlob(b, false);
			}
			HashBlobArchive.currentLength.set(sz);
			HashBlobArchive.compressedLength.set(cl);
			// this.resetCurrentSize();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}
		Thread thread = new Thread(this);
		thread.start();
		HashBlobArchive.init(this);
		HashBlobArchive.setReadSpeed(rsp, false);
		HashBlobArchive.setWriteSpeed(wsp, false);
	}

	Iterator<? extends StorageMetadata> iter = null;
	PageSet<? extends StorageMetadata> ips = null;
	MultiDownload dl = null;

	@SuppressWarnings("deprecation")
	@Override

	public void iterationInit(boolean deep) throws IOException {
		this.hid = 0;
		this.ht = null;

		if (this.atmosStore)
			ips = blobStore.list(this.name, ListContainerOptions.Builder.inDirectory("keys/"));
		else if (this.b2Store)
			ips = blobStore.list(this.name, ListContainerOptions.Builder.prefix("keys/").maxResults(100));
		else
			ips = blobStore.list(this.name, ListContainerOptions.Builder.prefix("keys/"));
		// SDFSLogger.getLog().info("llsz=" + ips.size());
		try {
			String lbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			// BlobMetadata dmd = blobStore.blobMetadata(this.name,
			// lbi);
			Map<String, String> md = this.getMetaData(lbi);
			md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
			md.put("clustered", Boolean.toString(this.clustered));
			md.put("hostname", InetAddress.getLocalHost().getHostName());
			md.put("lastupdated", Long.toString(System.currentTimeMillis()));
			md.put("bucketversion", Integer.toString(version));
			md.put("sdfsversion", Main.version);
			if (Main.volume != null) {
				md.put("port", Integer.toString(Main.sdfsCliPort));
			}

			Blob b = blobStore
					.blobBuilder(
							"bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled)
									+ "-" + System.currentTimeMillis())
					.payload(Long.toString(System.currentTimeMillis())).userMetadata(md).build();
			this.writeBlob(b, false);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to backu config", e);
		}
		iter = ips.iterator();
		HashBlobArchive.currentLength.set(0);
		HashBlobArchive.compressedLength.set(0);
		dl = new MultiDownload(this, "keys/");
		dl.iterationInit(false, "keys/");
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
		return HashBlobArchive.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			return blobStore.blobExists(this.name, "blocks/" + haName);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}

	private String[] getStrings(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		Map<String, String> md = this.getMetaData("keys/" + haName);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		this.readBlob("keys/" + haName, out);

		byte[] nm = out.toByteArray();

		boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
		if (encrypt) {
			nm = EncryptUtils.decryptCBC(nm);
		}
		boolean compress = Boolean.parseBoolean(md.get("lz4compress"));
		if (compress) {
			int size = Integer.parseInt(md.get("size"));
			nm = CompressionUtils.decompressLz4(nm, size);
		}
		String st = new String(nm);
		SDFSLogger.getLog().debug("string=" + st);
		return st.split(",");

	}

	private String getClaimName(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		return "claims/keys/" + haName + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
	}

	private int getClaimedObjects(String pth) throws IOException {
		try {
			long id = EncyptUtils.decHashArchiveName(pth.substring(5), Main.chunkStoreEncryptionEnabled);
			String[] hs = this.getStrings(id);

			int claims = 0;
			for (String ha : hs) {
				byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
				if (HCServiceProxy.getHashesMap().mightContainKey(b))
					claims++;
			}
			return claims;
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	private static final int MPSZ = 40 * 1024 * 1024; // 20 MB

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		IOException e = null;
		for (int i = 0; i < 9; i++) {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

			try {
				// container = pool.borrowObject();
				byte[] f = arc.getBytes();
				HashMap<String, String> metaData = new HashMap<String, String>();
				metaData.put("size", Integer.toString(arc.uncompressedLength.get()));
				if (Main.compress) {
					metaData.put("lz4compress", "true");
				} else {
					metaData.put("lz4compress", "false");
				}
				long csz = f.length;
				if (Main.chunkStoreEncryptionEnabled) {
					metaData.put("encrypt", "true");
				} else {
					metaData.put("encrypt", "false");
				}
				metaData.put("compressedsize", Long.toString(csz));
				metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
				metaData.put("objects", Integer.toString(arc.getSz()));
				HashCode hc = Hashing.md5().hashBytes(f);
				metaData.put("md5sum", BaseEncoding.base64().encode(hc.asBytes()));
				Blob blob = null;
				if (this.accessStore || this.atmosStore || b2Store)
					blob = blobStore.blobBuilder("blocks/" + haName).payload(f).contentLength(csz)
							.contentType(MediaType.APPLICATION_OCTET_STREAM).build();
				else
					blob = blobStore.blobBuilder("blocks/" + haName).userMetadata(metaData).payload(f)
							.contentLength(csz).contentType(MediaType.APPLICATION_OCTET_STREAM).build();

				if (csz >= MPSZ)
					this.writeBlob(blob, true);
				else
					this.writeBlob(blob, false);
				SDFSLogger.getLog().debug("uploaded blocks/" + haName);
				if (this.accessStore || this.atmosStore || b2Store)
					this.updateObject("blocks/" + haName, metaData);
				// upload the metadata
				String st = arc.getHashesString();
				byte[] chunks = st.getBytes();
				metaData = new HashMap<String, String>();
				// metaData = new HashMap<String, String>();
				int ssz = chunks.length;
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
				metaData.put("bcompressedsize", Long.toString(csz));
				metaData.put("objects", Integer.toString(arc.getSz()));
				if (this.accessStore || this.atmosStore || b2Store)
					blob = blobStore.blobBuilder("keys/" + haName).payload(chunks).contentLength(chunks.length)
							.contentType(MediaType.APPLICATION_OCTET_STREAM).build();
				else
					blob = blobStore.blobBuilder("keys/" + haName).userMetadata(metaData).payload(chunks)
							.contentLength(chunks.length).contentType(MediaType.APPLICATION_OCTET_STREAM).build();
				if (ssz >= MPSZ)
					this.writeBlob(blob, true);
				else
					this.writeBlob(blob, false);

				blob = blobStore.blobBuilder(this.getClaimName(id)).userMetadata(metaData)
						.payload(Long.toString(System.currentTimeMillis()))
						.contentType(MediaType.APPLICATION_OCTET_STREAM).build();
				SDFSLogger.getLog().debug("uploaded " + this.getClaimName(id));
				if (this.accessStore || this.atmosStore || b2Store) {
					blob = blobStore.blobBuilder(this.getClaimName(id))
							.payload(Long.toString(System.currentTimeMillis()))
							.contentType(MediaType.APPLICATION_OCTET_STREAM).build();
				}
				this.writeBlob(blob, false);
				if (this.accessStore || this.atmosStore || b2Store)
					this.updateObject(this.getClaimName(id), metaData);
				return;
			} catch (Throwable e1) {
				SDFSLogger.getLog().debug("unable to write archive " + arc.getID() + " with id " + id, e1);
				e = new IOException(e1);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e2) {

				}
			} finally {
				// pool.returnObject(container);
			}
		}
		if (e != null) {
			SDFSLogger.getLog().error("unable to write block", e);
			throw e;
		}
	}

	private void writeBlob(Blob blob, boolean mp) throws IOException {
		if (this.azureStore) {
			if (mp)
				blobStore.putBlob(this.name, blob, multipart());
			else
				blobStore.putBlob(this.name, blob);

		} else if (this.b2Store) {
			BlobStore st = null;
			try {
				st = bPool.borrowObject();
				if (mp) {
					st.putBlob(this.name, blob, multipart());
				}else {
					st.putBlob(this.name, blob);
				}
				
			} catch (java.lang.IllegalArgumentException e) {
				SDFSLogger.getLog().error("unable to borrow object", e);
				if (e.getMessage().startsWith("large files must have at least"))
					writeBlob(blob, false);
				else
					SDFSLogger.getLog().error("unable to borrow object", e);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to borrow object", e);
				throw new IOException(e);
			} finally {
				if (st != null)
					bPool.returnObject(st);
			}

		} else {
			blobStore.putBlob(this.name, blob);
		}
	}

	private void readBlob(String key, OutputStream os) throws IOException {

		if (this.b2Store) {
			BlobStore st = null;
			try {
				st = bPool.borrowObject();
				Blob blob = null;
				try {
					blob = st.getBlob(this.name, key);
					BufferedInputStream in = new BufferedInputStream(blob.getPayload().openStream());
					IOUtils.copy(in, os);
					// SDFSLogger.getLog().info("read " +key + " br=" + br);
					os.flush();
				} finally {
					IOUtils.closeQuietly(os);
					if (blob != null) {
						blob.getPayload().release();
						IOUtils.closeQuietly(blob.getPayload());
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to borrow object", e);
				throw new IOException(e);
			} finally {
				if (st != null)
					bPool.returnObject(st);
			}

		} else {
			Blob blob = null;
			try {
				blob = blobStore.getBlob(this.name, key);
				IOUtils.copy(blob.getPayload().openStream(), os);
				os.flush();
			} finally {
				IOUtils.closeQuietly(os);
				if (blob != null) {
					blob.getPayload().release();
					IOUtils.closeQuietly(blob.getPayload());
				}
			}
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException {
		try {
			Exception e = null;
			FileOutputStream out = null;
			Map<String, String> metaData = null;
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			for (int i = 0; i < 10; i++) {
				try {
					if(f.exists()&& !f.delete()) {
						SDFSLogger.getLog().warn("file already exists! " + f.getPath());
						File nf = new File(f.getPath() + " " + ".old");
						Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
					}
					metaData = this.getMetaData("blocks/" + haName);
					out = new FileOutputStream(f);
					this.readBlob("blocks/" + haName, out);
					e = null;
					break;
				} catch (java.io.FileNotFoundException e1) {
					try {
						Thread.sleep(5000);
					} catch (Exception e2) {

					}
					IOUtils.closeQuietly(out);
					f.delete();
					e = e1;
				} catch (Exception e1) {
					try {
						Thread.sleep(5000);
					} catch (Exception e2) {

					}
					IOUtils.closeQuietly(out);
					f.delete();
					e = e1;
				}
			}
			if (e != null) {
				SDFSLogger.getLog().error("getnewblob unable to get block", e);
				throw new IOException(e);
			}

			if (metaData.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean(metaData.get("deleted"));
				if (del) {
					// BlobMetadata kmd = blobStore.blobMetadata(this.name,
					// "keys/" + haName);

					Map<String, String> kmetaData = this.getMetaData("keys/" + haName);
					int claims = this.getClaimedObjects("keys/" + haName);
					int delobj = 0;
					if (kmetaData.containsKey("deletedobjects")) {
						delobj = Integer.parseInt(kmetaData.get("deletedobjects")) - claims;
						if (delobj < 0)
							delobj = 0;
					}
					kmetaData.remove("deleted");
					kmetaData.put("deletedobjects", Integer.toString(delobj));
					kmetaData.put("suspect", "true");
					// int _size = Integer.parseInt((String)
					// metaData.get("size"));
					// int _compressedSize = Integer.parseInt((String)
					// metaData.get("compressedsize"));
					// HashBlobArchive.currentLength.addAndGet(_size);
					// HashBlobArchive.compressedLength.addAndGet(_compressedSize);

					this.updateObject("keys/" + haName, kmetaData);
					metaData.remove("deleted");
					metaData.put("deletedobjects", Integer.toString(delobj));
					metaData.put("suspect", "true");
					this.updateObject("blocks/" + haName, metaData);
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch block [" + id + "]", e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	private void removeMetaData(String name) {
		if (this.accessStore || this.atmosStore || b2Store) {
			blobStore.removeBlob(this.name, name + mdExt);
		}
	}

	@SuppressWarnings("deprecation")
	private int verifyDelete(long id) throws Exception {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

		Map<String, String> metaData = null;
		if (clustered)
			metaData = this.getMetaData(this.getClaimName(id));
		else
			metaData = this.getMetaData("keys/" + haName);
		int claims = this.getClaimedObjects("keys/" + haName);
		if (claims == 0) {
			SDFSLogger.getLog().debug("doing Delete " + "keys/" + haName);
			if (clustered) {
				BlobStore bk = this.blobStore;
				if (this.b2Store) {
					bk = bPool.borrowObject();
				}
				try {
					String pth = this.getClaimName(id);
					bk.removeBlob(this.name, pth);
					removeMetaData(pth);

					SDFSLogger.getLog().debug("cheching Delete " + pth);
					PageSet<? extends StorageMetadata> _ps = null;

					if (this.atmosStore)
						_ps = bk.list(this.name, ListContainerOptions.Builder.inDirectory("claims/keys/" + haName));
					else if (this.b2Store) {
						_ps = bk.list(this.name,
								ListContainerOptions.Builder.prefix("claims/keys/" + haName).maxResults(100));
					} else
						_ps = bk.list(this.name, ListContainerOptions.Builder.prefix("claims/keys/" + haName));
					if (_ps.size() == 0 || this.atmosStore) {
						metaData = this.getMetaData("blocks/" + haName);
						bk.removeBlob(this.name, "keys/" + haName);
						removeMetaData("keys/" + haName);
						SDFSLogger.getLog().debug("Deleting " + "keys/" + haName);
						bk.removeBlob(this.name, "blocks/" + haName);
						removeMetaData("blocks/" + haName);
						SDFSLogger.getLog().debug("Deleting " + "blocks/" + haName + " size=" + metaData.get("size")
								+ " compressed size=" + metaData.get("compressedsize"));

						int _size = Integer.parseInt((String) metaData.get("size"));
						int _compressedSize = Integer.parseInt((String) metaData.get("compressedsize"));
						HashBlobArchive.currentLength.addAndGet(-1 * _size);
						HashBlobArchive.compressedLength.addAndGet(-1 * _compressedSize);
						if (HashBlobArchive.currentLength.get() < 0)
							HashBlobArchive.currentLength.set(0);
						if (HashBlobArchive.compressedLength.get() < 0) {
							HashBlobArchive.compressedLength.set(0);
						}
						SDFSLogger.getLog().debug("Current DSE Size  size=" + HashBlobArchive.currentLength.get()
								+ " compressed size=" + HashBlobArchive.compressedLength.get());
					} else {
						SDFSLogger.getLog().debug("Not deleting becuase still claimed by " + _ps.size());
						Iterator<? extends StorageMetadata> _di = _ps.iterator();
						while (_di.hasNext()) {
							SDFSLogger.getLog().debug("claimed by " + _di.next().getName());
						}

					}

				} finally {
					if (this.b2Store && bk != null) {
						bPool.returnObject(bk);
					}
				}

			} else {
				blobStore.removeBlob(this.name, "keys/" + haName);
				removeMetaData("keys/" + haName);
				blobStore.removeBlob(this.name, "blocks/" + haName);
				removeMetaData("blocks/" + haName);
			}
		}

		return claims;
	}

	@Override
	public void clearCounters() {
		HashBlobArchive.compressedLength.set(0);
		HashBlobArchive.currentLength.set(0);
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {
					String lbi = "bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					// BlobMetadata dmd = blobStore.blobMetadata(this.name,
					// lbi);
					Map<String, String> md = this.getMetaData(lbi);
					md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
					md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
					md.put("clustered", Boolean.toString(this.clustered));
					md.put("hostname", InetAddress.getLocalHost().getHostName());
					md.put("lastupdated", Long.toString(System.currentTimeMillis()));
					md.put("bucketversion", Integer.toString(version));
					md.put("sdfsversion", Main.version);
					if (Main.volume != null) {
						md.put("port", Integer.toString(Main.sdfsCliPort));
					}
					try {
						this.updateObject(lbi, md);
					} catch (Exception e) {
						Blob b = blobStore
								.blobBuilder("bucketinfo/"
										+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled))
								.payload(Long.toString(System.currentTimeMillis())).userMetadata(md).build();
						this.writeBlob(b, false);

					}
					// this.resetCurrentSize();
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to update size", e);
				}
				if (this.deletes.size() > 0) {
					SDFSLogger.getLog().debug("running garbage collection for " + this.deletes.size());
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
					BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
					ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Main.dseIOThreads, 10, TimeUnit.SECONDS,
							worksQueue, new ThreadPoolExecutor.CallerRunsPolicy());
					for (Long k : iter) {

						String hashString = EncyptUtils.encHashArchiveName(k.longValue(),
								Main.chunkStoreEncryptionEnabled);
						try {

							HashBlobArchive.removeCache(k.longValue());
							if (this.deleteUnclaimed) {
								DeleteObject obj = new DeleteObject(k.longValue(), this);
								executor.execute(obj);
							} else {
								// BlobMetadata dmd =
								// blobStore.blobMetadata(this.name,
								// this.getClaimName(k));
								Map<String, String> metaData = this.getMetaData(this.getClaimName(k));
								// SDFSLogger.getLog().info("remove requests for
								// " +
								// hashString + "=" + odel.get(k));
								int delobj = 0;
								if (metaData.containsKey("deletedobjects"))
									delobj = Integer.parseInt((String) metaData.get("deletedobjects"));
								// SDFSLogger.getLog().info("remove requests for
								// " +
								// hashString + "=" + odel.get(k));
								delobj = delobj + odel.get(k);
								// SDFSLogger.getLog().info("deleting " +
								// hashString);
								metaData.put("deleted", "true");
								metaData.put("deletedobjects", Integer.toString(delobj));
								if (this.atmosStore || this.accessStore || b2Store) {
									this.updateObject(this.getClaimName(k), metaData);
								} else {
									// BlobMetadata dmd =
									// blobStore.blobMetadata(this.name,
									// this.getClaimName(k));
									this.updateObject(this.getClaimName(k), metaData);
								}
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
					executor.shutdown();
					while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
						SDFSLogger.getLog().debug("Awaiting deletion task completion of threads.");
					}
					SDFSLogger.getLog().info("done running garbage collection");

				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				SDFSLogger.getLog().error("error in delete thread", e);
			}
		}
	}

	private void updateObject(String id, Map<String, String> md) throws IOException {
		if (accessStore || this.atmosStore || b2Store) {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(bo);
			o.writeObject(md);
			if (blobStore.blobExists(name, id + mdExt)) {
				blobStore.removeBlob(name, id + mdExt + ".ck");
				blobStore.removeBlob(name, id + mdExt + ".ck");
				Blob b = blobStore.blobBuilder(id + mdExt + ".ck").payload(bo.toByteArray()).build();
				this.writeBlob(b, false);
				blobStore.removeBlob(name, id + mdExt);
				b = blobStore.blobBuilder(id + mdExt).payload(bo.toByteArray()).build();
				this.writeBlob(b, false);
				blobStore.removeBlob(name, id + mdExt + ".ck");
			} else {
				Blob b = blobStore.blobBuilder(id + mdExt).payload(bo.toByteArray()).build();
				this.writeBlob(b, false);
			}
		} else {
			try {
				ContentMetadata dmd = blobStore.blobMetadata(this.name, id).getContentMetadata();
				blobStore.copyBlob(this.name, id, this.name, id,
						CopyOptions.builder().contentMetadata(dmd).userMetadata(md).build());
			} catch (Exception e) {
				ContentMetadata dmd = blobStore.blobMetadata(this.name, id).getContentMetadata();
				blobStore.copyBlob(this.name, id, this.name, id + ".ck",
						CopyOptions.builder().contentMetadata(dmd).userMetadata(md).build());
				blobStore.removeBlob(name, id);
				blobStore.copyBlob(this.name, id + ".ck", this.name, id,
						CopyOptions.builder().contentMetadata(dmd).userMetadata(md).build());
				blobStore.removeBlob(name, id + ".ck");
			}
		}
	}

	@Override
	public void sync() throws IOException {
		HashBlobArchive.sync();

	}

	@Override
	public void uploadFile(File f, String to, String pp) throws IOException {
		IOException e2 = null;
		for (int i = 0; i < 10; i++) {
			try {
				BufferedInputStream in = null;
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
						HashMap<String, String> metaData = new HashMap<String, String>();
						metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
						metaData.put("lastmodified", Long.toString(f.lastModified()));
						String slp = EncyptUtils.encString(Files.readSymbolicLink(f.toPath()).toFile().getPath(),
								Main.chunkStoreEncryptionEnabled);
						metaData.put("symlink", slp);
						Blob b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length())
								.userMetadata(metaData).build();
						if (this.accessStore || this.atmosStore || b2Store) {
							this.updateObject(pth, metaData);
							b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length()).build();
						}
						this.writeBlob(b, false);
						if (this.accessStore || this.atmosStore || b2Store)
							this.updateObject(pth, metaData);
						this.checkoutFile(pth);
					} catch (Exception e1) {
						throw new IOException(e1);
					}
				} else if (isDir) {
					try {
						HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
								Main.chunkStoreEncryptionEnabled);
						metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
						metaData.put("lastmodified", Long.toString(f.lastModified()));
						metaData.put("directory", "true");
						Blob b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length())
								.userMetadata(metaData).build();

						if (this.accessStore || this.atmosStore || b2Store) {
							this.updateObject(pth, metaData);
							b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length()).build();
						}
						this.writeBlob(b, false);
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
						BufferedInputStream is = new BufferedInputStream(new FileInputStream(f));
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
						while (to.startsWith(File.separator))
							to = to.substring(1);
						FilePayload fp = new FilePayload(p);
						HashCode hc = com.google.common.io.Files.hash(p, Hashing.md5());
						HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
								Main.chunkStoreEncryptionEnabled);
						metaData.put("lz4compress", Boolean.toString(Main.compress));
						metaData.put("md5sum", BaseEncoding.base64().encode(hc.asBytes()));
						metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
						metaData.put("lastmodified", Long.toString(f.lastModified()));
						Blob b = blobStore.blobBuilder(pth).payload(fp).contentLength(p.length()).userMetadata(metaData)
								.build();

						if (this.accessStore || this.atmosStore || b2Store) {
							this.updateObject(pth, metaData);
							b = blobStore.blobBuilder(pth).payload(fp).contentLength(p.length()).build();
						}
						if (f.length() >= MPSZ)
							this.writeBlob(b, true);
						else
							this.writeBlob(b, false);
						this.checkoutFile(pth);
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
			} catch (Exception e1) {
				e2 = new IOException(e1);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e3) {

				}
			}
		}
		if (e2 != null)
			throw e2;

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
			// HashCode md5 = md.getContentMetadata().getContentMD5AsHashCode();
			BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(p));
			this.readBlob(fn, os);

			Map<String, String> metaData = this.getMetaData(fn);
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
				os = new BufferedOutputStream(new FileOutputStream(to));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				FileUtils.setFileMetaData(to, metaData, encrypt);
			}
			this.checkoutFile(fn);
		} catch (Exception e1) {
			throw new IOException(e1);
		} finally {
			p.delete();
			z.delete();
			e.delete();
		}

	}

	@SuppressWarnings("deprecation")
	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);

		String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
		try {
			if (this.clustered) {
				String blb = "claims/" + haName + "/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				blobStore.removeBlob(this.name, blb);
				removeMetaData(blb);
				SDFSLogger.getLog().debug("deleting " + blb);
				PageSet<? extends StorageMetadata> _ps = null;
				if (this.atmosStore)
					_ps = blobStore.list(this.name, ListContainerOptions.Builder.inDirectory("claims/" + haName));
				else if (this.b2Store)
					_ps = blobStore.list(this.name,
							ListContainerOptions.Builder.prefix("claims/" + haName).maxResults(100));
				else
					_ps = blobStore.list(this.name, ListContainerOptions.Builder.prefix("claims/" + haName));
				if (_ps.size() == 0 || this.atmosStore) {
					blobStore.removeBlob(this.name, haName);
					removeMetaData(haName);
					SDFSLogger.getLog().debug("deleting " + haName);
				}
			} else {
				blobStore.removeBlob(this.name, haName);
				removeMetaData(haName);
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
			blobStore.copyBlob(this.name, fn, this.name, tn,
					CopyOptions.builder().userMetadata(this.getMetaData(fn)).build());
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	PageSet<? extends StorageMetadata> ps = null;
	Iterator<? extends StorageMetadata> di = null;

	public void clearIter() {
		di = null;
		ps = null;
	}

	@SuppressWarnings("deprecation")
	public String getNextName(String pp, long id) throws IOException {
		while (pp.startsWith("/"))
			pp = pp.substring(1);
		if (!pp.endsWith("/"))
			pp += "/";
		String pfx = pp;

		if (di == null) {
			if (this.atmosStore)
				ps = blobStore.list(this.name, ListContainerOptions.Builder.recursive().inDirectory(pp));
			else if (this.b2Store)
				ps = blobStore.list(this.name, ListContainerOptions.Builder.recursive().prefix(pp).maxResults(100));
			else
				ps = blobStore.list(this.name, ListContainerOptions.Builder.recursive().prefix(pp));
			// SDFSLogger.getLog().info("lsize=" + ps.size() + "pp=" + pp);
			di = ps.iterator();
		}
		if (!di.hasNext()) {
			if (ps.getNextMarker() == null) {
				di = null;
				ps = null;
				return null;
			} else {
				if (this.atmosStore)
					ps = blobStore.list(this.name,
							ListContainerOptions.Builder.recursive().afterMarker(ps.getNextMarker()).inDirectory(pp));
				else if (this.b2Store)
					ps = blobStore.list(this.name, ListContainerOptions.Builder.recursive()
							.afterMarker(ps.getNextMarker()).prefix(pp).maxResults(100));
				else
					ps = blobStore.list(this.name,
							ListContainerOptions.Builder.recursive().afterMarker(ps.getNextMarker()).prefix(pp));
				di = ps.iterator();
			}
		}
		while (di.hasNext()) {
			StorageMetadata bi = di.next();
			try {
				// SDFSLogger.getLog().info("name=" + bi.getName());
				if (!bi.getName().endsWith(mdExt) && !bi.getName().endsWith("/")) {

					Map<String, String> md = this.getMetaData(bi.getName());
					boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
					String fname = EncyptUtils.decString(bi.getName().substring(pfx.length()), encrypt);
					return fname;
				}
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
		String _k = "";
		try {
			String[] ks = this.getStrings(id);
			HashMap<String, Long> m = new HashMap<String, Long>(ks.length);
			for (String k : ks) {
				_k = k;
				String[] kv = k.split(":");
				m.put(kv[0], Long.parseLong(kv[1]));
			}
			return m;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error in string " + _k);
			throw new IOException(e);
		}
	}

	@Override
	public boolean checkAccess() {
		Exception e = null;
		for (int i = 0; i < 10; i++) {
			try {
				String lbi = "bucketinfo/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				Map<String, String> md = this.getMetaData(lbi);
				if (md.containsKey("currentlength")) {
					Long.parseLong(md.get("currentlength"));
					return true;
				}
			} catch (Exception _e) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {

				}
				e = _e;
				SDFSLogger.getLog().debug("unable to connect to bucket try " + i + " of 3", e);
			}
		}
		if (e != null && !this.atmosStore)
			SDFSLogger.getLog().warn("unable to connect to bucket try " + 3 + " of 3", e);
		return true;
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
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
		try {
			String service = props.getProperty("service-type");
			props.remove("service-type");
			if (service.equals("google-cloud-storage") && props.containsKey("auth-file")) {
				InputStream is = new FileInputStream(props.getProperty("auth-file"));
				String creds = org.apache.commons.io.IOUtils.toString(is, "UTF-8");
				org.apache.commons.io.IOUtils.closeQuietly(is);
				Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(creds);
				context = ContextBuilder.newBuilder(service).overrides(props).credentialsSupplier(credentialSupplier)
						.buildView(BlobStoreContext.class);

			} else if (service.equals("google-cloud-storage")) {
				props.setProperty(Constants.PROPERTY_ENDPOINT, "https://storage.googleapis.com");
				props.setProperty(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
				props.setProperty(Constants.PROPERTY_STRIP_EXPECT_HEADER, "true");
				context = ContextBuilder.newBuilder("s3").overrides(props).credentials(username, password)
						.buildView(BlobStoreContext.class);

			} else if (service.equals("filesystem")) {
				EncyptUtils.baseEncode = true;
				// SDFSLogger.getLog().info("share-path=" +
				// config.getAttribute("share-path"));
				// overrides.setProperty(FilesystemConstants.PROPERTY_BASEDIR,
				// config.getAttribute("share-path"));
				context = ContextBuilder.newBuilder("filesystem").overrides(props).buildView(BlobStoreContext.class);
				this.accessStore = true;
			} else {
				context = ContextBuilder.newBuilder(service).credentials(username, password).overrides(props)
						.buildView(BlobStoreContext.class);
			}

			blobStore = context.getBlobStore();

		} catch (Exception e) {
			System.err.println("Cannot authenticate to provider");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		BatchJCloudChunkStore st = new BatchJCloudChunkStore();
		st.name = "datishpool0";
		Properties p = new Properties();
		p.setProperty("service-type", "b2");
		st.checkAccess("a6ba900eebf7", "001c60230bbe493db4bbb8d5031a9707628790c7a9", p);
		System.out.println("zzz");
		PageSet<? extends StorageMetadata> zips;
		zips = st.blobStore.list(st.name, ListContainerOptions.Builder.prefix("files/").maxResults(100));
		System.out.println(zips.size());
	}

	@Override
	public void deleteStore() {
		// TODO Auto-generated method stub

	}

	@Override
	public void compact() {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("deprecation")
	@Override
	public Iterator<String> getNextObjectList(String prefix) throws IOException {
		List<String> al = new ArrayList<String>();
		if (!iter.hasNext()) {
			if (ips.getNextMarker() == null)
				return al.iterator();
			else {
				if (this.atmosStore)
					ips = blobStore.list(this.name, ListContainerOptions.Builder.recursive()
							.afterMarker(ips.getNextMarker()).inDirectory(prefix));
				else if (this.b2Store)
					ips = blobStore.list(this.name, ListContainerOptions.Builder.recursive()
							.afterMarker(ips.getNextMarker()).prefix(prefix).maxResults(100));
				else
					ips = blobStore.list(this.name,
							ListContainerOptions.Builder.recursive().afterMarker(ips.getNextMarker()).prefix(prefix));
				// SDFSLogger.getLog().info("lsize=" + ips.size());
				iter = ips.iterator();
			}
		}
		while (iter.hasNext()) {
			String fn = iter.next().getName();
			// SDFSLogger.getLog().info("fn=" + fn);
			if (!fn.endsWith(mdExt) && !fn.endsWith("/")) {
				al.add(fn);
			}
		}

		return al.iterator();
	}

	@Override
	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		IOException e = null;
		for (int i = 0; i < 10; i++) {
			try {
				// BlobMetadata dmd = blobStore.blobMetadata(this.name, key);
				Map<String, String> md = this.getMetaData(key);
				// HashCode md5 =
				// dmd.getContentMetadata().getContentMD5AsHashCode();

				ByteArrayOutputStream os = new ByteArrayOutputStream();
				this.readBlob(key, os);
				byte[] nm = os.toByteArray();
				// HashCode nmd5 = Hashing.md5().hashBytes(nm);
				/*
				 * if (!md5.equals(nmd5)) { throw new IOException("key " + key + " is corrupt");
				 * }
				 */
				boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
				if (encrypt) {
					nm = EncryptUtils.decryptCBC(nm);
				}
				long _hid = EncyptUtils.decHashArchiveName(key.substring(5), encrypt);
				boolean compress = Boolean.parseBoolean(md.get("lz4compress"));
				if (compress) {
					int size = Integer.parseInt(md.get("size"));
					nm = CompressionUtils.decompressLz4(nm, size);
				}
				final String st = new String(nm);
				StringTokenizer _ht = new StringTokenizer(st, ",");
				boolean changed = false;
				// dmd = blobStore.blobMetadata(this.name, this.getClaimName(hid));
				md = this.getMetaData(this.getClaimName(_hid));
				if (md.containsKey("deleted")) {
					md.remove("deleted");
					changed = true;
				}
				if (md.containsKey("deletedobjects")) {
					changed = true;
					md.remove("deletedobjects");
				}
				if (changed) {
					this.updateObject(this.getClaimName(_hid), md);
				}
				try {
					int _sz = Integer.parseInt(md.get("bsize"));
					int _cl = Integer.parseInt(md.get("bcompressedsize"));
					HashBlobArchive.currentLength.addAndGet(_sz);
					HashBlobArchive.compressedLength.addAndGet(_cl);
				} catch (Exception e1) {
					SDFSLogger.getLog().warn("unable to update size", e);
				}
				StringResult rslt = new StringResult();
				rslt.id = _hid;
				rslt.st = _ht;
				return rslt;
			} catch (Exception e1) {
				e = new IOException(e1);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e2) {

				}
			}
		}
		throw e;
	}

	@Override
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		IOException e = null;
		for (int i = 0; i < 9; i++) {
			String haName = "";
			try {
				if (blobStore.blobExists(this.name, this.getClaimName(id)))
					return;
				else {
					haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
					Map<String, String> md = this.getMetaData("keys/" + haName);
					int objs = Integer.parseInt(md.get("objects"));
					int delobj = objs - claims;
					md.put("deletedobjects", Integer.toString(delobj));

					Blob b = blobStore.blobBuilder(this.getClaimName(id))
							.payload(Long.toString(System.currentTimeMillis())).userMetadata(md).build();

					if (this.accessStore || this.atmosStore || b2Store) {
						this.updateObject(this.getClaimName(id), md);
						b = blobStore.blobBuilder(this.getClaimName(id))
								.payload(Long.toString(System.currentTimeMillis())).build();
					}
					this.writeBlob(b, false);
					return;
				}
			} catch (Exception e1) {
				e = new IOException(e1);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e2) {

				}
			}
		}
		if (e != null)
			throw e;

	}

	@Override
	public boolean objectClaimed(String key) throws IOException {
		if (!this.clustered)
			return true;
		String blb = "claims/" + key + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		return blobStore.blobExists(this.name, blb);

	}

	@Override
	public void checkoutFile(String name) throws IOException {
		name = FilenameUtils.separatorsToUnix(name);
		String blb = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		Blob b = blobStore.blobBuilder(blb).payload(Long.toString(System.currentTimeMillis())).build();
		this.writeBlob(b, false);

	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
		if (!this.clustered)
			return true;
		String blb = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		return blobStore.blobExists(this.name, blb);
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

	@SuppressWarnings("deprecation")
	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (this.clustered) {
			ArrayList<RemoteVolumeInfo> ids = new ArrayList<RemoteVolumeInfo>();
			PageSet<? extends StorageMetadata> bips = null;
			if (this.atmosStore)
				bips = blobStore.list(this.name, ListContainerOptions.Builder.inDirectory("bucketinfo"));
			else if (this.b2Store)
				bips = blobStore.list(this.name, ListContainerOptions.Builder.prefix("bucketinfo").maxResults(100));
			else
				bips = blobStore.list(this.name, ListContainerOptions.Builder.prefix("bucketinfo"));
			Iterator<? extends StorageMetadata> liter = bips.iterator();
			while (liter.hasNext()) {
				StorageMetadata md = liter.next();
				if (!md.getName().endsWith(mdExt) && !md.getName().endsWith("/")) {
					String st = md.getName().substring("bucketinfo/".length());
					long mids = EncyptUtils.decHashArchiveName(st, Main.chunkStoreEncryptionEnabled);
					RemoteVolumeInfo info = new RemoteVolumeInfo();
					Map<String, String> mdk = this.getMetaData(md.getName());
					info.id = mids;
					info.hostname = mdk.get("hostname");
					info.port = Integer.parseInt(mdk.get("port"));
					info.compressed = Long.parseLong(mdk.get("compressedlength"));
					info.data = Long.parseLong(mdk.get("currentlength"));
					info.lastupdated = Long.parseLong(mdk.get("lastupdated"));
					info.sdfsversion = mdk.get("bucketversion");
					info.version = Integer.parseInt(mdk.get("bucketversion"));
					ids.add(info);
				}
			}
			RemoteVolumeInfo[] lids = new RemoteVolumeInfo[ids.size()];
			for (int i = 0; i < ids.size(); i++) {
				lids[i] = ids.get(i);
			}
			return lids;
		} else {
			RemoteVolumeInfo info = new RemoteVolumeInfo();
			info.id = Main.DSEID;
			info.port = Main.sdfsCliPort;
			info.hostname = InetAddress.getLocalHost().getHostName();
			info.compressed = this.compressedSize();
			info.data = this.size();
			info.sdfsversion = Main.version;
			info.version = version;
			info.lastupdated = System.currentTimeMillis();
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

	@SuppressWarnings("deprecation")
	@Override
	public void removeVolume(long volumeID) throws IOException {
		if (volumeID == Main.DSEID)
			throw new IOException("volume can not remove its self");
		String klbi = "bucketinfo/" + EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		Map<String, String> md = this.getMetaData(klbi);
		long dur = System.currentTimeMillis() - Long.parseLong(md.get("lastupdated"));
		if (dur < (60000 * 2)) {
			throw new IOException("Volume [" + volumeID + "] is currently mounted");
		}
		if (this.atmosStore)
			ips = blobStore.list(this.name, ListContainerOptions.Builder.recursive().inDirectory("claims"));
		else if (this.b2Store)
			ips = blobStore.list(this.name, ListContainerOptions.Builder.recursive().prefix("claims").maxResults(100));
		else
			ips = blobStore.list(this.name, ListContainerOptions.Builder.recursive().prefix("claims"));
		iter = ips.iterator();
		Iterator<String> objs = this.getNextObjectList("claims");
		String vid = EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		String suffix = "/" + vid;
		String prefix = "claims/";
		while (objs != null) {
			while (objs.hasNext()) {
				String nm = objs.next();
				if (nm.endsWith(vid)) {
					blobStore.removeBlob(this.name, nm);
					removeMetaData(nm);
					String fldr = nm.substring(0, nm.length() - suffix.length());
					PageSet<? extends StorageMetadata> bips = null;
					if (this.atmosStore)
						bips = blobStore.list(this.name, ListContainerOptions.Builder.inDirectory(fldr));
					else if (this.b2Store)
						blobStore.list(this.name, ListContainerOptions.Builder.prefix(fldr).maxResults(100));
					else
						blobStore.list(this.name, ListContainerOptions.Builder.prefix(fldr));
					if (bips.isEmpty()) {
						String fl = fldr.substring(prefix.length());
						blobStore.removeBlob(this.name, fl);
						removeMetaData(fl);
					}

				}
			}
			objs = this.getNextObjectList("claims");
			if (!objs.hasNext())
				objs = null;
		}
		blobStore.removeBlob(this.name, klbi);
		removeMetaData(klbi);
	}

	private static class B2ConnectionFactory extends BasePooledObjectFactory<BlobStore> {
		private final String accessKey;
		private final String secretKey;
		private final Properties overrides;

		private B2ConnectionFactory(String accessKey, String secretKey, Properties overrides) {
			this.accessKey = accessKey;
			this.secretKey = secretKey;
			this.overrides = overrides;
		}

		@Override
		public BlobStore create() throws Exception {
			BlobStoreContext context = ContextBuilder.newBuilder("b2").credentials(this.accessKey, this.secretKey)
					.overrides(overrides).buildView(BlobStoreContext.class);
			return context.getBlobStore();
		}

		@Override
		public PooledObject<BlobStore> wrap(BlobStore store) {
			return new DefaultPooledObject<BlobStore>(store);
		}

	}

	private static class DeleteObject implements Runnable {
		long id;
		BatchJCloudChunkStore st;

		private DeleteObject(long id, BatchJCloudChunkStore st) {
			this.id = id;
			this.st = st;
		}

		@Override
		public void run() {
			int tries = 0;
			Exception e1 = null;
			for (;;) {
				try {
					st.verifyDelete(id);
					e1 = null;
					break;
				} catch (Exception e) {
					e1 = e;
					if (tries > 6)
						break;
					else
						tries++;
				}

			}
			if (e1 != null) {
				SDFSLogger.getLog().warn("unable to delete object " + id, e1);
			}

		}

	}

	@Override
	public void timeStampData(long key) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addRefresh(long id) {
		// TODO Auto-generated method stub

	}

}
