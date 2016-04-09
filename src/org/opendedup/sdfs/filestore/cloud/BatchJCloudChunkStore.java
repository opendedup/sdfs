package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;



import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.ws.rs.core.MediaType;

import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.StringResult;
import org.apache.commons.compress.utils.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
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
import com.google.common.hash.HashingInputStream;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;





import org.opendedup.collections.HashExistsException;

import static java.lang.Math.toIntExact;

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
	private AtomicLong rcurrentSize = new AtomicLong();
	private AtomicLong rcurrentCompressedSize = new AtomicLong();
	private int checkInterval = 15000;
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
			SDFSLogger.getLog().info("############ Closing Azure Container ##################");
			// container = pool.borrowObject();
			HashBlobArchive.close();
			BlobMetadata bmd = blobStore.blobMetadata(this.name, "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled));
			Map<String, String> md = bmd.getUserMetadata();
			md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
			blobStore.copyBlob(this.name, "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled), this.name, "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled),
					CopyOptions.builder().contentMetadata(bmd.getContentMetadata()).userMetadata(md).build());
			this.context.close();
			SDFSLogger.getLog().info("Updated container on close");
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

	public void cacheData(byte[] hash, long start, int len) throws IOException, DataArchivedException {
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
		return HashBlobArchive.currentLength.get() + this.rcurrentSize.get();
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

	public void deleteBucket() throws IOException {
		try {
			blobStore.deleteContainer(this.name);
		} finally {
			// pool.returnObject(container);
		}
		this.close();
	}
	
	private void resetCurrentSize() throws IOException {
		PageSet<? extends StorageMetadata> bul = blobStore.list(this.name, ListContainerOptions.Builder.inDirectory("bucketinfo").withDetails());
		long rcs = 0;
		long rccs = 0;
		String lbi = "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		for(StorageMetadata st : bul) {
			if(!st.getName().equalsIgnoreCase(lbi)) {
			Map<String,String> md = st.getUserMetadata();
			rcs += Long.parseLong(md.get("currentlength"));
			rccs += Long.parseLong(md.get("compressedlength"));
			}
		}
		this.rcurrentCompressedSize.set(rccs);
		this.rcurrentSize.set(rcs);
	}

	@Override
	public void init(Element config) throws IOException {
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
		if(config.hasAttribute("connection-check-interval")) {
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
			context = ContextBuilder.newBuilder(service).credentials(Main.cloudAccessKey, Main.cloudSecretKey)
					.buildView(BlobStoreContext.class);
			blobStore = context.getBlobStore();
			
			// Retry after 25 seconds of no response
			overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "5000");
			overrides.setProperty(Constants.PROPERTY_USER_THREADS, Integer.toString(Main.dseIOThreads * 2));
			// Keep retrying indefinitely
			overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, "10");
			// Do not wait between retries
			overrides.setProperty(Constants.PROPERTY_RETRY_DELAY_START, "0");
			if (!blobStore.containerExists(this.name))
				blobStore.createContainerInLocation(null, this.name);
			/*
			 * serviceClient.getDefaultRequestOptions().setTimeoutIntervalInMs(
			 * 10 * 1000);
			 * 
			 * serviceClient.getDefaultRequestOptions().setRetryPolicyFactory(
			 * new RetryExponentialRetry(500, 5));
			 */
			Map<String, String> md = new HashMap<String,String>();
			String lbi = "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			if (blobStore.blobExists(this.name,lbi))
				md = blobStore.blobMetadata(this.name, lbi).getUserMetadata();
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
				Blob b = blobStore.blobBuilder("bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled)).payload(Long.toString(System.currentTimeMillis()))
						.userMetadata(md).build();
				blobStore.putBlob(this.name, b);
			}
			HashBlobArchive.currentLength.set(sz);
			HashBlobArchive.compressedLength.set(cl);
			this.resetCurrentSize();
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

	Iterator<? extends StorageMetadata> iter = null;
	PageSet<? extends StorageMetadata> ips = null;
	MultiDownload dl = null;

	@Override
	public void iterationInit(boolean deep) throws IOException {
		this.hid = 0;
		this.ht = null;
		ips = blobStore.list(this.name, ListContainerOptions.Builder.withDetails().inDirectory("keys"));
		iter = ips.iterator();
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
		return HashBlobArchive.compressedLength.get() + this.rcurrentCompressedSize.get();
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
		Blob blob = blobStore.getBlob(this.name, "keys/" + haName);
		BlobMetadata dmd = blobStore.blobMetadata(this.name, "keys/" + haName);
		Map<String, String> md = dmd.getUserMetadata();
		ByteArrayOutputStream out = new ByteArrayOutputStream(toIntExact(dmd.getContentMetadata().getContentLength()));
		IOUtils.copy(blob.getPayload().openStream(), out);
		try {
			blob.getPayload().close();
			out.close();
		} catch (Exception e) {

		}

		byte[] nm = out.toByteArray();

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
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		try {
			// container = pool.borrowObject();
			File f = arc.getFile();
			HashMap<String, String> metaData = new HashMap<String, String>();
			metaData.put("size", Integer.toString(arc.uncompressedLength.get()));
			if (Main.compress) {
				metaData.put("lz4compress", "true");
			} else {
				metaData.put("lz4compress", "false");
			}
			long csz = f.length();
			if (Main.chunkStoreEncryptionEnabled) {
				metaData.put("encrypt", "true");
			} else {
				metaData.put("encrypt", "false");
			}
			metaData.put("compressedsize", Long.toString(csz));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			metaData.put("objects", Integer.toString(arc.getSz()));
			HashCode hc = com.google.common.io.Files.hash(f, Hashing.md5());
			Blob blob = blobStore.blobBuilder("blocks/" + haName).userMetadata(metaData).payload(f).contentLength(csz)
					.contentMD5(hc).contentType(MediaType.APPLICATION_OCTET_STREAM)
					.build();
			blobStore.putBlob(this.name, blob, multipart());
			// upload the metadata
			String st = arc.getHashesString();
			byte [] chunks = st.getBytes();
			metaData = new HashMap<String, String>();
			// metaData = new HashMap<String, String>();
			int ssz = chunks.length;
			if (Main.compress) {
				chunks = CompressionUtils.compressLz4(chunks);
				metaData.put("lz4compress", "true");
			} else {
				metaData.put("lz4compress", "false");
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
			metaData.put("bcompressedsize", Long.toString(csz));
			metaData.put("objects", Integer.toString(arc.getSz()));
			blob = blobStore.blobBuilder("keys/" + haName).userMetadata(metaData).payload(chunks).contentLength(chunks.length)
					.contentMD5(Hashing.md5().hashBytes(chunks)).contentType(MediaType.APPLICATION_OCTET_STREAM)
					.build();
			
			blobStore.putBlob(this.name, blob);
			blob = blobStore.blobBuilder(this.getClaimName(id)).userMetadata(metaData)
					.payload(Long.toString(System.currentTimeMillis())).contentType(MediaType.APPLICATION_OCTET_STREAM)
					.build();
			blobStore.putBlob(this.name, blob);

		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to write archive " + arc.getID() + " with id " + id, e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

			Blob blob = blobStore.getBlob(this.name, "blocks/" + haName);
			BlobMetadata md = blobStore.blobMetadata(this.name, "blocks/" + haName);
			Map<String, String> metaData = md.getUserMetadata();
			FileOutputStream out = new FileOutputStream(f);
			
			try {
				IOUtils.copy(blob.getPayload().openStream(), out);
			} catch (Exception e) {

			} finally {
				IOUtils.closeQuietly(blob.getPayload());
				IOUtils.closeQuietly(out);
			}
			if (metaData.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean(metaData.get("deleted"));
				if (del) {
					BlobMetadata kmd = blobStore.blobMetadata(this.name, "keys/" + haName);

					Map<String, String> kmetaData = kmd.getUserMetadata();
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
					int _size = Integer.parseInt((String) metaData.get("size"));
					int _compressedSize = Integer.parseInt((String) metaData.get("compressedsize"));
					HashBlobArchive.currentLength.addAndGet(_size);
					HashBlobArchive.compressedLength.addAndGet(_compressedSize);
					blobStore.copyBlob(this.name, "keys/" + haName, this.name, "keys/" + haName, CopyOptions.builder()
							.userMetadata(kmetaData).contentMetadata(kmd.getContentMetadata()).build());
					metaData.remove("deleted");
					metaData.put("deletedobjects", Integer.toString(delobj));
					metaData.put("suspect", "true");
					blobStore.copyBlob(this.name, "blocks/" + haName, this.name, "blocks/" + haName, CopyOptions
							.builder().userMetadata(metaData).contentMetadata(md.getContentMetadata()).build());

				}

			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch block [" + id + "]", e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	private int verifyDelete(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

		Map<String, String> metaData = null;
		if (clustered)
			metaData = blobStore.blobMetadata(this.name, this.getClaimName(id)).getUserMetadata();
		else
			metaData = blobStore.blobMetadata(this.name, "keys/" + haName).getUserMetadata();
		int claims = this.getClaimedObjects("keys/" + haName);
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
			HashBlobArchive.currentLength.addAndGet(_size);
			HashBlobArchive.compressedLength.addAndGet(_compressedSize);

			if (clustered) {
				blobStore.copyBlob(this.name, this.getClaimName(id), this.name, this.getClaimName(id),
						CopyOptions.builder().userMetadata(metaData).build());
			} else {
				blobStore.copyBlob(this.name, this.getClaimName(id), this.name, "keys/" + haName,
						CopyOptions.builder().userMetadata(metaData).build());
			}
		} else {

			if (clustered) {
				blobStore.removeBlob(this.name, this.getClaimName(id));
				if (!blobStore.list(this.name,ListContainerOptions.Builder.inDirectory("claims/keys/" + haName)).iterator().hasNext()) {
					blobStore.removeBlob(this.name, "keys/" + haName);
					blobStore.removeBlob(this.name, "blocks/" + haName);
				}
			} else {
				blobStore.removeBlob(this.name, "keys/" + haName);
				blobStore.removeBlob(this.name, "blocks/" + haName);
			}
		}

		return claims;
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {
					String lbi = "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					BlobMetadata dmd = blobStore.blobMetadata(this.name, lbi);
					Map<String, String> md = dmd.getUserMetadata();
					md.put("currentlength", Long.toString(HashBlobArchive.currentLength.get()));
					md.put("compressedlength", Long.toString(HashBlobArchive.compressedLength.get()));
					md.put("clustered", Boolean.toString(this.clustered));
					blobStore.copyBlob(this.name, lbi, this.name, lbi,
							CopyOptions.builder().contentMetadata(dmd.getContentMetadata()).userMetadata(md).build());
					this.resetCurrentSize();
				} catch (Exception e) {
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
						String hashString = EncyptUtils.encHashArchiveName(k.longValue(),
								Main.chunkStoreEncryptionEnabled);
						try {
							BlobMetadata dmd = blobStore.blobMetadata(this.name, this.getClaimName(k));
							Map<String, String> metaData = dmd.getUserMetadata();
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
								if (HashBlobArchive.compressedLength.get() > 0) {

									HashBlobArchive.compressedLength.addAndGet(-1 * compressedSize);
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
									metaData.put("deletedobjects", Integer.toString(delobj));
									blobStore.copyBlob(this.name, this.getClaimName(k), this.name, this.getClaimName(k),
											CopyOptions.builder().contentMetadata(dmd.getContentMetadata())
													.userMetadata(metaData).build());
								}

							} else {
								// SDFSLogger.getLog().info("updating " +
								// hashString + " sz=" +objs);
								metaData.put("deletedobjects", Integer.toString(delobj));
								blobStore.copyBlob(this.name, this.getClaimName(k), this.name, this.getClaimName(k),
										CopyOptions.builder().contentMetadata(dmd.getContentMetadata())
												.userMetadata(metaData).build());
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
	public void uploadFile(File f, String to, String pp) throws IOException {
		BufferedInputStream in = null;
		while (to.startsWith(File.separator))
			to = to.substring(1);
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
				Blob b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length()).userMetadata(metaData).build();
				blobStore.putBlob(this.name, b);
				this.checkoutFile(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else if (isDir) {
			try {
				HashMap<String, String> metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				metaData.put("directory", "true");
				Blob b = blobStore.blobBuilder(pth).payload(pth).contentLength(pth.length()).userMetadata(metaData).build();
				blobStore.putBlob(this.name, b);
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
				HashCode hc = com.google.common.io.Files.hash(p,Hashing.md5());
				HashMap<String, String> metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.put("lz4compress", Boolean.toString(Main.compress));
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				Blob b = blobStore.blobBuilder(pth).payload(fp).contentLength(p.length()).contentMD5(hc).userMetadata(metaData).build();
				blobStore.putBlob(this.name, b, multipart());
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
			Blob blob = blobStore.getBlob(this.name,
					fn);
			
			BlobMetadata md = blobStore.blobMetadata(this.name,
					fn);
			HashCode md5 = md.getContentMetadata().getContentMD5AsHashCode();
			BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(p));
			try (HashingInputStream his = new HashingInputStream(Hashing.md5(), blob.getPayload().openStream())) {
				ByteStreams.copy(his, os);
				try {
				his.close();
				os.flush();
				os.close();
				}catch(Exception e1) {}
				if (!md5.equals(his.hash())) {
					throw new IOException("file " + p.getPath() + " is corrupt");
				}
				
			}
			
			Map<String, String> metaData = md.getUserMetadata();
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

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		
		String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
		try {
			if(this.clustered) {
				String blb = "claims/" + haName +  "/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				blobStore.removeBlob(this.name, blb);
				SDFSLogger.getLog().info("deleting " + blb );
				if (!blobStore.list(this.name,ListContainerOptions.Builder.inDirectory("claims/"+haName)).iterator().hasNext()) {
					blobStore.removeBlob(this.name, haName);
					SDFSLogger.getLog().info("deleting " + haName );
				}
			}else {
				blobStore.removeBlob(this.name, haName);
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
			BlobMetadata dmd = blobStore.blobMetadata(this.name, fn);
			blobStore.copyBlob(this.name, fn, this.name, tn, CopyOptions.builder().userMetadata(dmd.getUserMetadata()).build());
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	PageSet<? extends StorageMetadata>ps = null;
	Iterator<? extends StorageMetadata> di = null;

	public void clearIter() {
		di = null;
		ps = null;
	}

	public String getNextName(String pp) throws IOException {
		String pfx = pp + "/";
		if (di == null) {
			ps = blobStore.list(this.name,ListContainerOptions.Builder.withDetails().recursive().inDirectory(pp));
			di = ps.iterator();
		} if(!di.hasNext()) {
			if(ps.getNextMarker() == null) {
				di = null;
				ps = null;
				return null;
			}
			else {
				ps = blobStore.list(this.name,ListContainerOptions.Builder.withDetails().recursive().afterMarker(ps.getNextMarker()).inDirectory(pp));
				di = ps.iterator();
			}
		}
		while (di.hasNext()) {
			StorageMetadata bi = di.next();
			try {
				

				Map<String, String> md = bi.getUserMetadata();
				boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
				String fname = EncyptUtils.decString(bi.getName().substring(pfx.length()), encrypt);
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
		try {
			String[] ks = this.getStrings(id);
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
				Map<String, String> md = blobStore.blobMetadata(this.name, "bucketinfo/" +EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled)).getUserMetadata();
				if (md.containsKey("currentlength")) {
					Long.parseLong(md.get("currentlength"));
					return true;
				}
			} catch (Exception _e) {
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
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
		String service = props.getProperty("service-type");
		context = ContextBuilder.newBuilder(service).credentials(Main.cloudAccessKey, Main.cloudSecretKey)
				.buildView(BlobStoreContext.class);
		blobStore = context.getBlobStore();
		return true;
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
		if(!iter.hasNext()) {
			if(ips.getNextMarker() == null)
				return al.iterator();
			else {
				ips = blobStore.list(this.name, ListContainerOptions.Builder.afterMarker(ips.getNextMarker()).inDirectory("keys"));
				iter = ips.iterator();
			}
		}
		while(iter.hasNext()) {
			al.add(iter.next().getName());
		}
		
		return al.iterator();
	}

	@Override
	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		try {
			BlobMetadata dmd = blobStore.blobMetadata(this.name, key);
			Map<String, String> md = dmd.getUserMetadata();
			HashCode md5 = dmd.getContentMetadata().getContentMD5AsHashCode();
			Blob blob = blobStore.getBlob(this.name,key);
			ByteArrayOutputStream os = new ByteArrayOutputStream(toIntExact(dmd.getContentMetadata().getContentLength()));
			IOUtils.copy(blob.getPayload().openStream(), os);
			os.flush();
			IOUtils.closeQuietly(os);
			blob.getPayload().release();
			byte [] nm = os.toByteArray();
			HashCode nmd5 = Hashing.md5().hashBytes(nm);
			
			if (!md5.equals(nmd5)) {
				throw new IOException("key " + key + " is corrupt");
			}
			boolean encrypt = Boolean.parseBoolean(md.get("encrypt"));
			if (encrypt) {
				nm = EncryptUtils.decryptCBC(nm);
			}
			this.hid = EncyptUtils.decHashArchiveName(key.substring(5), encrypt);
			boolean compress = Boolean.parseBoolean(md.get("lz4compress"));
			if (compress) {
				int size = Integer.parseInt(md.get("size"));
				nm = CompressionUtils.decompressLz4(nm, size);
			}
			final String st = new String(nm);
			ht = new StringTokenizer(st, ",");
			boolean changed = false;
			dmd = blobStore.blobMetadata(this.name, this.getClaimName(hid));
			md = dmd.getUserMetadata();
			if (md.containsKey("deleted")) {
				md.remove("deleted");
				changed = true;
			}
			if (md.containsKey("deletedobjects")) {
				changed = true;
				md.remove("deletedobjects");
			}
			if (changed) {
				blobStore.copyBlob(this.name, this.getClaimName(hid), this.name, this.getClaimName(hid), CopyOptions.builder().userMetadata(md).contentMetadata(dmd.getContentMetadata()).build());
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
			if (blobStore.blobExists(this.name, this.getClaimName(id)))
				return;
			else {
				String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
				BlobMetadata dmd = blobStore.blobMetadata(this.name,"keys/" + haName);
				Map<String,String> md = dmd.getUserMetadata();
				int objs = Integer.parseInt(md.get("objects"));
				int delobj = objs-claims;
				md.put("deletedobjects", Integer.toString(delobj));
				Blob b = blobStore.blobBuilder(this.getClaimName(id)).payload(Long.toString(System.currentTimeMillis())).userMetadata(md).build();
				blobStore.putBlob(this.name, b);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}

	}
	
	

	@Override
	public boolean objectClaimed(String key) throws IOException {
		if(!this.clustered)
			return true;
		String blb = "claims/" + key +  "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		return blobStore.blobExists(this.name, blb);
		
	}

	@Override
	public void checkoutFile(String name) throws IOException {
		String blb = "claims/" + name +  "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		Blob b =blobStore.blobBuilder(blb).payload(Long.toString(System.currentTimeMillis())).build();
		blobStore.putBlob(this.name, b);
	}

	@Override
	public boolean isCheckedOut(String name) throws IOException {
		if(!this.clustered)
			return true;
		String blb = "claims/" + name +  "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		return blobStore.blobExists(this.name, blb);
	}

	@Override
	public int getCheckInterval() {
		return this.checkInterval;
	}

}
