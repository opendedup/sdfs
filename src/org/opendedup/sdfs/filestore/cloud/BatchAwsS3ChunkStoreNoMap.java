package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;



import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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
import java.nio.file.attribute.PosixFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Math.toIntExact;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.jets3t.service.utils.ServiceUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.io.BaseEncoding;

import org.opendedup.collections.HashExistsException;
import org.opendedup.fsync.SyncFSScheduler;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.HashBlobArchiveNoMap;
import org.opendedup.sdfs.filestore.StringResult;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.filestore.cloud.utils.FileUtils;

/**
 * 
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 * 
 */
@SuppressWarnings("deprecation")
public class BatchAwsS3ChunkStoreNoMap implements AbstractChunkStore,
		AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private static BasicAWSCredentials awsCredentials = null;
	private HashMap<Integer, Integer> deletes = new HashMap<Integer, Integer>();
	private String name;
	private com.amazonaws.regions.Region bucketLocation = null;

	AmazonS3Client s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	private int downloadBlockSize = 128 * 1024;
	private int glacierDays = 0;
	File staged_sync_location = new File(Main.chunkStore + File.separator
			+ "syncstaged");
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private static transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private boolean genericS3 = false;
	private static transient ThreadPoolExecutor executor = null;
	private WeakHashMap<Integer, String> restoreRequests = new WeakHashMap<Integer, String>();

	static {
		try {
			if (!Main.useAim)
				awsCredentials = new BasicAWSCredentials(Main.cloudAccessKey,
						Main.cloudSecretKey);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			System.out.println("unable to authenticate");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		BasicAWSCredentials creds = null;
		try {
			creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
			AmazonS3Client s3Service = new AmazonS3Client(creds);
			s3Service.listBuckets();
			s3Service.shutdown();
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
			creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
			AmazonS3Client s3Service = new AmazonS3Client(creds);
			return s3Service.doesBucketExist(bucketName);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to create aws bucket", e);
			return false;
		}
	}

	public BatchAwsS3ChunkStoreNoMap() {

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
		this.closed = true;
		try {
			SDFSLogger.getLog().info(
					"############ Closing Bucket##################");
			HashBlobArchive.close();
			ObjectMetadata omd = s3Service
					.getObjectMetadata(name, "bucketinfo");
			HashMap<String, String> md = new HashMap<String, String>();
			md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("currentcompressedsize",
					Long.toString(HashBlobArchive.compressedLength.get()));
			omd.setUserMetadata(md);
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(name,
					"bucketinfo", name, "bucketinfo")
					.withNewObjectMetadata(omd);
			s3Service.copyObject(copyObjectRequest);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while closing bucket " + this.name,
					e);
		} finally {
			try {
				s3Service.shutdown();
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"error while closing bucket " + this.name, e);
			}
		}

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len)
			throws IOException, DataArchivedException {
		try {
			return HashBlobArchiveNoMap.getBlock(hash, start);
		} catch (ExecutionException e) {
			SDFSLogger.getLog().error("Unable to get block at " + start, e);
			throw new IOException(e);
		}

	}

	public void cacheData(byte[] hash, long start, int len) throws IOException,
			DataArchivedException {
		try {
			ByteBuffer bf = ByteBuffer.allocate(8);
			bf.putLong(start);
			bf.position(0);
			int hbid = bf.getInt();
			HashBlobArchiveNoMap.cacheArchive(hash, hbid);
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
		return HashBlobArchiveNoMap.currentLength.get();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		try {
			return HashBlobArchiveNoMap.writeBlock(hash, chunk);
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
			ByteBuffer bf = ByteBuffer.allocate(8);
			bf.putLong(start);
			bf.position(0);
			int hbid = bf.getInt();
			if (this.deletes.containsKey(hbid)) {
				int sz = this.deletes.get(hbid) + 1;
				this.deletes.put(hbid, sz);
			} else
				this.deletes.put(hbid, 1);

		} finally {
			delLock.unlock();
		}
	}

	public static void deleteBucket(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out.println("");
			System.out.print("Deleting Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new BasicAWSCredentials(
					awsAccessKey, awsSecretKey);
			AmazonS3Client bs3Service = new AmazonS3Client(bawsCredentials);
			ObjectListing ls = bs3Service.listObjects(bucketName);
			for (S3ObjectSummary objectSummary : ls.getObjectSummaries()) {
				bs3Service.deleteObject(bucketName, objectSummary.getKey());
				System.out.print(".");
			}
			bs3Service.deleteBucket(bucketName);
			SDFSLogger.getLog().info("Bucket [" + bucketName + "] deleted");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (Exception e) {
			SDFSLogger.getLog()
					.warn("Unable to delete bucket " + bucketName, e);
		}
	}

	@Override
	public void init(Element config) throws IOException {
		this.name = Main.cloudBucket.toLowerCase();
		this.staged_sync_location.mkdirs();
		try {
			if (config.hasAttribute("default-bucket-location")) {
				bucketLocation = RegionUtils.getRegion(config
						.getAttribute("default-bucket-location"));
			}

			if (config.hasAttribute("block-size")) {
				int sz = (int) StringUtils.parseSize(config
						.getAttribute("block-size"));
				HashBlobArchive.MAX_LEN = sz;
			}
			if (config.hasAttribute("allow-sync")) {
				HashBlobArchive.allowSync = Boolean.parseBoolean(config
						.getAttribute("allow-sync"));
				if (config.hasAttribute("sync-check-schedule")) {
					try {
						new SyncFSScheduler(
								config.getAttribute("sync-check-schedule"));
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"unable to start sync scheduler", e);
					}
				}

			}
			if (config.hasAttribute("upload-thread-sleep-time")) {
				int tm = Integer.parseInt(config
						.getAttribute("upload-thread-sleep-time"));
				HashBlobArchive.THREAD_SLEEP_TIME = tm;
			}

			if (config.hasAttribute("sync-files")) {
				boolean syncf = Boolean.parseBoolean(config
						.getAttribute("sync-files"));
				if (syncf) {
					new FileReplicationService(this);
				}
			}
			int rsp = 0;
			int wsp = 0;
			if (config.hasAttribute("read-speed")) {
				rsp = Integer.parseInt(config.getAttribute("read-speed"));
			}
			if (config.hasAttribute("write-speed")) {
				wsp = Integer.parseInt(config.getAttribute("write-speed"));
			}
			if (config.hasAttribute("local-cache-size")) {
				long sz = StringUtils.parseSize(config
						.getAttribute("local-cache-size"));
				HashBlobArchive.setLocalCacheSize(sz);
			}
			if (config.hasAttribute("map-cache-size")) {
				int sz = Integer
						.parseInt(config.getAttribute("map-cache-size"));
				HashBlobArchive.MAP_CACHE_SIZE = sz;
			}
			if (config.hasAttribute("io-threads")) {
				int sz = Integer.parseInt(config.getAttribute("io-threads"));
				Main.dseIOThreads = sz;
			}
			if (config.hasAttribute("delete-unclaimed")) {
				this.deleteUnclaimed = Boolean.parseBoolean(config
						.getAttribute("delete-unclaimed"));
			}
			if (config.hasAttribute("glacier-archive-days")) {
				this.glacierDays = Integer.parseInt(config
						.getAttribute("glacier-archive-days"));
				if (this.glacierDays > 0)
					Main.checkArchiveOnRead = true;
			}
			if (config.hasAttribute("download-blocksize-kb"))
				this.downloadBlockSize = Integer.parseInt(config
						.getAttribute("download-blocksize-kb")) * 1024;
			ClientConfiguration clientConfig = new ClientConfiguration();
			clientConfig.setMaxConnections(Main.dseIOThreads * 2);
			clientConfig.setConnectionTimeout(10000);
			clientConfig.setSocketTimeout(10000);

			String s3Target = null;
			if (config.getElementsByTagName("connection-props").getLength() > 0) {
				Element el = (Element) config.getElementsByTagName(
						"connection-props").item(0);
				if (el.hasAttribute("connection-timeout"))
					clientConfig.setConnectionTimeout(Integer.parseInt(el
							.getAttribute("connection-timeout")));
				if (el.hasAttribute("socket-timeout"))
					clientConfig.setSocketTimeout(Integer.parseInt(el
							.getAttribute("socket-timeout")));
				if (el.hasAttribute("local-address"))
					clientConfig.setLocalAddress(InetAddress.getByName(el
							.getAttribute("local-address")));
				if (el.hasAttribute("max-retry"))
					clientConfig.setMaxErrorRetry(Integer.parseInt(el
							.getAttribute("max-retry")));
				if (el.hasAttribute("protocol")) {
					String pr = el.getAttribute("protocol");
					if (pr.equalsIgnoreCase("http"))
						clientConfig.setProtocol(Protocol.HTTP);
					else
						clientConfig.setProtocol(Protocol.HTTPS);

				}
				if (el.hasAttribute("s3-target")) {
					s3Target = el.getAttribute("s3-target");
				}
				if (el.hasAttribute("proxy-host")) {
					clientConfig.setProxyHost(el.getAttribute("proxy-host"));
				}
				if (el.hasAttribute("proxy-domain")) {
					clientConfig
							.setProxyDomain(el.getAttribute("proxy-domain"));
				}
				if (el.hasAttribute("proxy-password")) {
					clientConfig.setProxyPassword(el
							.getAttribute("proxy-password"));
				}
				if (el.hasAttribute("proxy-port")) {
					clientConfig.setProxyPort(Integer.parseInt(el
							.getAttribute("proxy-port")));
				}
				if (el.hasAttribute("proxy-username")) {
					clientConfig.setProxyUsername(el
							.getAttribute("proxy-username"));
				}
			}
			if (awsCredentials != null)
				s3Service = new AmazonS3Client(awsCredentials, clientConfig);
			else
				s3Service = new AmazonS3Client(
						new InstanceProfileCredentialsProvider(), clientConfig);
			if (s3Target != null) {
				TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate,
							String authType) {
						return true;
					}
				};
				SSLSocketFactory sf = new SSLSocketFactory(
						acceptingTrustStrategy,
						SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
				clientConfig.getApacheHttpClientConfig().withSslSocketFactory(
						sf);
				s3Service.setEndpoint(s3Target);
				this.genericS3 = true;
			}
			if (config.hasAttribute("disableDNSBucket")) {
				s3Service.setS3ClientOptions(new S3ClientOptions()
						.withPathStyleAccess(Boolean.parseBoolean(config
								.getAttribute("disableDNSBucket"))));
			}
			if (bucketLocation != null)
				s3Service.setRegion(bucketLocation);
			if (!s3Service.doesBucketExist(this.name)) {
				s3Service.createBucket(this.name);
				SDFSLogger.getLog().info("created new store " + name);
				ObjectMetadata md = new ObjectMetadata();
				md.addUserMetadata("currentsize", "0");
				md.addUserMetadata("currentcompressedsize", "0");
				byte [] sz = "bucketinfodatanow".getBytes();
				md.setContentLength(sz.length);
				s3Service.putObject(this.name, "bucketinfo",
						new ByteArrayInputStream(sz), md);
			} else {
				Map<String, String> obj = null;
				ObjectMetadata omd = null;
				try {
					omd = s3Service.getObjectMetadata(this.name,
							"bucketinfo");
					obj = omd.getUserMetadata();
					obj.get("currentsize");
				} catch (Exception e) {
					omd = null;
					SDFSLogger.getLog().debug(
							"unable to find bucketinfo object", e);
				}
				if (omd == null) {
					ObjectMetadata md = new ObjectMetadata();
					md.addUserMetadata("currentsize", "0");
					md.addUserMetadata("currentcompressedsize", "0");
					byte [] sz = "bucketinfodatanow".getBytes();
					md.setContentLength(sz.length);
					s3Service.putObject(this.name, "bucketinfo",
							new ByteArrayInputStream(sz), md);
				} else {
					if (obj.containsKey("currentsize")) {
						long cl = Long.parseLong((String) obj
								.get("currentsize"));
						if (cl >= 0) {
							HashBlobArchive.currentLength.set(cl);

						} else
							SDFSLogger.getLog().warn(
									"The S3 objectstore DSE did not close correctly len="
											+ cl);
					} else {
						SDFSLogger
								.getLog()
								.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}

					if (obj.containsKey("currentcompressedsize")) {
						long cl = Long.parseLong((String) obj
								.get("currentcompressedsize"));
						if (cl >= 0) {
							HashBlobArchive.compressedLength.set(cl);

						} else
							SDFSLogger.getLog().warn(
									"The S3 objectstore DSE did not close correctly clen="
											+ cl);
					} else {
						SDFSLogger
								.getLog()
								.warn("The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}
					omd.setUserMetadata(obj);
					try {
					CopyObjectRequest reg = new CopyObjectRequest(this.name,
							"bucketinfo", this.name, "bucketinfo")
							.withNewObjectMetadata(omd);
					s3Service.copyObject(reg);
					}catch(Exception e) {
						SDFSLogger.getLog().warn("unable to update bucket info in init",e);
					}
				}
			}
			executor = new ThreadPoolExecutor(Main.dseIOThreads + 1,
					Main.dseIOThreads + 1, 10, TimeUnit.SECONDS, worksQueue,
					executionHandler);
			if (this.glacierDays > 0 && !this.genericS3) {
				Transition transToArchive = new Transition().withDays(
						this.glacierDays)
						.withStorageClass(StorageClass.Glacier);
				BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
						.withId("SDFS Automated Archive Rule for Block Data")
						.withPrefix("blocks/")
						.withTransition(transToArchive)
						.withStatus(
								BucketLifecycleConfiguration.ENABLED.toString());
				List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
				rules.add(ruleArchiveAndExpire);

				BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
						.withRules(rules);

				// Save configuration.
				s3Service.setBucketLifecycleConfiguration(this.name,
						configuration);
			} else if(!this.genericS3) {
				s3Service.deleteBucketLifecycleConfiguration(this.name);
			}
			HashBlobArchiveNoMap.init(this);
			HashBlobArchiveNoMap.setReadSpeed(rsp);
			HashBlobArchiveNoMap.setWriteSpeed(wsp);
				Thread th = new Thread(this);
				th.start();
		} catch (Exception e) {
			throw new IOException(e);
		}

	}
	int k = 0;
	@Override
	public ChunkData getNextChunck() throws IOException {
		if (ht == null || !ht.hasMoreElements()) {
			StringResult rs;
			try {
				rs = dl.getStringTokenizer();
			} catch (Exception e) {
				throw new IOException(e);
			}
			if (rs == null) {
				SDFSLogger.getLog().debug("no more " + k);
				return null;
			} else {
				k++;
			}
			ht = rs.st;
			hid = toIntExact(rs.id);
		}
		String [] tk = ht.nextToken().split(":");
		iterBuf.position(0);
		iterBuf.putInt(hid);
		iterBuf.putInt(Integer.parseInt(tk[1]));
		iterBuf.position(0);
		ChunkData chk = new ChunkData(BaseEncoding.base64().decode(
				tk[0]), iterBuf.getLong());
		return chk;

	}

	private String[] getStrings(S3Object sobj) throws IOException {
		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;
		Map<String, String> mp = sobj.getObjectMetadata().getUserMetadata();
		int size = Integer.parseInt((String) mp.get("size"));
		if (mp.containsKey("encrypt")) {
			encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
		}
		if (mp.containsKey("compress")) {
			compress = Boolean.parseBoolean((String) mp.get("compress"));
		} else if (mp.containsKey("lz4compress")) {

			lz4compress = Boolean.parseBoolean((String) mp.get("lz4compress"));
		}
		int cl = (int) sobj.getObjectMetadata().getContentLength();

		byte[] data = new byte[cl];
		DataInputStream in = null;
		try {
			in = new DataInputStream(sobj.getObjectContent());
			in.readFully(data);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				in.close();
			} catch (Exception e) {
			}
		}
		if (encrypt)
			data = EncryptUtils.decryptCBC(data);
		if (compress)
			data = CompressionUtils.decompressZLIB(data);
		else if (lz4compress) {
			data = CompressionUtils.decompressLz4(data, size);
		}
		String hast = new String(data);
		SDFSLogger.getLog().debug(
				"reading hashes " + (String) mp.get("hashes") + " from "
						+ sobj.getKey());
		String[] st = hast.split(",");
		return st;
	}

	private int getClaimedObjects(S3Object sobj, long id) throws Exception,
			IOException {

		Map<String, String> mp = sobj.getObjectMetadata().getUserMetadata();
		if (!mp.containsKey("encrypt")) {
			mp = s3Service.getObjectMetadata(this.name, sobj.getKey())
					.getUserMetadata();
		}
		String[] st = this.getStrings(sobj);
		int claims = 0;
		for (String ha : st) {
			byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
			long cid = HCServiceProxy.getHashesMap().get(b);
			if (cid == id)
				claims++;
		}
		return claims;

	}

	MultiDownload dl = null;
	StringTokenizer ht = null;
	ObjectListing ck = null;
	ByteBuffer iterBuf = ByteBuffer.allocate(8);
	int hid;

	@Override
	public void iterationInit(boolean deep) {
		try {
			resetLength();
			this.ht = null;
			this.hid = 0;
			iterBuf = ByteBuffer.allocate(8);
			dl = new MultiDownload(this);
			dl.iterationInit(false, "/keys");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}

	}

	protected void resetLength() {
		HashBlobArchiveNoMap.currentLength.set(0);
		HashBlobArchiveNoMap.compressedLength.set(0);
	}

	@Override
	public long getFreeBlocks() {
		return 0;
	}

	@Override
	public long maxSize() {
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		return HashBlobArchiveNoMap.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);

		try {
			s3Service.getObject(this.name, "blocks/" + haName);
			return true;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found")
					&& !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return false;
		}
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc,long id) throws IOException {
		throw new IOException("not implemented");

	}

	private byte[] getData(long id) throws Exception {
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		boolean compress = false;
		try {
			long tm = System.currentTimeMillis();
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name,
					"blocks/" + haName);
			int cl = (int) omd.getContentLength();
			byte[] data = new byte[cl];
			int bz = 0;
			if (this.genericS3)
				bz = cl;
			else
				bz = (int) Math.ceil(cl / (double) downloadBlockSize);
			final int dobjSz = bz;

			AbstractDownloadListener l = new AbstractDownloadListener() {
				@Override
				public void commandException(Exception e) {

					this.incrementAndGetDNEX();
					SDFSLogger.getLog().debug(
							"Error while getting data block ", e);

					synchronized (this) {
						this.notifyAll();
					}
				}

				@Override
				public void commandResponse(DownloadShard result) {
					if (this.incrementandGetDN() >= dobjSz) {

						synchronized (this) {
							this.notifyAll();
						}
					}
				}

			};
			int remaining = cl;
			DownloadShard[] shs = new DownloadShard[dobjSz];
			int i = 0;
			while (remaining > 0) {
				int dsz = downloadBlockSize;
				if (this.genericS3)
					dsz = -1;
				else if (remaining < downloadBlockSize)
					dsz = remaining;
				DownloadShard sh = new DownloadShard(l, "blocks/" + haName, cl
						- remaining, dsz, s3Service, this.name);
				executor.execute(sh);
				remaining -= dsz;
				shs[i] = sh;
				i++;
			}
			synchronized (l) {
				try {
					l.wait();
				} catch (InterruptedException e1) {
					throw new IOException(e1);
				}
			}
			if (l.getDAR() != null) {
				throw l.getDAR();
			}

			if (l.getDNEX() > 0) {
				for (DownloadShard sh : shs) {
					sh.close();
				}
				throw new IOException("unable to download all shards for " + id);
			}
			ByteBuffer buf = ByteBuffer.wrap(data);
			for (DownloadShard sh : shs) {
				buf.put(sh.b);
			}
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			byte[] shash = BaseEncoding.base16().decode(
					omd.getETag().toUpperCase());
			byte[] chash = ServiceUtils.computeMD5Hash(data);
			if (!Arrays.equals(shash, chash))
				throw new IOException("download corrupt at " + id);
			Map<String, String> mp = omd.getUserMetadata();
			
			if (mp.containsKey("compress")) {
				compress = Boolean.parseBoolean((String) mp.get("compress"));
			}

			tm = System.currentTimeMillis();
			if (compress)
				data = CompressionUtils.decompressZLIB(data);
			if (mp.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean((String) mp.get("deleted"));
				if (del) {
					S3Object kobj = s3Service.getObject(this.name, "keys/"
							+ haName);

					int claims = this.getClaimedObjects(kobj, id);

					int delobj = 0;
					if (mp.containsKey("deleted-objects")) {
						delobj = Integer.parseInt((String) mp
								.get("deleted-objects")) - claims;
						if (delobj < 0)
							delobj = 0;
					}
					mp.remove("deleted");
					mp.put("deleted-objects", Integer.toString(delobj));
					mp.put("suspect", "true");
					omd.setUserMetadata(mp);
					CopyObjectRequest req = new CopyObjectRequest(this.name,
							"keys/" + haName, this.name, "keys/" + haName)
							.withNewObjectMetadata(omd);
					s3Service.copyObject(req);
					int _size = Integer.parseInt((String) mp.get("size"));
					int _compressedSize = Integer.parseInt((String) mp
							.get("compressedsize"));
					HashBlobArchiveNoMap.currentLength.addAndGet(_size);
					HashBlobArchiveNoMap.compressedLength.addAndGet(_compressedSize);
					SDFSLogger.getLog().warn(
							"Reclaimed [" + claims
									+ "] blocks marked for deletion");
					kobj.close();
				}
			}
			dtm = (System.currentTimeMillis() - tm) / 1000d;
			bps = (cl / 1024) / dtm;
			return data;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState"))
				throw new DataArchivedException(id, null);
			else {
				SDFSLogger.getLog().error(
						"unable to get block [" + id + "] at [blocks/" + haName
								+ "]", e);
				throw e;

			}
		}
	}

	@Override
	public byte[] getBytes(long id) throws IOException, DataArchivedException {
		Exception e = null;
		for (int i = 0; i < 5; i++) {
			try {
				return this.getData(id);
			} catch (DataArchivedException e1) {
				throw e1;
			} catch (Exception e1) {
				e = e1;
			}
		}
		throw new IOException(e);

	}

	private int verifyDelete(int id, ObjectMetadata obj) throws IOException,
			Exception {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);
		int claims = this.getClaimedObjects(kobj, id);
		try {

			if (claims > 0) {
				Map<String, String> mp = obj.getUserMetadata();
				int delobj = 0;
				if (mp.containsKey("deleted-objects")) {
					delobj = Integer.parseInt((String) mp
							.get("deleted-objects")) - claims;
					if (delobj < 0)
						delobj = 0;
				}
				mp.remove("deleted");
				mp.put("deleted-objects", Integer.toString(delobj));
				mp.put("suspect", "true");
				obj.setUserMetadata(mp);
				CopyObjectRequest req = new CopyObjectRequest(this.name,
						"keys/" + haName, this.name, "keys/" + haName)
						.withNewObjectMetadata(obj);
				s3Service.copyObject(req);
				int _size = Integer.parseInt((String) mp.get("size"));
				int _compressedSize = Integer.parseInt((String) mp
						.get("compressedsize"));
				HashBlobArchiveNoMap.currentLength.addAndGet(_size);
				HashBlobArchiveNoMap.compressedLength.addAndGet(_compressedSize);
				SDFSLogger.getLog()
						.warn("Reclaimed [" + claims
								+ "] blocks marked for deletion");

			}

			if (claims == 0) {
				s3Service.deleteObject(this.name, "blocks/" + haName);
				s3Service.deleteObject(this.name, "keys/" + haName);
				SDFSLogger.getLog().debug(
						"deleted block " + "blocks/" + haName + " id " + id);
			}
		} finally {
			try {
				kobj.close();
			} catch (Exception e) {
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
					ObjectMetadata omd = s3Service.getObjectMetadata(name,
							"bucketinfo");
					HashMap<String, String> md = new HashMap<String, String>();
					md.put("currentsize",
							Long.toString(HashBlobArchiveNoMap.currentLength.get()));
					md.put("currentcompressedsize",
							Long.toString(HashBlobArchiveNoMap.compressedLength.get()));
					omd.setUserMetadata(md);
					
					CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
							name, "bucketinfo", name, "bucketinfo")
							.withNewObjectMetadata(omd);
					s3Service.copyObject(copyObjectRequest);
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to update metadata", e);
				}
				if (this.deletes.size() > 0) {
					this.delLock.lock();
					HashMap<Integer, Integer> odel = null;
					try {
						odel = this.deletes;
						this.deletes = new HashMap<Integer, Integer>();
						// SDFSLogger.getLog().info("delete hash table size of "
						// + odel.size());
					} finally {
						this.delLock.unlock();
					}
					Set<Integer> iter = odel.keySet();
					for (Integer k : iter) {
						String hashString = EncyptUtils
								.encHashArchiveName(k.intValue(),
										Main.chunkStoreEncryptionEnabled);
						try {
							ObjectMetadata obj = s3Service.getObjectMetadata(
									this.name, "keys/" + hashString);
							Map<String, String> mp = obj.getUserMetadata();
							int objects = Integer.parseInt((String) mp
									.get("objects"));
							int delobj = 0;
							if (mp.containsKey("deleted-objects"))
								delobj = Integer.parseInt((String) mp
										.get("deleted-objects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							delobj = delobj + odel.get(k);
							if (objects <= delobj) {
								// SDFSLogger.getLog().info("deleting " +
								// hashString);
								int size = Integer.parseInt((String) mp
										.get("bsize"));
								int compressedSize = Integer
										.parseInt((String) mp
												.get("bcompressedsize"));
								HashBlobArchiveNoMap.currentLength.addAndGet(-1 * size);
								HashBlobArchiveNoMap.compressedLength.addAndGet(-1
										* compressedSize);
								if (this.deleteUnclaimed) {
									this.verifyDelete(k.intValue(), obj);
								} else {
									mp.put("deleted", "true");
									mp.put("deleted-objects",
											Integer.toString(delobj));
									obj.setUserMetadata(mp);
									CopyObjectRequest req = new CopyObjectRequest(
											this.name, "keys/" + hashString,
											this.name, "keys/" + hashString)
											.withNewObjectMetadata(obj);
									s3Service.copyObject(req);
								}
								HashBlobArchiveNoMap.removeCache(k.intValue());
							} else {
								mp.put("deleted-objects",
										Integer.toString(delobj));
								obj.setUserMetadata(mp);
								CopyObjectRequest req = new CopyObjectRequest(
										this.name, "keys/" + hashString,
										this.name, "keys/" + hashString)
										.withNewObjectMetadata(obj);
								s3Service.copyObject(req);
							}
						} catch (Exception e) {
							SDFSLogger.getLog().warn(
									"Unable to delete object " + hashString, e);
						} finally {
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

	public Iterator<String> getNextObjectList() {
		List<String> keys = new ArrayList<String>();
		if(ck==null)
			ck = s3Service.listObjects(this.getName(), "keys/");
		else if (ck.isTruncated()) {
			ck = s3Service.listNextBatchOfObjects(ck);
		} else {
			return keys.iterator();
		}
		List<S3ObjectSummary> objs = ck.getObjectSummaries();
		for (S3ObjectSummary obj : objs) {
			keys.add(obj.getKey());
		}
		return keys.iterator();
	}

	public StringResult getStringResult(String key) throws IOException,
			InterruptedException {
		S3Object sobj = null;
		ObjectMetadata md = null;
		try {
			sobj = s3Service.getObject(getName(), key);
			md = sobj.getObjectMetadata();
		} catch (Exception e) {
			throw new IOException(e);
		}
		int cl = (int) md.getContentLength();

		byte[] data = new byte[cl];
		DataInputStream in = null;
		try {
			in = new DataInputStream(sobj.getObjectContent());
			in.readFully(data);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (in != null)
				in.close();
		}
		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;
		int size = Integer.parseInt(md.getUserMetaDataOf("size"));
		Map<String, String> mp = md.getUserMetadata();
		encrypt = Boolean.parseBoolean((String) sobj.getObjectMetadata()
				.getUserMetaDataOf("encrypt"));

		lz4compress = Boolean.parseBoolean((String) md
				.getUserMetaDataOf("lz4compress"));
		boolean changed = false;
		if (mp.containsKey("deleted")) {
			mp.remove("deleted");
			changed = true;
		}
		if (mp.containsKey("deleted-objects")) {
			mp.remove("deleted-objects");
			changed = true;
		}

		if (encrypt) {
			data = EncryptUtils.decryptCBC(data);
		}
		if (compress)
			data = CompressionUtils.decompressZLIB(data);
		else if (lz4compress) {
			data = CompressionUtils.decompressLz4(data, size);
		}

		Long bid = EncyptUtils.decHashArchiveName(sobj.getKey().substring(5),
				encrypt);

		String hast = new String(data);
		SDFSLogger.getLog()
				.info("reading hashes " + (String) mp.get("objects")
						+ " from " + bid + " encn " + sobj.getKey().substring(5));
		StringTokenizer sht = new StringTokenizer(hast, ",");
		StringResult st = new StringResult();
		st.id = bid;
		st.st = sht;
		if (mp.containsKey("bsize")) {
			HashBlobArchiveNoMap.currentLength.addAndGet(Integer.parseInt(mp.get("bsize")));
		}
		if (mp.containsKey("bcompressedsize")) {
			HashBlobArchiveNoMap.compressedLength.addAndGet(Integer.parseInt(mp
					.get("bcompressedsize")));
		}
		if (changed) {
			try {
				md = sobj.getObjectMetadata();
				md.setUserMetadata(mp);
				CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
						getName(), sobj.getKey(), getName(), sobj.getKey())
						.withNewObjectMetadata(md);
				s3Service.copyObject(copyObjectRequest);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		return st;
	}

	@Override
	public void sync() throws IOException {
		HashBlobArchiveNoMap.sync();
	}

	private long getLastModified(String st) {

		try {
			ObjectMetadata obj = s3Service.getObjectMetadata(this.name, st);
			Map<String, String> metaData = obj.getUserMetadata();
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
		if (f.lastModified() == this.getLastModified(pth))
			return;
		boolean isDir = false;
		boolean isSymlink = false;
		if (!OSValidator.isWindows()) {
			isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			isSymlink = Files.readAttributes(f.toPath(),
					PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		} else {
			isDir = f.isDirectory();
		}
		if (isSymlink) {
			try {
				HashMap<String, String> metaData = new HashMap<String, String>();
				metaData.put("encrypt",
						Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				String slp = EncyptUtils.encString(
						Files.readSymbolicLink(f.toPath()).toFile().getPath(),
						Main.chunkStoreEncryptionEnabled);
				metaData.put("symlink", slp);
				ObjectMetadata md = new ObjectMetadata();
				md.setContentType("binary/octet-stream");
				md.setContentLength(pth.getBytes().length);
				md.setUserMetadata(metaData);
				PutObjectRequest req = new PutObjectRequest(this.name, pth,
						new ByteArrayInputStream(pth.getBytes()), md);
				s3Service.putObject(req);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else if (isDir) {
			HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
					Main.chunkStoreEncryptionEnabled);
			metaData.put("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			metaData.put("lastmodified", Long.toString(f.lastModified()));
			metaData.put("directory", "true");
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("binary/octet-stream");
			md.setContentLength(pth.getBytes().length);
			md.setUserMetadata(metaData);
			try {
				PutObjectRequest req = new PutObjectRequest(this.name, pth,
						new ByteArrayInputStream(pth.getBytes()), md);
				s3Service.putObject(req);
			} catch (Exception e1) {
				SDFSLogger.getLog().error("error uploading", e1);
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
				String objName = (pth);
				ObjectMetadata md = new ObjectMetadata();
				Map<String, String> umd = FileUtils.getFileMetaData(f,
						Main.chunkStoreEncryptionEnabled);
				md.setUserMetadata(umd);
				md.addUserMetadata("lz4compress",
						Boolean.toString(Main.compress));
				md.addUserMetadata("encrypt",
						Boolean.toString(Main.chunkStoreEncryptionEnabled));
				md.addUserMetadata("lastmodified",
						Long.toString(f.lastModified()));

				md.setContentType("binary/octet-stream");
				in = new BufferedInputStream(new FileInputStream(p), 32768);
				try {
					byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
					in.close();
					md.setContentMD5(BaseEncoding.base64().encode(md5Hash));
					in = new BufferedInputStream(new FileInputStream(p), 32768);
				} catch (NoSuchAlgorithmException e2) {
					SDFSLogger.getLog().error("while hashing", e2);
					throw new IOException(e2);
				}
				md.setContentLength(p.length());
				try {
					PutObjectRequest req = new PutObjectRequest(this.name,
							objName, in, md);
					s3Service.putObject(req);
				} catch (Exception e1) {
					// SDFSLogger.getLog().error("error uploading", e1);
					throw new IOException(e1);
				}
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
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
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
		if (nm.startsWith(File.separator))
			nm = nm.substring(1);
		String haName = EncyptUtils.encString(nm,
				Main.chunkStoreEncryptionEnabled);
		S3Object obj = null;
		try {
			obj = s3Service.getObject(this.name, pp + "/" + haName);
			BufferedInputStream in = new BufferedInputStream(
					obj.getObjectContent());
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(p));
			IOUtils.copy(in, out);
			out.flush();
			out.close();
			in.close();
			byte[] shash = BaseEncoding.base16().decode(
					obj.getObjectMetadata().getETag().toUpperCase());
			if (!FileUtils.fileValid(p, shash))
				throw new IOException("file " + p.getPath() + " is corrupt");
			boolean encrypt = false;
			boolean lz4compress = false;
			Map<String, String> mp = obj.getObjectMetadata().getUserMetadata();
			if (mp.containsKey("encrypt")) {
				encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
			}
			if (mp.containsKey("lz4compress")) {
				lz4compress = Boolean.parseBoolean((String) mp
						.get("lz4compress"));
			}
			if (mp.containsKey("symlink")) {
				if (OSValidator.isWindows())
					throw new IOException(
							"unable to restore symlinks to windows");
				else {
					String spth = EncyptUtils.decString(mp.get("symlink"),
							encrypt);
					Path srcP = Paths.get(spth);
					Path dstP = Paths.get(to.getPath());
					Files.createSymbolicLink(dstP, srcP);
				}
			} else if (mp.containsKey("directory")) {
				to.mkdirs();
				FileUtils.setFileMetaData(to, mp, encrypt);
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
				File parent = to.getParentFile();
				if (!parent.exists())
					parent.mkdirs();
				BufferedInputStream is = new BufferedInputStream(
						new FileInputStream(p));
				BufferedOutputStream os = new BufferedOutputStream(
						new FileOutputStream(to));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				FileUtils.setFileMetaData(to, mp, encrypt);
			}

		} catch (Exception e1) {
			throw new IOException(e1);
		} finally {
			if (obj != null)
				obj.close();
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
			s3Service.deleteObject(this.name, pp + "/" + haName);
		} catch (Exception e1) {
			throw new IOException(e1);
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
			CopyObjectRequest req = new CopyObjectRequest(this.name, pp + "/"
					+ fn, this.name, pp + "/" + tn);
			s3Service.copyObject(req);
			s3Service.deleteObject(this.name, pp + "/" + fn);
		} catch (Exception e1) {
			throw new IOException(e1);
		}
	}

	ObjectListing nck = null;
	List<S3ObjectSummary> nsummaries = null;
	int nobjPos = 0;

	public void clearIter() {
		nck = null;
		nobjPos = 0;
		nsummaries = null;
	}

	public String getNextName(String pp) throws IOException {
		try {
			String pfx = pp + "/";
			if (nck == null) {
				nck = s3Service.listObjects(this.getName(), pfx);
				nsummaries = nck.getObjectSummaries();
				nobjPos = 0;
			} else if (nobjPos == nsummaries.size()) {
				nck = s3Service.listNextBatchOfObjects(nck);

				nsummaries = nck.getObjectSummaries();
				nobjPos = 0;
			}

			if (nsummaries.size() == 0)
				return null;
			ObjectMetadata sobj = null;

			sobj = s3Service.getObjectMetadata(this.name,
					nsummaries.get(nobjPos).getKey());
			Map<String, String> mp = sobj.getUserMetadata();
			boolean encrypt = false;
			if (mp.containsKey("encrypt")) {
				encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
			}
			String pt = nsummaries.get(nobjPos).getKey()
					.substring(pfx.length());
			String fname = EncyptUtils.decString(pt, encrypt);
			nobjPos++;
			return fname;
			/*
			 * this.downloadFile(fname, new File(to.getPath() + File.separator +
			 * fname), pp);
			 */

		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, Integer> getHashMap(long id) throws IOException {

		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);
		try {
			String[] ks = this.getStrings(kobj);
			HashMap<String, Integer> m = new HashMap<String, Integer>(ks.length);
			for (String k : ks) {
				String[] kv = k.split(":");
				m.put(kv[0], Integer.parseInt(kv[1]));
			}

			return m;
		} finally {
			kobj.close();
		}

	}

	@Override
	public boolean checkAccess() {
		Exception e = null;
		for (int i = 0; i < 3; i++) {
			try {
				ObjectMetadata omd = s3Service.getObjectMetadata(this.name,
						"bucketinfo");
				Map<String, String> obj = omd.getUserMetadata();
				obj.get("currentsize");
				return true;
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
		HashBlobArchiveNoMap.setReadSpeed((double) kbps);

	}

	@Override
	public void setWriteSpeed(int kbps) {
		HashBlobArchiveNoMap.setWriteSpeed((double) kbps);

	}

	@Override
	public void setCacheSize(long sz) throws IOException {
		HashBlobArchiveNoMap.setCacheSize(sz);
	}

	@Override
	public int getReadSpeed() {
		return (int) HashBlobArchiveNoMap.getReadSpeed();
	}

	@Override
	public int getWriteSpeed() {
		return (int) HashBlobArchiveNoMap.getWriteSpeed();
	}

	@Override
	public long getCacheSize() {
		return HashBlobArchiveNoMap.getCacheSize();
	}

	@Override
	public long getMaxCacheSize() {
		return HashBlobArchiveNoMap.getLocalCacheSize();
	}

	@Override
	public synchronized String restoreBlock(long id, byte[] hash)
			throws IOException {
		if(id== -1) {
			SDFSLogger.getLog().warn("Hash not found for " + StringUtils.getHexString(hash));
			return null;
		}
		ByteBuffer bf = ByteBuffer.allocate(8);
		bf.putLong(id);
		bf.position(0);
		int hbid = bf.getInt();
		String haName = this.restoreRequests.get(new Integer(hbid));
		if (haName == null)
			haName = EncyptUtils.encHashArchiveName(hbid,
					Main.chunkStoreEncryptionEnabled);
		else if (haName.equalsIgnoreCase("InvalidObjectState"))
			return null;
		else {
			return haName;
		}
		try {
			RestoreObjectRequest request = new RestoreObjectRequest(this.name,
					"blocks/" + haName, 2);
			s3Service.restoreObject(request);
			
			if (blockRestored(haName)) {
				restoreRequests.put(new Integer(hbid), "InvalidObjectState");
				return null;
			}
			restoreRequests.put(new Integer(hbid), haName);
			return haName;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {

				restoreRequests.put(new Integer(hbid), "InvalidObjectState");
				return null;
			}
			if (e.getErrorCode().equalsIgnoreCase("RestoreAlreadyInProgress")) {

				restoreRequests.put(new Integer(hbid), haName);
				return haName;
			} else {
				SDFSLogger.getLog().error(
						"Error while restoring block " + e.getErrorCode() + " id=" + hbid + " name=blocks/" + haName);
				throw e;
			}
		}

	}

	@Override
	public boolean blockRestored(String id) {
		ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/"
				+ id);
		if (omd.getOngoingRestore())
			return false;
		else
			return true;

	}

	@Override
	public boolean checkAccess(String username, String password,
			Properties props) throws Exception {
		BasicAWSCredentials _cred = new BasicAWSCredentials(username, password);
		if (props.containsKey("default-bucket-location")) {
			bucketLocation = RegionUtils.getRegion(props
					.getProperty("default-bucket-location"));
		}

		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(Main.dseIOThreads * 2);
		clientConfig.setConnectionTimeout(10000);
		clientConfig.setSocketTimeout(10000);
		String s3Target = null;

		if (props.containsKey("s3-target")) {
			s3Target = props.getProperty("s3-target");
		}
		if (props.containsKey("proxy-host")) {
			clientConfig.setProxyHost(props.getProperty("proxy-host"));
		}
		if (props.containsKey("proxy-domain")) {
			clientConfig.setProxyDomain(props.getProperty("proxy-domain"));
		}
		if (props.containsKey("proxy-password")) {
			clientConfig.setProxyPassword(props.getProperty("proxy-password"));
		}
		if (props.containsKey("proxy-port")) {
			clientConfig.setProxyPort(Integer.parseInt(props
					.getProperty("proxy-port")));
		}
		if (props.containsKey("proxy-username")) {
			clientConfig.setProxyUsername(props.getProperty("proxy-username"));
		}
		s3Service = new AmazonS3Client(_cred, clientConfig);
		if (s3Target != null) {
			TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
				@Override
				public boolean isTrusted(X509Certificate[] certificate,
						String authType) {
					return true;
				}
			};
			SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy,
					SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			clientConfig.getApacheHttpClientConfig().withSslSocketFactory(sf);
			s3Service.setEndpoint(s3Target);
		}
		s3Service.listBuckets();
		return true;
	}

	@Override
	public void recoverVolumeConfig(String name, File to, String parentPath,
			String accessKey, String secretKey, String bucket, Properties props)
			throws IOException {
		boolean done = false;
		try {
			if (to.exists())
				throw new IOException("file exists " + to.getPath());
			BasicAWSCredentials _cred = new BasicAWSCredentials(accessKey,
					secretKey);
			AmazonS3Client s3 = null;
			boolean encrypt = Boolean.parseBoolean(props.getProperty("encrypt",
					"false"));
			String keyStr = null;
			String ivStr = null;
			if (encrypt) {
				keyStr = props.getProperty("key");
				ivStr = props.getProperty("iv");
			}
			ClientConfiguration clientConfig = new ClientConfiguration();
			clientConfig.setMaxConnections(Main.dseIOThreads * 2);
			clientConfig.setConnectionTimeout(10000);
			clientConfig.setSocketTimeout(10000);
			String s3Target = null;

			if (props.containsKey("s3-target")) {
				s3Target = props.getProperty("s3-target");
			}
			if (props.containsKey("proxy-host")) {
				clientConfig.setProxyHost(props.getProperty("proxy-host"));
			}
			if (props.containsKey("proxy-domain")) {
				clientConfig.setProxyDomain(props.getProperty("proxy-domain"));
			}
			if (props.containsKey("proxy-password")) {
				clientConfig.setProxyPassword(props
						.getProperty("proxy-password"));
			}
			if (props.containsKey("proxy-port")) {
				clientConfig.setProxyPort(Integer.parseInt(props
						.getProperty("proxy-port")));
			}
			if (props.containsKey("proxy-username")) {
				clientConfig.setProxyUsername(props
						.getProperty("proxy-username"));
			}
			s3 = new AmazonS3Client(_cred, clientConfig);
			if (s3Target != null) {
				TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate,
							String authType) {
						return true;
					}
				};
				SSLSocketFactory sf = new SSLSocketFactory(
						acceptingTrustStrategy,
						SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
				clientConfig.getApacheHttpClientConfig().withSslSocketFactory(
						sf);
				s3.setEndpoint(s3Target);
			}
			ObjectListing ol = s3.listObjects(bucket, "volume/");
			List<S3ObjectSummary> sms = ol.getObjectSummaries();
			for (S3ObjectSummary sm : sms) {
				S3Object zobj = s3.getObject(bucket, sm.getKey());
				Map<String, String> md = zobj.getObjectMetadata()
						.getUserMetadata();
				String vn = sm.getKey().substring("volume/".length());
				if (md.containsKey("encrypt")) {
					vn = new String(EncryptUtils.decryptCBC(BaseEncoding
							.base64Url().decode(vn), keyStr, ivStr));
				}
				zobj.close();
				if (vn.equalsIgnoreCase(name + "-volume-cfg.xml")) {
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
					S3Object obj = null;
					try {

						obj = s3.getObject(bucket, sm.getKey());
						BufferedInputStream in = new BufferedInputStream(
								obj.getObjectContent());
						BufferedOutputStream out = new BufferedOutputStream(
								new FileOutputStream(p));
						IOUtils.copy(in, out);
						out.flush();
						out.close();
						in.close();
						boolean enc = false;
						boolean lz4compress = false;
						Map<String, String> mp = obj.getObjectMetadata()
								.getUserMetadata();
						if (mp.containsKey("encrypt")) {
							enc = Boolean.parseBoolean((String) mp
									.get("encrypt"));
						}
						if (mp.containsKey("lz4compress")) {
							lz4compress = Boolean.parseBoolean((String) mp
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
						File parent = to.getParentFile();
						if (!parent.exists())
							parent.mkdirs();
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
						if (obj != null)
							obj.close();
						p.delete();
						z.delete();
						e.delete();
					}
					done = true;
					break;
				}

			}
		} catch (Exception e) {
			throw new IOException(e);
		}
		if (!done)
			throw new IOException("Volume [" + name + "] not found");
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
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void writeHashBlobArchive(HashBlobArchiveNoMap arc, int id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);

		try {
			byte[] chunks = arc.getBytes();
			int csz = chunks.length;
			ObjectMetadata md = new ObjectMetadata();
			md.addUserMetadata("size", Integer.toString(arc.uncompressedLength.get()));

			md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
			md.addUserMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			md.addUserMetadata("compressedsize",
					Integer.toString(chunks.length));
			md.addUserMetadata("bsize", Integer.toString(arc.getLen()));
			md.addUserMetadata("objects", Integer.toString(arc.getSz()));
			md.setContentType("binary/octet-stream");
			md.setContentLength(chunks.length);
			md.setContentMD5(BaseEncoding.base64().encode(
					ServiceUtils.computeMD5Hash(chunks)));
			PutObjectRequest req = new PutObjectRequest(this.name, "blocks/"
					+ haName, new ByteArrayInputStream(chunks), md);
			s3Service.putObject(req);
			byte[] hs = arc.getHashesString().getBytes();
			int sz = hs.length;
			if (Main.compress) {
				hs = CompressionUtils.compressLz4(hs);
			}
			if (Main.chunkStoreEncryptionEnabled) {
				hs = EncryptUtils.encryptCBC(hs);
			}
			md = new ObjectMetadata();
			md.addUserMetadata("size", Integer.toString(sz));
			md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
			md.addUserMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			md.addUserMetadata("compressedsize",
					Integer.toString(chunks.length));
			md.addUserMetadata("bsize", Integer.toString(arc.uncompressedLength.get()));
			md.addUserMetadata("bcompressedsize", Integer.toString(csz));
			md.addUserMetadata("objects", Integer.toString(arc.getSz()));

			md.setContentType("binary/octet-stream");
			md.setContentLength(hs.length);
			md.setContentMD5(BaseEncoding.base64().encode(
					ServiceUtils.computeMD5Hash(hs)));
			req = new PutObjectRequest(this.name, "keys/" + haName,
					new ByteArrayInputStream(hs), md);
			s3Service.putObject(req);
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to upload " + arc.getID() + " with id " + id, e);
			throw new IOException(e);
		} finally {

		}
		
	}

}
