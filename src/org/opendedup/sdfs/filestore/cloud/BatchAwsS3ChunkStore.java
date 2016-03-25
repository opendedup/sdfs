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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

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
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
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
import org.opendedup.sdfs.filestore.StringResult;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.filestore.cloud.utils.FileUtils;

import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

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
public class BatchAwsS3ChunkStore implements AbstractChunkStore,
		AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private static BasicAWSCredentials awsCredentials = null;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	private String name;
	private com.amazonaws.regions.Region bucketLocation = null;
	AmazonS3Client s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean md5sum = true;
	private int glacierDays = 0;
	File staged_sync_location = new File(Main.chunkStore + File.separator
			+ "syncstaged");
	private boolean genericS3 = false;
	private WeakHashMap<Long, String> restoreRequests = new WeakHashMap<Long, String>();

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

	public BatchAwsS3ChunkStore() {

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
			md.put("currentsize",
					Long.toString(HashBlobArchive.currentLength.get()));
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
		return HashBlobArchive.getBlock(hash, start);

	}

	public void cacheData(byte[] hash, long start, int len) throws IOException,
			DataArchivedException {
		try {
			HashBlobArchive.cacheArchive(hash, start);
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
			if (this.deletes.containsKey(start)) {
				int sz = this.deletes.get(start) + 1;
				this.deletes.put(start, sz);
			} else
				this.deletes.put(start, 1);

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
			if (config.hasAttribute("simple-s3")) {
				this.md5sum = !Boolean.parseBoolean(config
						.getAttribute("simple-s3"));
				EncyptUtils.baseEncode = Boolean.parseBoolean(config
						.getAttribute("simple-s3"));
				System.setProperty(
						"com.amazonaws.services.s3.disableGetObjectMD5Validation",
						"true");
				System.setProperty(
						"com.amazonaws.services.s3.disablePutObjectMD5Validation",
						"true");
			}
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
				byte[] sz = "bucketinfodatanow".getBytes();
				md.setContentLength(sz.length);

				s3Service.putObject(this.name, "bucketinfo",
						new ByteArrayInputStream(sz), md);
			} else {
				Map<String, String> obj = null;
				ObjectMetadata omd = null;
				try {
					omd = s3Service.getObjectMetadata(this.name, "bucketinfo");
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
					byte[] sz = "bucketinfodatanow".getBytes();
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
						CopyObjectRequest reg = new CopyObjectRequest(
								this.name, "bucketinfo", this.name,
								"bucketinfo").withNewObjectMetadata(omd);
						s3Service.copyObject(reg);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"unable to update bucket info in init", e);
						SDFSLogger.getLog().info("created new store " + name);
						ObjectMetadata md = new ObjectMetadata();
						md.addUserMetadata("currentsize", "0");
						md.addUserMetadata("currentcompressedsize", "0");
						byte[] sz = "bucketinfodatanow".getBytes();
						md.setContentLength(sz.length);
						s3Service.putObject(this.name, "bucketinfo",
								new ByteArrayInputStream(sz), md);
					}
				}
			}
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
			} else if (!this.genericS3) {
				s3Service.deleteBucketLifecycleConfiguration(this.name);
			}
			HashBlobArchive.init(this);
			HashBlobArchive.setReadSpeed(rsp);
			HashBlobArchive.setWriteSpeed(wsp);
			Thread th = new Thread(this);
			th.start();
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	int k = 0;

	@Override
	public ChunkData getNextChunck() throws IOException {
		synchronized (this) {
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
				hid = rs.id;
			}
			String tk = ht.nextToken();
			SDFSLogger.getLog().debug(
					"hid="
							+ hid
							+ " val="
							+ StringUtils.getHexString(BaseEncoding.base64()
									.decode(tk.split(":")[0])));
			ChunkData chk = new ChunkData(BaseEncoding.base64().decode(
					tk.split(":")[0]), hid);
			return chk;
		}

	}

	private String[] getStrings(S3Object sobj) throws IOException {
		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;

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
		Map<String, String> mp = this.getUserMetaData(sobj.getObjectMetadata());
		int size = Integer.parseInt((String) mp.get("size"));
		if (mp.containsKey("encrypt")) {
			encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
		}
		if (mp.containsKey("compress")) {
			compress = Boolean.parseBoolean((String) mp.get("compress"));
		} else if (mp.containsKey("lz4compress")) {

			lz4compress = Boolean.parseBoolean((String) mp.get("lz4compress"));
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

		Map<String, String> mp = this.getUserMetaData(sobj.getObjectMetadata());
		if (!mp.containsKey("encrypt")) {
			mp = this.getUserMetaData(s3Service.getObjectMetadata(this.name,
					sobj.getKey()));
		}
		String[] st = this.getStrings(sobj);
		int claims = 0;
		for (String ha : st) {
			byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
			if (HCServiceProxy.getHashesMap().containsKey(b))
				claims++;
		}
		return claims;

	}

	MultiDownload dl = null;
	StringTokenizer ht = null;
	ObjectListing ck = null;

	long hid;

	@Override
	public void iterationInit(boolean deep) {
		try {
			resetLength();
			this.ht = null;
			this.hid = 0;
			dl = new MultiDownload(this);
			dl.iterationInit(false, "/keys");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}

	}

	protected void resetLength() {
		HashBlobArchive.currentLength.set(0);
		HashBlobArchive.compressedLength.set(0);
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
		return HashBlobArchive.compressedLength.get();
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

	private String getClaimName(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		return "claims/"
				+ haName
				+ "/"
				+ EncyptUtils.encHashArchiveName(Main.volume.getSerialNumber(),
						Main.chunkStoreEncryptionEnabled);
	}

	private ObjectMetadata getClaimMetaData(long id) throws IOException {
		try {
			ObjectMetadata md = s3Service.getObjectMetadata(this.name,
					this.getClaimName(id));
			return md;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found")
					&& !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return null;
		}
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id)
			throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);

		try {
			byte[] chunks = arc.getBytes();
			int csz = chunks.length;
			ObjectMetadata md = new ObjectMetadata();
			md.addUserMetadata("size",
					Integer.toString(arc.uncompressedLength.get()));

			md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
			md.addUserMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			md.addUserMetadata("compressedsize",
					Integer.toString(chunks.length));
			md.addUserMetadata("bsize", Integer.toString(arc.getLen()));
			md.addUserMetadata("objects", Integer.toString(arc.getSz()));
			md.setContentType("binary/octet-stream");
			md.setContentLength(chunks.length);
			if (md5sum)
				md.setContentMD5(BaseEncoding.base64().encode(
						ServiceUtils.computeMD5Hash(chunks)));
			PutObjectRequest req = new PutObjectRequest(this.name, "blocks/"
					+ haName, new ByteArrayInputStream(chunks), md);
			byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();
			s3Service.putObject(req);
			md.setContentLength(msg.length);
			md.setContentMD5(BaseEncoding.base64().encode(
					ServiceUtils.computeMD5Hash(msg)));
			PutObjectRequest creq = new PutObjectRequest(this.name,
					this.getClaimName(id), new ByteArrayInputStream(msg), md);
			s3Service.putObject(creq);
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
			md.addUserMetadata("lastaccessed", "0");
			md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
			md.addUserMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			md.addUserMetadata("compressedsize",
					Integer.toString(chunks.length));
			md.addUserMetadata("bsize",
					Integer.toString(arc.uncompressedLength.get()));
			md.addUserMetadata("bcompressedsize", Integer.toString(csz));
			md.addUserMetadata("objects", Integer.toString(arc.getSz()));

			md.setContentType("binary/octet-stream");
			md.setContentLength(hs.length);
			if (md5sum)
				md.setContentMD5(BaseEncoding.base64().encode(
						ServiceUtils.computeMD5Hash(hs)));
			req = new PutObjectRequest(this.name, "keys/" + haName,
					new ByteArrayInputStream(hs), md);
			s3Service.putObject(req);
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal(
					"unable to upload " + arc.getID() + " with id " + id, e);
			throw new IOException(e);
		} finally {

		}

	}

	private byte[] getData(long id) throws Exception {
		//SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		boolean compress = false;
		try {

			long tm = System.currentTimeMillis();
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name,
					"blocks/" + haName);
			S3Object sobj = null;
			try {
				sobj = s3Service.getObject(this.name, "blocks/" + haName);
			} catch (Exception e) {
				throw new IOException(e);
			}
			int cl = (int) omd.getContentLength();
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
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			if (md5sum) {
				byte[] shash = BaseEncoding.base16().decode(
						omd.getETag().toUpperCase());
				byte[] chash = ServiceUtils.computeMD5Hash(data);
				if (!Arrays.equals(shash, chash))
					throw new IOException("download corrupt at " + id);
			}
			Map<String, String> mp = this.getUserMetaData(omd);

			if (mp.containsKey("compress")) {
				compress = Boolean.parseBoolean((String) mp.get("compress"));
			}

			tm = System.currentTimeMillis();
			if (compress)
				data = CompressionUtils.decompressZLIB(data);
			try {
				mp.put("lastaccessed",
						Long.toString(System.currentTimeMillis()));
				omd.setUserMetadata(mp);
				CopyObjectRequest req = new CopyObjectRequest(this.name,
						"blocks/" + haName, this.name, "blocks/" + haName)
						.withNewObjectMetadata(omd);
				s3Service.copyObject(req);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error setting last accessed", e);
			}
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
					HashBlobArchive.currentLength.addAndGet(_size);
					HashBlobArchive.compressedLength.addAndGet(_compressedSize);
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

	private Map<String, String> getUserMetaData(ObjectMetadata obj) {
		if (!md5sum) {
			HashMap<String, String> omd = new HashMap<String, String>();
			Set<String> mdk = obj.getRawMetadata().keySet();
			for (String k : mdk) {
				if (k.toLowerCase().startsWith(Headers.S3_USER_METADATA_PREFIX)) {
					String key = k.substring(
							Headers.S3_USER_METADATA_PREFIX.length())
							.toLowerCase();
					omd.put(key, (String) obj.getRawMetadataValue(k));
				}
			}

			return omd;
		} else {
			return obj.getUserMetadata();
		}

	}

	private int verifyDelete(long id) throws IOException, Exception {
		String haName = EncyptUtils.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		ObjectMetadata om = null;
		S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);
		int claims = this.getClaimedObjects(kobj, id);
		boolean hcm = false;
		try {

			if (claims > 0) {
				om = this.getClaimMetaData(id);
				if (om == null) {
					om = s3Service.getObjectMetadata(this.name, "keys/"
							+ haName);
				} else {
					hcm = true;
					om = s3Service.getObjectMetadata(this.name,
							this.getClaimName(id));
				}
				Map<String, String> mp = this.getUserMetaData(om);
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
				om.setUserMetadata(mp);
				String kn = this.getClaimName(id);
				if (!hcm)
					kn = "keys/" + haName;
				CopyObjectRequest req = new CopyObjectRequest(this.name, kn,
						this.name, kn).withNewObjectMetadata(om);
				s3Service.copyObject(req);
				int _size = Integer.parseInt((String) mp.get("size"));
				int _compressedSize = Integer.parseInt((String) mp
						.get("compressedsize"));
				HashBlobArchive.currentLength.addAndGet(_size);
				HashBlobArchive.compressedLength.addAndGet(_compressedSize);
				SDFSLogger.getLog()
						.warn("Reclaimed [" + claims
								+ "] blocks marked for deletion");

			}

			if (claims == 0) {
				if (!hcm) {
					s3Service.deleteObject(this.name, "blocks/" + haName);
					s3Service.deleteObject(this.name, "keys/" + haName);
					SDFSLogger.getLog()
							.debug("deleted block " + "blocks/" + haName
									+ " id " + id);
				} else {
					s3Service.deleteObject(this.name, this.getClaimName(id));
					ObjectListing ol = s3Service.listObjects(this.getName(),
							"claims/" + haName);
					if (ol.getObjectSummaries().size() == 0) {
						s3Service.deleteObject(this.name, "blocks/" + haName);
						s3Service.deleteObject(this.name, "keys/" + haName);
						SDFSLogger.getLog().debug(
								"deleted block " + "blocks/" + haName + " id "
										+ id);
					}
				}
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
							Long.toString(HashBlobArchive.currentLength.get()));
					md.put("currentcompressedsize", Long
							.toString(HashBlobArchive.compressedLength.get()));
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
						boolean cmp = false;
						try {
							ObjectMetadata om = this.getClaimMetaData(k
									.longValue());
							if (om == null) {
								om = s3Service.getObjectMetadata(this.name,
										"keys/" + hashString);

							} else {
								om = s3Service.getObjectMetadata(this.name,
										this.getClaimName(k.longValue()));
								cmp = true;
							}
							Map<String, String> mp = this.getUserMetaData(om);
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
								HashBlobArchive.currentLength.addAndGet(-1
										* size);
								HashBlobArchive.compressedLength.addAndGet(-1
										* compressedSize);
								if (this.deleteUnclaimed) {
									this.verifyDelete(k.longValue());
								} else {
									mp.put("deleted", "true");
									mp.put("deleted-objects",
											Integer.toString(delobj));
									om.setUserMetadata(mp);
									String km = this
											.getClaimName(k.longValue());
									if (!cmp)
										km = "keys/" + hashString;
									CopyObjectRequest req = new CopyObjectRequest(
											this.name, km, this.name, km)
											.withNewObjectMetadata(om);
									s3Service.copyObject(req);
								}
								HashBlobArchive.removeCache(k.longValue());
							} else {
								mp.put("deleted-objects",
										Integer.toString(delobj));
								om.setUserMetadata(mp);
								String km = this.getClaimName(k.longValue());
								if (!cmp)
									km = "keys/" + hashString;
								CopyObjectRequest req = new CopyObjectRequest(
										this.name, km, this.name, km)
										.withNewObjectMetadata(om);
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
		if (ck == null)
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
			md = s3Service.getObjectMetadata(this.name, key);
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
		Map<String, String> mp = this.getUserMetaData(md);
		int size = Integer.parseInt(mp.get("size"));
		encrypt = Boolean.parseBoolean(mp.get("encrypt"));

		lz4compress = Boolean.parseBoolean(mp.get("lz4compress"));
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

		Long hid = EncyptUtils.decHashArchiveName(sobj.getKey().substring(5),
				encrypt);

		String hast = new String(data);
		SDFSLogger.getLog().debug(
				"reading hashes " + (String) mp.get("objects") + " from " + hid
						+ " encn " + sobj.getKey().substring(5));
		StringTokenizer ht = new StringTokenizer(hast, ",");
		StringResult st = new StringResult();
		st.id = hid;
		st.st = ht;
		if (mp.containsKey("bsize")) {
			HashBlobArchive.currentLength.addAndGet(Integer.parseInt(mp
					.get("bsize")));
		}
		if (mp.containsKey("bcompressedsize")) {
			HashBlobArchive.compressedLength.addAndGet(Integer.parseInt(mp
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
		HashBlobArchive.sync();
	}

	@Override
	public void uploadFile(File f, String to, String pp) throws IOException {

		BufferedInputStream in = null;
		while (to.startsWith(File.separator))
			to = to.substring(1);

		String pth = pp + "/"
				+ EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		SDFSLogger.getLog().info(
				"uploading " + f.getPath() + " to " + to + " pth " + pth);
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
				String objName = pth;
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
				if (!md5sum || genericS3) {
					md.setContentType("binary/octet-stream");
					in = new BufferedInputStream(new FileInputStream(p), 32768);
					try {
						if (md5sum) {
							byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
							in.close();
							md.setContentMD5(BaseEncoding.base64().encode(
									md5Hash));
						}

					} catch (NoSuchAlgorithmException e2) {
						SDFSLogger.getLog().error("while hashing", e2);
						throw new IOException(e2);
					}

					in = new BufferedInputStream(new FileInputStream(p), 32768);
					md.setContentLength(p.length());
					try {
						PutObjectRequest req = new PutObjectRequest(this.name,
								objName, in, md);
						s3Service.putObject(req);
						SDFSLogger.getLog().debug(
								"uploaded="
										+ f.getPath()
										+ " lm="
										+ md.getUserMetadata().get(
												"lastmodified"));
					} catch (Exception e1) {
						// SDFSLogger.getLog().error("error uploading", e1);
						throw new IOException(e1);
					}
				} else {
					try {
							md.setContentType("binary/octet-stream");
							in = new BufferedInputStream(new FileInputStream(p), 32768);
							byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
							in.close();
							md.setContentMD5(BaseEncoding.base64().encode(md5Hash));
							multiPartUpload(p, objName, md);
					} catch (Exception e1) {
						SDFSLogger.getLog().error("error uploading " + objName,
								e1);
					}
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

	private void multiPartUpload(File file, String keyName, ObjectMetadata md)
			throws AmazonServiceException, AmazonClientException,
			InterruptedException {
		TransferManager tx = null;
		try {
			if (awsCredentials != null)
				tx = new TransferManager(awsCredentials);
			else
				tx = new TransferManager(
						new InstanceProfileCredentialsProvider());
			Upload myUpload = tx.upload(this.name, keyName, file);
			myUpload.waitForCompletion();
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(name,
					keyName, name, keyName).withNewObjectMetadata(md);
			s3Service.copyObject(copyObjectRequest);
		} finally {
			if (tx != null)
				tx.shutdownNow();
		}

	}

	private void multiPartDownload(String keyName, File f)
			throws AmazonServiceException, AmazonClientException,
			InterruptedException {
		TransferManager tx = null;
		try {
			if (awsCredentials != null)
				tx = new TransferManager(awsCredentials);
			else
				tx = new TransferManager(
						new InstanceProfileCredentialsProvider());
			Download myDownload = tx.download(this.name, keyName, f);
			myDownload.waitForCompletion();
		} finally {
			if (tx != null)
				tx.shutdownNow();
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
		Map<String, String> mp = null;
		byte[] shash = null;
		try {
			if (!md5sum || genericS3) {
				S3Object obj = null;

				obj = s3Service.getObject(this.name, pp + "/" + haName);
				BufferedInputStream in = new BufferedInputStream(
						obj.getObjectContent());
				BufferedOutputStream out = new BufferedOutputStream(
						new FileOutputStream(p));
				IOUtils.copy(in, out);
				out.flush();
				out.close();
				in.close();
				shash = BaseEncoding.base16().decode(
						obj.getObjectMetadata().getETag().toUpperCase());
				mp = this.getUserMetaData(obj.getObjectMetadata());
				try {
					if (obj != null)
						obj.close();
				} catch (Exception e1) {
				}
			} else {
				SDFSLogger.getLog().info("downloading " + pp + "/" + haName);
				this.multiPartDownload(pp + "/" + haName, p);
				ObjectMetadata omd = s3Service.getObjectMetadata(name, pp + "/"
						+ haName);
				shash = BaseEncoding.base16().decode(
						omd.getETag().toUpperCase());
				mp = this.getUserMetaData(s3Service.getObjectMetadata(name, pp
						+ "/" + haName));
			}
			if (!FileUtils.fileValid(p, shash))
				throw new IOException("file " + p.getPath() + " is corrupt");
			boolean encrypt = false;
			boolean lz4compress = false;
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
			Map<String, String> mp = this.getUserMetaData(sobj);
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
		//SDFSLogger.getLog().info("downloading map for " + id);
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
				Map<String, String> obj = this.getUserMetaData(omd);
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
	public synchronized String restoreBlock(long id, byte[] hash)
			throws IOException {
		if (id == -1) {
			SDFSLogger.getLog().warn(
					"Hash not found for " + StringUtils.getHexString(hash));
			return null;
		}
		String haName = this.restoreRequests.get(new Long(id));
		if (haName == null)
			haName = EncyptUtils.encHashArchiveName(id,
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
				restoreRequests.put(new Long(id), "InvalidObjectState");
				return null;
			}
			restoreRequests.put(new Long(id), haName);
			return haName;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {

				restoreRequests.put(new Long(id), "InvalidObjectState");
				return null;
			}
			if (e.getErrorCode().equalsIgnoreCase("RestoreAlreadyInProgress")) {

				restoreRequests.put(new Long(id), haName);
				return haName;
			} else {
				SDFSLogger.getLog().error(
						"Error while restoring block " + e.getErrorCode()
								+ " id=" + id + " name=blocks/" + haName);
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
	public void checkoutObject(long id, int claims) throws IOException {
		ObjectMetadata om = this.getClaimMetaData(id);
		if (om != null)
			return;
		else {
			String haName = EncyptUtils.encHashArchiveName(id,
					Main.chunkStoreEncryptionEnabled);
			om = s3Service.getObjectMetadata(this.name, "keys/" + haName);
			Map<String, String> md = om.getUserMetadata();
			md.put("objects", Integer.toString(claims));
			if (md.containsKey("deleted")) {
				md.remove("deleted");
			}
			if (md.containsKey("deleted-objects")) {
				md.remove("deleted-objects");
			}
			if (md.containsKey("bsize")) {
				HashBlobArchive.currentLength.addAndGet(Integer.parseInt(md
						.get("bsize")));
			}
			if (md.containsKey("bcompressedsize")) {
				HashBlobArchive.compressedLength.addAndGet(Integer.parseInt(md
						.get("bcompressedsize")));
			}
			byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();

			om.setContentLength(msg.length);
			try {
				om.setContentMD5(BaseEncoding.base64().encode(
						ServiceUtils.computeMD5Hash(msg)));
			} catch (Exception e) {
				throw new IOException(e);
			}
			PutObjectRequest creq = new PutObjectRequest(this.name,
					this.getClaimName(id), new ByteArrayInputStream(msg), om);
			s3Service.putObject(creq);
		}
	}

}