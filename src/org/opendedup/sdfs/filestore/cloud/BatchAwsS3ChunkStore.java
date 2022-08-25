/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.filestore.cloud;

import static java.lang.Math.toIntExact;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.spec.IvParameterSpec;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GlacierJobParameters;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tier;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashExistsException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.fsync.SyncFSScheduler;
import org.opendedup.grpc.CloudFileInfo.CloudMetaData;
import org.opendedup.grpc.FileInfo.FileInfoResponse;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.StringResult;
import org.opendedup.sdfs.filestore.cloud.gcp.GCPSessionCredentials;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.filestore.cloud.utils.FileUtils;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.FileHashing;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

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
public class BatchAwsS3ChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private BasicAWSCredentials awsCredentials = null;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	private HashSet<Long> refresh = new HashSet<Long>();
	private String name;
	private Region bucketLocation = null;
	AmazonS3 s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean md5sum = true;
	boolean simpleS3 = false;
	boolean encryptBucket = false;
	private int glacierDays = 0;
	private boolean useGlacier = false;
	private int infrequentAccess = 0;
	// private ReentrantReadWriteLock s3clientLock = new
	// ReentrantReadWriteLock();
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	private WeakHashMap<Long, String> restoreRequests = new WeakHashMap<Long, String>();
	private int checkInterval = 15000;
	private String binm = "bucketinfo";
	private int mdVersion = 0;
	private final static String mdExt = ".6442";
	private String dExt = "";
	private boolean tcpKeepAlive = true;
	private String accessKey = Main.cloudAccessKey;
	private String secretKey = Main.cloudSecretKey;
	private boolean standAlone = true;
	private int transferSize = 10 * 1024 * 1024;
	private com.amazonaws.services.s3.model.Tier glacierTier = Tier.Bulk;
	boolean gcsSigner = false;
	TransferManager tx = null;
	static {
		try {

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

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
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

	public void addRefresh(long id) {
		this.delLock.lock();
		try {
			if (Main.REFRESH_BLOBS)
				this.refresh.add(id);
		} finally {
			this.delLock.unlock();
		}
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
		
		try {
			SDFSLogger.getLog().info("############ Closing Bucket##################");
			if (this.standAlone) {
				HashBlobArchive.close();

				Map<String, String> md = null;
				md = this.getUserMetaData(binm);

				ObjectMetadata nmd = new ObjectMetadata();
				nmd.setUserMetadata(md);
				md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
				md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
				md.put("lastupdate", Long.toString(System.currentTimeMillis()));
				md.put("hostname", InetAddress.getLocalHost().getHostName());
				md.put("port", Integer.toString(Main.sdfsCliPort));
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
				md.put("md5sum", st);
				nmd.setContentMD5(st);
				nmd.setContentLength(sz.length);
				nmd.setUserMetadata(md);
				try {

					this.updateObject(binm, nmd);
				} catch (AmazonS3Exception e1) {
					if (e1.getStatusCode() == 409) {
						try {
							this.updateObject(binm, nmd);

						} catch (Exception e2) {
							throw new IOException(e2);
						}
					} else {
						throw new IOException(e1);
					}
				} catch (Exception e1) {
					// SDFSLogger.getLog().error("error uploading", e1);
					throw new IOException(e1);
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while closing bucket " + this.name, e);
		} finally {
			this.closed = true;
			try {
				tx.shutdownNow(false);
				s3Service.shutdown();
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while closing bucket " + this.name, e);
			}
		}

	}

	public void expandFile(long length) throws IOException {

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException, DataArchivedException {
		return HashBlobArchive.getBlock(hash, start);

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
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {

	}

	@Override
	public long size() {

		return HashBlobArchive.getLength();
	}

	@Override
	public InsertRecord writeChunk(byte[] hash, byte[] chunk, int len, String uuid) throws IOException {
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
			if (this.deletes.containsKey(start)) {
				int sz = this.deletes.get(start) + 1;
				this.deletes.put(start, sz);
			} else
				this.deletes.put(start, 1);

		} finally {
			delLock.unlock();
		}
	}

	public static void deleteBucket(String bucketName, String awsAccessKey, String awsSecretKey) {
		try {
			System.out.println("");
			System.out.print("Deleting Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
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
			SDFSLogger.getLog().warn("Unable to delete bucket " + bucketName, e);
		}
	}

	@Override
	public void init(Element config) throws IOException {

		if (config.hasAttribute("bucket-name")) {
			this.name = config.getAttribute("bucket-name").toLowerCase();
		} else {
			this.name = Main.cloudBucket.toLowerCase();
		}
		if (config.hasAttribute("access-key")) {
			this.accessKey = config.getAttribute("access-key");
		}
		if (config.hasAttribute("secret-key")) {
			this.secretKey = config.getAttribute("secret-key");
		}

		if (config.hasAttribute("service-type")
				&& config.getAttribute("service-type").equalsIgnoreCase("google-cloud-storage")) {

			gcsSigner = true;
		}
		this.staged_sync_location.mkdirs();
		try {
			if (!Main.useAim && !config.hasAttribute("auth-file"))
				awsCredentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
			if (config.hasAttribute("default-bucket-location")) {
				bucketLocation = RegionUtils.getRegion(config.getAttribute("default-bucket-location"));
			} else {
				bucketLocation = RegionUtils.getRegion("us-west-2");
			}
			if (config.hasAttribute("connection-check-interval")) {
				this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
			}
			if (this.standAlone) {
				if (config.hasAttribute("block-size")) {
					int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
					this.transferSize = (int) Math.round(((double) sz * 1.5));
					HashBlobArchive.MAX_LEN = sz;
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
				} else {
					HashBlobArchive.maxQueueSize = 0;
				}
				if (config.hasAttribute("allow-sync")) {
					HashBlobArchive.allowSync = Boolean.parseBoolean(config.getAttribute("allow-sync"));
					if (config.hasAttribute("sync-check-schedule")
							&& !config.getAttribute("sync-check-schedule").equalsIgnoreCase("none")) {
						try {
							new SyncFSScheduler(config.getAttribute("sync-check-schedule"));
						} catch (Exception e) {
							SDFSLogger.getLog().error("unable to start sync scheduler", e);
						}
					}

				}
				if (config.hasAttribute("upload-thread-sleep-time")) {
					int tm = Integer.parseInt(config.getAttribute("upload-thread-sleep-time"));
					HashBlobArchive.THREAD_SLEEP_TIME = tm;
				} else {
					HashBlobArchive.THREAD_SLEEP_TIME = 1000 * 60 * 10;
				}
				if (config.hasAttribute("single-writer")) {
					HashBlobArchive.SINGLE_WRITER = Boolean.parseBoolean("singel-writer");
				}
				if (config.hasAttribute("cache-writes")) {
					HashBlobArchive.cacheWrites = Boolean.parseBoolean(config.getAttribute("cache-writes"));
				}
				if (config.hasAttribute("cache-reads")) {
					HashBlobArchive.cacheReads = Boolean.parseBoolean(config.getAttribute("cache-reads"));
				}

				if (config.hasAttribute("sync-files")) {
					boolean syncf = Boolean.parseBoolean(config.getAttribute("sync-files"));
					if (syncf) {
						new FileReplicationService(this);
					}
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
			if (this.standAlone && config.hasAttribute("local-cache-size")) {
				long sz = StringUtils.parseSize(config.getAttribute("local-cache-size"));
				HashBlobArchive.setLocalCacheSize(sz);
			}
			if (config.hasAttribute("transfer-size")) {
				this.transferSize = (int) StringUtils.parseSize(config.getAttribute("transfer-size"));
			}

			if (config.hasAttribute("metadata-version")) {
				this.mdVersion = Integer.parseInt(config.getAttribute("metadata-version"));

			}
			SDFSLogger.getLog().info("###### Set Metadata Version to " + this.mdVersion);
			if (config.hasAttribute("data-appendix")) {
				this.dExt = config.getAttribute("data-appendix");
			}
			if (this.standAlone && config.hasAttribute("map-cache-size")) {
				int sz = Integer.parseInt(config.getAttribute("map-cache-size"));
				HashBlobArchive.MAP_CACHE_SIZE = sz;
			}
			if (config.hasAttribute("io-threads")) {
				int sz = Integer.parseInt(config.getAttribute("io-threads"));
				Main.dseIOThreads = sz;
			}
			if (config.hasAttribute("smart-cache")) {
				boolean sm = Boolean.parseBoolean(config.getAttribute("smart-cache"));
				HashBlobArchive.SMART_CACHE = sm;
			}

			if (config.hasAttribute("delete-unclaimed")) {
				this.deleteUnclaimed = Boolean.parseBoolean(config.getAttribute("delete-unclaimed"));
			}
			if (config.hasAttribute("glacier-archive-days")) {
				this.glacierDays = Integer.parseInt(config.getAttribute("glacier-archive-days"));
				if (this.glacierDays > 0) {
					Main.checkArchiveOnRead = true;
					Main.REFRESH_BLOBS = true;
					this.useGlacier = true;
				} else if (config.hasAttribute("glacier-zero-day")
						&& config.getAttribute("glacier-zero-day").equalsIgnoreCase("true")) {
					this.glacierDays = 0;
					Main.checkArchiveOnRead = true;
					Main.REFRESH_BLOBS = true;
					this.useGlacier = true;
				}
				if (this.useGlacier) {
					if (config.hasAttribute("glacier-tier")) {
						String ts = config.getAttribute("glacier-tier");
						if (ts.equalsIgnoreCase("standard"))
							this.glacierTier = Tier.Standard;
						if (ts.equalsIgnoreCase("expedited"))
							this.glacierTier = Tier.Expedited;
						if (ts.equalsIgnoreCase("bulk"))
							this.glacierTier = Tier.Bulk;

					}
					SDFSLogger.getLog().info("Glacier Will be initialized after " + this.glacierDays
							+ " and restored with tier " + this.glacierTier);
				}
			}

			if (config.hasAttribute("infrequent-access-days")) {
				this.infrequentAccess = Integer.parseInt(config.getAttribute("infrequent-access-days"));
			}
			if (config.hasAttribute("simple-s3")) {
				EncyptUtils.baseEncode = Boolean.parseBoolean(config.getAttribute("simple-s3"));
				this.simpleS3 = true;
			}
			if (config.hasAttribute("md5-sum")) {
				this.md5sum = Boolean.parseBoolean(config.getAttribute("md5-sum"));
				if (!this.md5sum) {
					System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
					System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
				}

			}
			ClientConfiguration clientConfig = new ClientConfiguration();
			AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withClientConfiguration(clientConfig);
			if (config.hasAttribute("use-v4-signer")) {
				boolean v4s = Boolean.parseBoolean(config.getAttribute("use-v4-signer"));

				if (v4s) {
					clientConfig.setSignerOverride("AWSS3V4SignerType");
				}
			} else if (config.hasAttribute("use-basic-signer")) {
				boolean v4s = Boolean.parseBoolean(config.getAttribute("use-basic-signer"));
				// if (v4s) {
				// clientConfig.setSignerOverride("S3SignerType");
				// }
			} else if (gcsSigner) {
				System.out.println("Target is GCS Storage");
				Map<String, String> env = System.getenv();
				if (config.hasAttribute("auth-file") || env.containsKey("GOOGLE_APPLICATION_CREDENTIALS")) {
					String credPath = null;
					if (config.hasAttribute("auth-file")) {
						credPath = config.getAttribute("auth-file");
					} else {
						credPath = env.get("GOOGLE_APPLICATION_CREDENTIALS");
					}
					GoogleCredentials sourceCredentials = null;
					if (credPath.equalsIgnoreCase("default")) {
						sourceCredentials = ServiceAccountCredentials.getApplicationDefault();
					} else {

						sourceCredentials = (GoogleCredentials) ServiceAccountCredentials
								.fromStream(new FileInputStream(credPath));
					}
					sourceCredentials = (GoogleCredentials) sourceCredentials
							.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));

					String _projectId = null;
					if (env.containsKey("GOOGLE_PROJECT_ID")) {
						_projectId = env.get("GOOGLE_PROJECT_ID");
					} else if (sourceCredentials instanceof ServiceAccountCredentials) {
						_projectId = ((ServiceAccountCredentials) sourceCredentials).getProjectId();
					}

					final String projectId = _projectId;

					GCPSessionCredentials creds = new GCPSessionCredentials((GoogleCredentials) sourceCredentials);

					SignerFactory.registerSigner("org.opendedup.sdfs.filestore.cloud.gcp.CustomGCPSigner",
							org.opendedup.sdfs.filestore.cloud.gcp.CustomGCPSigner.class);
					clientConfig.setSignerOverride("org.opendedup.sdfs.filestore.cloud.gcp.CustomGCPSigner");
					builder = builder
							.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
									"https://storage.googleapis.com", "auto"))
							.withRequestHandlers(new RequestHandler2() {
								@Override
								public void beforeRequest(Request<?> request) {
									if (projectId != null) {
										request.addHeader("x-goog-project-id", projectId);
									}
								}
							}).withCredentials(new AWSStaticCredentialsProvider(creds));

				} else {
					builder = builder
							.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
									"https://storage.googleapis.com", "auto"))
							.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
				}

			}

			clientConfig.setMaxConnections(Main.dseIOThreads * 2);
			clientConfig.setConnectionTimeout(120000);
			clientConfig.setSocketTimeout(120000);
			if (config.hasAttribute("tcp-keepalive")) {
				this.tcpKeepAlive = Boolean.parseBoolean(config.getAttribute("tcp-keepalive"));
			}
			if (!this.tcpKeepAlive) {
				clientConfig.setUseTcpKeepAlive(false);
			}
			String s3Target = null;
			if (config.hasAttribute("user-agent-prefix")) {
				clientConfig.setUserAgentPrefix(config.getAttribute("user-agent-prefix"));
			}
			if (config.getElementsByTagName("connection-props").getLength() > 0) {
				Element el = (Element) config.getElementsByTagName("connection-props").item(0);
				if (el.hasAttribute("connection-timeout"))
					clientConfig.setConnectionTimeout(Integer.parseInt(el.getAttribute("connection-timeout")));
				if (el.hasAttribute("socket-timeout"))
					clientConfig.setSocketTimeout(Integer.parseInt(el.getAttribute("socket-timeout")));
				if (el.hasAttribute("local-address"))
					clientConfig.setLocalAddress(InetAddress.getByName(el.getAttribute("local-address")));
				if (el.hasAttribute("max-retry"))
					clientConfig.setMaxErrorRetry(Integer.parseInt(el.getAttribute("max-retry")));
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
					clientConfig.setProxyDomain(el.getAttribute("proxy-domain"));
				}
				if (el.hasAttribute("proxy-password")) {
					clientConfig.setProxyPassword(el.getAttribute("proxy-password"));
				}
				if (el.hasAttribute("proxy-port")) {
					clientConfig.setProxyPort(Integer.parseInt(el.getAttribute("proxy-port")));
				}
				if (el.hasAttribute("proxy-username")) {
					clientConfig.setProxyUsername(el.getAttribute("proxy-username"));
				}
			}

			if (s3Target != null && !gcsSigner && s3Target.toLowerCase().startsWith("https")) {
				TrustStrategy acceptingTrustStrategy = new TrustStrategy() {

					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType) {
						return true;
					}
				};

				SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy,
						SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
				clientConfig.getApacheHttpClientConfig().withSslSocketFactory(sf);
			}
			if (!gcsSigner) {
				if (config.hasAttribute("default-bucket-location")) {
					String bl = config.getAttribute("default-bucket-location");
					System.out.println("bucketLocation=" + bucketLocation.toString());
					builder = builder.withRegion(bl);
				} else if (s3Target != null) {
					String[] tokens = s3Target.split("\\.");
					String region = "us-west-2";
					if (tokens.length > 1) {
						region = tokens[1];
					}
					SDFSLogger.getLog().info("region=" + region);
					EndpointConfiguration ep = new EndpointConfiguration(s3Target, region);
					builder = builder.withEndpointConfiguration(ep);
					SDFSLogger.getLog().info("target=" + s3Target);
				} else {
					String bl = "us-west-2";
					System.out.println("bucketLocation=" + bl);
					builder = builder.withRegion(bl);
				}
				if (awsCredentials != null)
					builder = builder.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
				else
					builder = builder.withCredentials(new InstanceProfileCredentialsProvider(false));

				if (config.hasAttribute("disableDNSBucket")) {
					builder.withPathStyleAccessEnabled(Boolean.parseBoolean(config.getAttribute("disableDNSBucket")));
					System.out.println(
							"disableDNSBucket=" + Boolean.parseBoolean(config.getAttribute("disableDNSBucket")));
				}
			}
			s3Service = builder.build();

			this.binm = "bucketinfo/" + EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
			boolean connected = false;

			for (int i = 0; i < 4; i++) {
				try {
					s3Service.doesBucketExist(this.name);
					connected = true;

				} catch (Exception e) {
					try {
						SDFSLogger.getLog().warn("unable to connect to server", e);
						Thread.sleep(20 * 1000);
					} catch (Exception e1) {

					}
					i++;
				}
			}
			if (!connected) {
				throw new IOException("Failed to connect to storage bucket. Exiting");
			}
			if (!s3Service.doesBucketExist(this.name)) {
				this.encryptBucket = Main.encryptBucket;
				s3Service.createBucket(this.name);
				SDFSLogger.getLog().info("created new store " + name);
				if (Main.encryptBucket) {
					SDFSLogger.getLog().info("Setting server-side encryption on the store " + name);
					s3Service.setBucketEncryption(new SetBucketEncryptionRequest()
							.withBucketName(this.name)
							.withServerSideEncryptionConfiguration(new ServerSideEncryptionConfiguration()
									.withRules(new ServerSideEncryptionRule()
											.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
													.withSSEAlgorithm(SSEAlgorithm.AES256)))));
				}
				if (this.standAlone) {
					ObjectMetadata md = new ObjectMetadata();
					md.addUserMetadata("currentsize", "0");
					md.addUserMetadata("currentcompressedsize", "0");
					md.addUserMetadata("clustered", "true");
					md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
					md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
					md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));

					byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
					if (md5sum) {
						String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
						md.setContentMD5(mds);
					}
					md.setContentLength(sz.length);
					this.binm = "bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);
					this.updateObject(binm, md);
				}
			} else if (this.standAlone) {

				Map<String, String> obj = null;
				try {
					obj = this.getUserMetaData(binm);
				} catch (Exception e) {
					SDFSLogger.getLog().debug("unable to find bucketinfo object", e);
				}
				if (obj == null || !obj.containsKey("currentsize")) {
					try {
						this.binm = "bucketinfo/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);

						obj = this.getUserMetaData(binm);
						obj.get("currentsize");
					} catch (Exception e) {
						if (obj != null) {
							obj.get("currentsize");
						} else
							obj = new HashMap<String, String>();
						SDFSLogger.getLog().debug("unable to find bucketinfo object", e);
					}
				}
				if (!obj.containsKey("currentsize")) {
					ObjectMetadata md = new ObjectMetadata();
					md.addUserMetadata("currentsize", "0");
					md.addUserMetadata("currentcompressedsize", "0");
					md.addUserMetadata("clustered", "true");
					md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
					md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
					md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));
					this.binm = "bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
					if (md5sum) {
						String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
						md.setContentMD5(mds);
					}
					md.setContentLength(sz.length);
					s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);
					this.updateObject(binm, md);
				} else {
					if (this.standAlone && obj.containsKey("currentsize")) {
						long cl = Long.parseLong((String) obj.get("currentsize"));
						if (cl >= 0) {
							HashBlobArchive.setLength(cl);

						} else
							SDFSLogger.getLog().warn("The S3 objectstore DSE did not close correctly len=" + cl);
					} else {
						SDFSLogger.getLog().warn(
								"The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}

					if (this.standAlone && obj.containsKey("currentcompressedsize")) {
						long cl = Long.parseLong((String) obj.get("currentcompressedsize"));
						if (cl >= 0) {
							HashBlobArchive.setCompressedLength(cl);

						} else
							SDFSLogger.getLog().warn("The S3 objectstore DSE did not close correctly clen=" + cl);
					} else {
						SDFSLogger.getLog().warn(
								"The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}

					obj.put("clustered", Boolean.toString(true));

					try {
						ObjectMetadata omd = new ObjectMetadata();
						omd.setUserMetadata(obj);

						updateObject(binm, omd);
					} catch (

					Exception e) {
						SDFSLogger.getLog().warn("unable to update bucket info in init", e);
						SDFSLogger.getLog().info("created new store " + name);
						ObjectMetadata md = new ObjectMetadata();
						md.addUserMetadata("currentsize", "0");
						md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
						md.addUserMetadata("currentcompressedsize", "0");
						md.addUserMetadata("clustered", Boolean.toString(true));
						md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
						md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));
						byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
						if (md5sum) {
							String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
							md.setContentMD5(mds);
						}
						md.setContentLength(sz.length);
						s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);
						this.updateObject(binm, md);

					}
				}
			}
			ArrayList<Transition> trs = new ArrayList<Transition>();
			if (this.useGlacier && s3Target != null) {
				Transition transToArchive = new Transition().withDays(this.glacierDays)
						.withStorageClass(StorageClass.Glacier);
				trs.add(transToArchive);
			}

			if (this.infrequentAccess > 0 && s3Target == null) {
				Transition transToArchive = new Transition().withDays(this.infrequentAccess)
						.withStorageClass(StorageClass.StandardInfrequentAccess);
				trs.add(transToArchive);

			}
			if (trs.size() > 0) {
				SDFSLogger.getLog().info("Setting S3 bucket lifecycle configuration");
				BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
						.withId("SDFS Automated Archive Rule for Block Data")
						.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("blocks/"))).withTransitions(trs)
						.withStatus(BucketLifecycleConfiguration.ENABLED.toString());
				List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
				rules.add(ruleArchiveAndExpire);

				BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration().withRules(rules);
				// Save configuration.
				s3Service.setBucketLifecycleConfiguration(this.name, configuration);
			} else if (s3Target == null && !gcsSigner) {
				s3Service.deleteBucketLifecycleConfiguration(this.name);
			}
			if (this.standAlone) {
				HashBlobArchive.init(this);
				HashBlobArchive.setReadSpeed(rsp, false);
				HashBlobArchive.setWriteSpeed(wsp, false);
			}
			tx = TransferManagerBuilder.standard().withS3Client(builder.build()).build();
			Thread th = new Thread(this);
			th.start();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to start service", e);
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
					"hid=" + hid + " val=" + StringUtils.getHexString(BaseEncoding.base64().decode(tk.split(":")[0])));
			ChunkData chk = new ChunkData(BaseEncoding.base64().decode(tk.split(":")[0]), hid);
			return chk;
		}

	}

	private String[] getStrings(S3Object sobj) throws IOException {

		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;

		int cl = (int) sobj.getObjectMetadata().getContentLength();

		byte[] data = new byte[cl];
		S3ObjectInputStream in = null;
		try {

			in = sobj.getObjectContent();
			int ncl = IOUtils.readFully(in, data);
			if (ncl != cl) {
				SDFSLogger.getLog().warn("Read less bytes that expected. Expected " + cl + " read " + ncl);
			}

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				in.abort();
				in.close();
				sobj.close();
			} catch (Exception e) {
			}
		}
		Map<String, String> mp = this.getUserMetaData(sobj.getKey());
		if (mp.containsKey("md5sum")) {
			byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));
			byte[] chash;
			chash = Hashing.md5().hashBytes(data).asBytes();
			if (!Arrays.equals(shash, chash))
				throw new IOException("download corrupt at " + sobj.getKey());
		}

		int size = Integer.parseInt((String) mp.get("size"));
		if (mp.containsKey("encrypt")) {
			encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
		}
		if (mp.containsKey("compress")) {
			compress = Boolean.parseBoolean((String) mp.get("compress"));
		} else if (mp.containsKey("lz4compress")) {

			lz4compress = Boolean.parseBoolean((String) mp.get("lz4compress"));
		}
		byte[] ivb = null;
		if (mp.containsKey("ivspec"))
			ivb = BaseEncoding.base64().decode(mp.get("ivspec"));
		if (encrypt) {
			if (ivb != null)
				data = EncryptUtils.decryptCBC(data, new IvParameterSpec(ivb));
			else
				data = EncryptUtils.decryptCBC(data);
		}
		if (compress)
			data = CompressionUtils.decompressZLIB(data);
		else if (lz4compress) {
			data = CompressionUtils.decompressLz4(data, size);
		}
		String hast = new String(data);
		SDFSLogger.getLog().debug("reading hashes " + (String) mp.get("hashes") + " from " + sobj.getKey());
		String[] st = hast.split(",");
		return st;
	}

	private int getClaimedObjects(S3Object sobj, long id) throws Exception, IOException {

		String[] st = this.getStrings(sobj);
		int claims = 0;
		for (String ha : st) {
			byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
			if (HCServiceProxy.getHashesMap().mightContainKey(b, id))
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
			dl = new MultiDownload(this, "keys/");
			dl.iterationInit(false, "keys/");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}

	}

	protected void resetLength() {
		if (this.standAlone) {
			try {
				ObjectMetadata omd = null;
				Map<String, String> md = this.getUserMetaData(binm);
				md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
				md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
				md.put("lastupdate", Long.toString(System.currentTimeMillis()));
				md.put("hostname", InetAddress.getLocalHost().getHostName());
				md.put("port", Integer.toString(Main.sdfsCliPort));
				omd = new ObjectMetadata();
				omd.setUserMetadata(md);

				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
				omd.addUserMetadata("md5sum", st);
				omd.setContentMD5(st);
				omd.setContentLength(sz.length);
				s3Service.putObject(this.name, binm + "-" + System.currentTimeMillis(), new ByteArrayInputStream(sz),
						omd);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to create backup of filesystem metadata", e);
			}
			HashBlobArchive.setLength(0);
			HashBlobArchive.setCompressedLength(0);
		}
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

		return HashBlobArchive.getCompressedLength();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		try {
			s3Service.getObject(this.name, "blocks/" + haName + this.dExt);
			return true;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found") && !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return false;
		}
	}

	private String getClaimName(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		return "claims/keys/" + haName + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
	}

	private Map<String, String> getClaimMetaData(long id) throws IOException {
		// this.s3clientLock.readLock().lock();
		try {
			Map<String, String> md = this.getUserMetaData(this.getClaimName(id));
			if (md.size() == 0)
				return null;
			else
				return md;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found") && !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return null;
		}
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}

		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		IOException e = null;
		for (int i = 0; i < 9; i++) {
			try {
				int csz = toIntExact(arc.getFile().length());
				ObjectMetadata md = new ObjectMetadata();
				md.addUserMetadata("size", Integer.toString(arc.uncompressedLength.get()));
				md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
				md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				md.addUserMetadata("compressedsize", Integer.toString(csz));
				md.addUserMetadata("bsize", Integer.toString(arc.getLen()));
				md.addUserMetadata("objects", Integer.toString(arc.getSz()));
				md.addUserMetadata("uuid", arc.getUUID());
				md.addUserMetadata("bcompressedsize", Integer.toString(csz));
				md.setContentType("binary/octet-stream");
				md.setContentLength(csz);
				if (md5sum) {
					FileInputStream in = new FileInputStream(arc.getFile());
					Hasher hasher = Hashing.md5().newHasher();
					ByteStreams.copy(in, Funnels.asOutputStream(hasher));
					String mds = BaseEncoding.base64().encode(hasher.hash().asBytes());
					md.setContentMD5(mds);
					md.addUserMetadata("md5sum", mds);
					IOUtils.closeQuietly(in);
				}
				FileInputStream in = new FileInputStream(arc.getFile());
				PutObjectRequest req = new PutObjectRequest(this.name, "blocks/" + haName + this.dExt, arc.getFile())
						.withMetadata(md);
				try {
					in.close();
				} catch (Exception e1s) {

				}
				this.multiPartUpload(req, arc.getFile());
				this.updateObject("blocks/" + haName, md);
				byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();
				String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(msg).asBytes());
				md.setContentMD5(mds);
				md.addUserMetadata("md5sum", mds);

				md.setContentType("binary/octet-stream");
				md.setContentLength(msg.length);
				PutObjectRequest creq = new PutObjectRequest(this.name, this.getClaimName(id),
						new ByteArrayInputStream(msg), md);

				s3Service.putObject(creq);
				this.updateObject(this.getClaimName(id), md);
				byte[] hs = arc.getHashesString().getBytes();
				int sz = hs.length;
				if (Main.compress) {
					hs = CompressionUtils.compressLz4(hs);
				}
				byte[] ivb = PassPhrase.getByteIV();
				if (Main.chunkStoreEncryptionEnabled) {
					hs = EncryptUtils.encryptCBC(hs, new IvParameterSpec(ivb));
				}
				md = new ObjectMetadata();
				md.addUserMetadata("size", Integer.toString(sz));
				md.addUserMetadata("ivspec", BaseEncoding.base64().encode(ivb));
				md.addUserMetadata("lastaccessed", "0");
				md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
				md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				md.addUserMetadata("compressedsize", Integer.toString(csz));
				md.addUserMetadata("bsize", Integer.toString(arc.uncompressedLength.get()));
				md.addUserMetadata("bcompressedsize", Integer.toString(csz));
				md.addUserMetadata("objects", Integer.toString(arc.getSz()));

				md.setContentType("binary/octet-stream");
				md.setContentLength(hs.length);
				if (md5sum) {

					mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(hs).asBytes());
					md.setContentMD5(mds);
					md.addUserMetadata("md5sum", mds);
				}
				req = new PutObjectRequest(this.name, "keys/" + haName, new ByteArrayInputStream(hs), md);
				s3Service.putObject(req);
				this.updateObject("keys/" + haName, md);
				return;
			} catch (Throwable e1) {
				// SDFSLogger.getLog().warn("unable to upload " + arc.getID() + " with id " +
				// id, e1);
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

	public byte[] getBytes(long id, int from, int to) throws IOException, DataArchivedException {
		// SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		S3Object sobj = null;
		byte[] data = null;
		// int ol = 0;
		try {

			long tm = System.currentTimeMillis();
			// ObjectMetadata omd = s3Service.getObjectMetadata(this.name,
			// "blocks/" + haName);
			// Map<String, String> mp = this.getUserMetaData(omd);
			// ol = Integer.parseInt(mp.get("compressedsize"));
			// if (ol <= to) {
			// to = ol;
			// SDFSLogger.getLog().info("change to=" + to);
			// }
			int cl = (int) to - from;
			GetObjectRequest gr = new GetObjectRequest(this.name, "blocks/" + haName + this.dExt);
			gr.setRange(from, to - 1);
			sobj = s3Service.getObject(gr);
			S3ObjectInputStream in = sobj.getObjectContent();
			data = new byte[cl];
			int ncl = IOUtils.readFully(in, data);
			if (ncl != cl) {
				SDFSLogger.getLog().warn("Read less bytes that expected. Expected " + cl + " read " + ncl);
			}
			in.abort();
			IOUtils.closeQuietly(in);
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (ncl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			// mp = this.getUserMetaData(omd);
			/*
			 * try { mp.put("lastaccessed", Long.toString(System.currentTimeMillis()));
			 * omd.setUserMetadata(mp); CopyObjectRequest req = new
			 * CopyObjectRequest(this.name, "blocks/" + haName, this.name, "blocks/" +
			 * haName) .withNewObjectMetadata(omd); s3Service.copyObject(req); } catch
			 * (Exception e) { SDFSLogger.getLog().debug("error setting last accessed", e);
			 * }
			 */
			/*
			 * if (mp.containsKey("deleted")) { boolean del = Boolean.parseBoolean((String)
			 * mp.get("deleted")); if (del) { S3Object kobj = s3Service.getObject(this.name,
			 * "keys/" + haName);
			 *
			 * int claims = this.getClaimedObjects(kobj, id);
			 *
			 * int delobj = 0; if (mp.containsKey("deleted-objects")) { delobj =
			 * Integer.parseInt((String) mp .get("deleted-objects")) - claims; if (delobj <
			 * 0) delobj = 0; } mp.remove("deleted"); mp.put("deleted-objects",
			 * Integer.toString(delobj)); mp.put("suspect", "true");
			 * omd.setUserMetadata(mp); CopyObjectRequest req = new
			 * CopyObjectRequest(this.name, "keys/" + haName, this.name, "keys/" + haName)
			 * .withNewObjectMetadata(omd); s3Service.copyObject(req); int _size =
			 * Integer.parseInt((String) mp.get("size")); int _compressedSize =
			 * Integer.parseInt((String) mp .get("compressedsize"));
			 * HashBlobArchive.currentLength.addAndGet(_size);
			 * HashBlobArchive.compressedLength.addAndGet(_compressedSize);
			 * SDFSLogger.getLog().warn( "Reclaimed [" + claims +
			 * "] blocks marked for deletion"); kobj.close(); } }
			 */
			dtm = (System.currentTimeMillis() - tm) / 1000d;
			bps = (cl / 1024) / dtm;
		} catch (AmazonS3Exception e) {

			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
				SDFSLogger.getLog().info("block [" + id + "] at [blocks/" + haName + "] is in glacier", e);
				throw new DataArchivedException(id, null);
			} else {
				SDFSLogger.getLog().error(
						"unable to get block [" + id + "] at [blocks/" + haName + "] pos " + from + " to " + to, e);
				throw e;

			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				if (sobj != null) {
					sobj.getObjectContent().abort();
					sobj.close();
				}
			} catch (Exception e) {

			}
			// this.s3clientLock.readLock().unlock();
		}
		return data;
	}

	private void getData(long id, File f) throws DataArchivedException, IOException {
		// SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		S3Object sobj = null;
		try {
			if (f.exists() && !f.delete()) {
				SDFSLogger.getLog().warn("file already exists! " + f.getPath());
				File nf = new File(f.getPath() + " " + ".old");
				Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			long tm = System.currentTimeMillis();
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/" + haName + this.dExt);
			try {
				sobj = s3Service.getObject(this.name, "blocks/" + haName + this.dExt);
			} catch (AmazonS3Exception e) {
				if (e.getErrorCode().equalsIgnoreCase("NoSuchKey") && Main.chunkStoreEncryptionEnabled) {
					haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
					try {
						omd = s3Service.getObjectMetadata(this.name, "blocks/" + haName + this.dExt);
					} catch (AmazonS3Exception e1) {
						if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
							SDFSLogger.getLog().info("block [" + id + "] at [blocks/" + haName + "] is in glacier", e);
							throw new DataArchivedException(id, null);
						}
					}
				}
				if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
					SDFSLogger.getLog().info("block [" + id + "] at [blocks/" + haName + "] is in glacier", e);
					throw new DataArchivedException(id, null);
				} else {
					SDFSLogger.getLog().error("unable to get block [" + id + "] at [blocks/" + haName + "]", e);
					throw e;

				}
			} catch (Exception e) {
				throw new IOException(e);
			}
			int cl = (int) omd.getContentLength();
			if (this.simpleS3) {

				FileOutputStream out = null;
				S3ObjectInputStream in = null;
				try {

					out = new FileOutputStream(f);
					in = sobj.getObjectContent();
					IOUtils.copy(in, out);
					out.flush();
					out.close();

				} catch (Exception e) {
					f.delete();
					throw new IOException(e);
				} finally {
					in.abort();
					IOUtils.closeQuietly(out);
					IOUtils.closeQuietly(in);
				}
			} else {
				try {
					this.multiPartDownload("blocks/" + haName + this.dExt, f);
				} catch (AmazonS3Exception e) {
					if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
						SDFSLogger.getLog().info("block [" + id + "] at [blocks/" + haName + "] is in glacier", e);
						throw new DataArchivedException(id, null);
					} else {
						SDFSLogger.getLog().error("unable to get block [" + id + "] at [blocks/" + haName + "]", e);
						throw e;

					}
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			Map<String, String> mp = this.getUserMetaData("blocks/" + haName);
			if (md5sum && mp.containsKey("md5sum")) {
				byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));

				InputStream in = new FileInputStream(f);
				byte[] chash = null;
				try {
					Hasher hasher = Hashing.md5().newHasher();
					ByteStreams.copy(in, Funnels.asOutputStream(hasher));
					chash = hasher.hash().asBytes();
				} catch (Exception e) {
					SDFSLogger.getLog().error("file " + f.getPath() + " exists=" + f.exists());
					throw new IOException(e);
				} finally {
					IOUtils.closeQuietly(in);
				}
				if (!Arrays.equals(shash, chash))
					throw new IOException("download corrupt at " + id);
			}

			try {
				mp.put("lastaccessed", Long.toString(System.currentTimeMillis()));
				omd.setUserMetadata(mp);

				updateObject("blocks/" + haName, omd);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error setting last accessed", e);
			}
			if (mp.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean((String) mp.get("deleted"));
				if (del) {
					S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);
					int claims = 0;
					try {
						claims = this.getClaimedObjects(kobj, id);
					} catch (Exception e) {
						throw new IOException(e);
					}

					int delobj = 0;
					if (mp.containsKey("deleted-objects")) {
						delobj = Integer.parseInt((String) mp.get("deleted-objects")) - claims;
						if (delobj < 0)
							delobj = 0;
					}
					mp.remove("deleted");
					mp.put("deleted-objects", Integer.toString(delobj));
					mp.put("suspect", "true");
					omd.setUserMetadata(mp);

					updateObject("keys/" + haName, omd);
					int _size = Integer.parseInt((String) mp.get("size"));
					int _compressedSize = Integer.parseInt((String) mp.get("compressedsize"));
					if (this.standAlone) {
						HashBlobArchive.addToLength(_size);
						HashBlobArchive.addToCompressedLength(_compressedSize);
					}
					SDFSLogger.getLog().warn("Reclaimed [" + claims + "] blocks marked for deletion");
					kobj.close();
				}
			}
			dtm = (System.currentTimeMillis() - tm) / 1000d;
			bps = (cl / 1024) / dtm;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
				SDFSLogger.getLog().error("invalid object state", e);
				SDFSLogger.getLog().info("block [" + id + "] at [blocks/" + haName + "] is in glacier", e);
				throw new DataArchivedException(id, null);
			} else {
				SDFSLogger.getLog().error("unable to get block [" + id + "] at [blocks/" + haName + "]", e);
				throw e;

			}
		} finally {
			try {
				if (sobj != null) {
					sobj.getObjectContent().abort();
					sobj.close();
				}
			} catch (Exception e) {

			}
			// this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException, DataArchivedException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		Exception e = null;
		for (int i = 0; i < 9; i++) {
			try {

				this.getData(id, f);
				return;
			} catch (DataArchivedException e1) {
				throw e1;
			} catch (Exception e1) {
				try {
					Thread.sleep(10000);
					if (f.exists())
						f.delete();
				} catch (Exception e2) {

				}
				e = e1;
			}
		}
		if (e != null) {
			SDFSLogger.getLog().error("getnewblob unable to get block", e);
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, String> getUserMetaData(String name) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		// this.s3clientLock.readLock().lock();
		if (s3Service.doesObjectExist(this.name, name + mdExt)) {
			ObjectMetadata _md = s3Service.getObjectMetadata(this.name, name + mdExt);
			int cl = (int) _md.getContentLength();
			S3Object sobj = s3Service.getObject(this.name, name + mdExt);
			byte[] data = new byte[cl];
			S3ObjectInputStream in = sobj.getObjectContent();
			IOUtils.readFully(in, data);
			CloudMetaData.Builder bl = CloudMetaData.newBuilder().mergeFrom(data);
			try {
				Map<String, String> md = new HashMap<String, String>();
				for (Entry<String, String> entry : bl.build().getAttributesMap().entrySet()) {
					md.put(entry.getKey(), entry.getValue());
				}
				return md;
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				if (in != null) {
					in.abort();
					IOUtils.closeQuietly(in);
				}
				sobj.close();
			}
		} else {
			return new HashMap<String, String>();
		}

	}

	public int verifyDelete(long id) throws IOException {
		// this.s3clientLock.readLock().lock();
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// ObjectMetadata om = null;
		S3Object kobj = null;

		int claims = 0;
		// this.s3clientLock.readLock().lock();

		try {
			kobj = null;
			try {
				kobj = s3Service.getObject(this.name, "keys/" + haName);
			} catch (AmazonS3Exception e) {
				if (e.getStatusCode() == 404) {
					SDFSLogger.getLog().warn("object keys/" + haName + " already removed");
					return 0;
				} else
					throw new IOException(e);
			}
			try {
				claims = this.getClaimedObjects(kobj, id);
			} catch (Exception e) {
				throw new IOException(e);
			}
			String name = this.getClaimName(id);
			Map<String, String> mp = this.getUserMetaData(name);

			if (claims == 0) {
				s3Service.deleteObject(this.name, this.getClaimName(id));
				s3Service.deleteObject(this.name, this.getClaimName(id) + mdExt);
				int _size = Integer.parseInt((String) mp.get("bsize"));
				int _compressedSize = Integer.parseInt((String) mp.get("bcompressedsize"));
				if (this.standAlone) {
					HashBlobArchive.addToLength(-1 * _size);
					HashBlobArchive.addToCompressedLength(-1 * _compressedSize);
					if (HashBlobArchive.getLength() < 0) {
						HashBlobArchive.setLength(0);
					}
					if (HashBlobArchive.getCompressedLength() < 0) {
						HashBlobArchive.setCompressedLength(0);
					}
				}
				ObjectListing ol = s3Service.listObjects(this.getName(), "claims/keys/" + haName + "/");
				if (ol.getObjectSummaries().size() == 0) {
					s3Service.deleteObject(this.name, "blocks/" + haName + this.dExt);
					s3Service.deleteObject(this.name, "keys/" + haName);
					SDFSLogger.getLog().info("deleted block " + "blocks/" + haName + " id " + id);
					s3Service.deleteObject(this.name, "blocks/" + haName + mdExt);
					s3Service.deleteObject(this.name, "keys/" + haName + mdExt);
				}
			}
		} finally {
			try {
				kobj.close();
			} catch (Exception e) {
			}
			// this.s3clientLock.readLock().unlock();
		}
		return claims;
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				if (this.standAlone) {
					try {
						Map<String, String> md = this.getUserMetaData(binm);
						md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
						md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
						md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
						md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
						md.put("lastupdate", Long.toString(System.currentTimeMillis()));
						md.put("hostname", InetAddress.getLocalHost().getHostName());
						md.put("port", Integer.toString(Main.sdfsCliPort));
						ObjectMetadata omd = new ObjectMetadata();
						omd.setUserMetadata(md);
						this.updateObject(binm, omd);

					} catch (Exception e) {
						try {
							ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
							Map<String, String> md = omd.getUserMetadata();
							ObjectMetadata nmd = new ObjectMetadata();
							nmd.setUserMetadata(md);
							md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
							md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
							md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
							md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
							md.put("lastupdate", Long.toString(System.currentTimeMillis()));
							md.put("hostname", InetAddress.getLocalHost().getHostName());
							md.put("port", Integer.toString(Main.sdfsCliPort));
							byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
							String st = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
							md.put("md5sum", st);
							nmd.setContentMD5(st);
							nmd.setContentLength(sz.length);
							nmd.setUserMetadata(md);

							this.updateObject(binm, nmd);
						} catch (Exception e1) {
							SDFSLogger.getLog().error("unable to update metadata for " + binm, e);
						}
					}

					if (this.deletes.size() > 0) {
						SDFSLogger.getLog().info("running garbage collection");
						BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
						ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Main.dseIOThreads, 10, TimeUnit.SECONDS,
								worksQueue, new ThreadPoolExecutor.CallerRunsPolicy());
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
							DeleteObject obj = new DeleteObject();
							obj.k = k;
							obj.odel = odel;
							obj.st = this;
							executor.execute(obj);
						}
						executor.shutdown();
						while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
							SDFSLogger.getLog().debug("Awaiting deletion task completion of threads.");
						}
						SDFSLogger.getLog().info("done running garbage collection");
					}
				}

				if (this.refresh.size() > 0) {
					SDFSLogger.getLog().info("running object refesh for sz " + this.refresh.size());
					this.delLock.lock();
					HashSet<Long> odel = null;
					try {
						odel = this.refresh;
						this.refresh = new HashSet<Long>();
						// SDFSLogger.getLog().info("delete hash table size of "
						// + odel.size());
					} finally {
						this.delLock.unlock();
					}

					for (Long k : odel) {
						try {
							SDFSLogger.getLog().debug("refreshing " + k);
							this.refreshObject(k);
							SDFSLogger.getLog().debug("done refreshing " + k);
						} catch (Exception e) {
							SDFSLogger.getLog().debug("error in refresh thread for " + k, e);
						}
					}
					odel = null;
					SDFSLogger.getLog().info("done running refresh");
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				SDFSLogger.getLog().error("error in delete thread", e);
			}
		}

	}

	public Iterator<String> getNextObjectList(String prefix) {
		// this.s3clientLock.readLock().lock();
		try {
			List<String> keys = new ArrayList<String>();
			if (ck == null) {
				ck = s3Service.listObjects(this.getName(), prefix);
			} else if (ck.isTruncated()) {
				ck = s3Service.listNextBatchOfObjects(ck);
			} else {
				return keys.iterator();
			}
			List<S3ObjectSummary> objs = ck.getObjectSummaries();
			for (S3ObjectSummary obj : objs) {

				if (obj.getKey().length() > prefix.length() && !obj.getKey().endsWith(mdExt))
					keys.add(obj.getKey());
			}
			return keys.iterator();
		} finally {
			// this.s3clientLock.readLock().unlock();
		}
	}

	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		// this.s3clientLock.readLock().lock();
		S3Object sobj = null;
		try {

			ObjectMetadata md = null;
			try {
				sobj = s3Service.getObject(getName(), key);
				md = s3Service.getObjectMetadata(this.name, key);
			} catch (Exception e) {
				throw new IOException(e);
			}
			int cl = (int) md.getContentLength();

			byte[] data = new byte[cl];
			S3ObjectInputStream in = null;
			try {
				in = sobj.getObjectContent();
				int ncl = IOUtils.readFully(in, data);
				if (ncl != cl) {
					SDFSLogger.getLog().warn("Read less bytes that expected. Expected " + cl + " read " + ncl);
				}

			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				if (in != null) {
					in.abort();
					in.close();
				}
				if (sobj != null) {
					sobj.close();
				}
			}
			boolean encrypt = false;
			boolean compress = false;
			boolean lz4compress = false;
			Map<String, String> mp = this.getUserMetaData(key);
			byte[] ivb = null;
			if (mp.containsKey("ivspec")) {
				ivb = BaseEncoding.base64().decode(mp.get("ivspec"));
			}
			if (mp.containsKey("md5sum")) {
				byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));
				byte[] chash = Hashing.md5().hashBytes(data).asBytes();
				if (!Arrays.equals(shash, chash))
					throw new IOException("download corrupt at " + sobj.getKey());
			}
			int size = Integer.parseInt(mp.get("size"));
			encrypt = Boolean.parseBoolean(mp.get("encrypt"));

			lz4compress = Boolean.parseBoolean(mp.get("lz4compress"));
			boolean changed = false;

			Long _hid = EncyptUtils.decHashArchiveName(sobj.getKey().substring(5), encrypt);

			try {
				mp = s3Service.getObjectMetadata(this.name, this.getClaimName(_hid)).getUserMetadata();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to get object " + this.getClaimName(_hid), e);
			}
			if (mp.containsKey("deleted")) {
				mp.remove("deleted");
				changed = true;
			}
			if (mp.containsKey("deleted-objects")) {
				mp.remove("deleted-objects");
				changed = true;
			}

			if (encrypt) {

				if (ivb != null) {
					data = EncryptUtils.decryptCBC(data, new IvParameterSpec(ivb));
				} else {
					data = EncryptUtils.decryptCBC(data);
				}
			}
			if (compress)
				data = CompressionUtils.decompressZLIB(data);
			else if (lz4compress) {
				data = CompressionUtils.decompressLz4(data, size);
			}

			String hast = new String(data);
			SDFSLogger.getLog().debug("reading hashes " + (String) mp.get("objects") + " from " + _hid + " encn "
					+ sobj.getKey().substring(5));
			StringTokenizer _ht = new StringTokenizer(hast, ",");
			StringResult st = new StringResult();
			st.id = _hid;
			st.st = _ht;
			if (this.standAlone) {
				if (mp.containsKey("bsize")) {
					HashBlobArchive.addToLength(Integer.parseInt(mp.get("bsize")));
				}
				if (mp.containsKey("bcompressedsize")) {
					HashBlobArchive.addToCompressedLength(Integer.parseInt(mp.get("bcompressedsize")));
				}
			}
			if (changed) {
				try {
					md = sobj.getObjectMetadata();
					md.setUserMetadata(mp);
					String kn = null;
					kn = this.getClaimName(hid);

					this.updateObject(kn, md);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			return st;
		} finally {
			if (sobj != null)
				try {
					sobj.getObjectContent().close();
					sobj.close();
				} catch (Exception e) {
				}
			// this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void sync() throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		HashBlobArchive.sync();
	}

	@Override
	public void uploadFile(File f, String to, String pp, HashMap<String, String> metaData, boolean disableComp)
			throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		// this.s3clientLock.readLock().lock();
		InputStream in = null;
		while (to.startsWith(File.separator))
			to = to.substring(1);
		to = FilenameUtils.separatorsToUnix(to);
		String pth = pp + "/" + EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		SDFSLogger.getLog().debug("uploading " + f.getPath() + " to " + to + " pth " + pth + " pp " + pp + " ");
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
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				metaData.put("lastmodified", Long.toString(f.lastModified()));
				String slp = EncyptUtils.encString(Files.readSymbolicLink(f.toPath()).toFile().getPath(),
						Main.chunkStoreEncryptionEnabled);
				metaData.put("symlink", slp);
				ObjectMetadata md = new ObjectMetadata();
				md.setContentType("binary/octet-stream");
				md.setContentLength(pth.getBytes().length);
				md.setUserMetadata(metaData);
				PutObjectRequest req = new PutObjectRequest(this.name, pth, new ByteArrayInputStream(pth.getBytes()),
						md);
				s3Service.putObject(req);
				this.updateObject(pth, md);
				this.checkoutFile(pth);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else if (isDir) {
			HashMap<String, String> _metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
			metaData.putAll(_metaData);
			metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
			metaData.put("lastmodified", Long.toString(f.lastModified()));
			metaData.put("directory", "true");
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("binary/octet-stream");
			md.setContentLength(pth.getBytes().length);
			md.setUserMetadata(metaData);
			try {
				PutObjectRequest req = new PutObjectRequest(this.name, pth, new ByteArrayInputStream(pth.getBytes()),
						md);
				s3Service.putObject(req);
				this.updateObject(pth, md);
				this.checkoutFile(pth);
				this.updateObject(pth, md);
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
				byte[] ivb = null;
				if (disableComp) {
					p = f;
				} else {
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
							ivb = PassPhrase.getByteIV();
							EncryptUtils.encryptFile(p, e, new IvParameterSpec(ivb));

						} catch (Exception e1) {
							throw new IOException(e1);
						}
						p.delete();
						p = e;
					}
				}
				String objName = pth;
				ObjectMetadata md = new ObjectMetadata();
				Map<String, String> umd = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.putAll(umd);
				md.setUserMetadata(metaData);
				md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
				md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				if (ivb != null)
					md.addUserMetadata("ivspec", BaseEncoding.base64().encode(ivb));
				md.addUserMetadata("lastmodified", Long.toString(f.lastModified()));
				if (simpleS3) {
					md.setContentType("binary/octet-stream");
					in = new BufferedInputStream(new FileInputStream(p), 32768);
					if (md5sum) {
						Hasher hasher = Hashing.md5().newHasher();
						ByteStreams.copy(in, Funnels.asOutputStream(hasher));
						String mds = BaseEncoding.base64().encode(hasher.hash().asBytes());
						in.close();
						md.setContentMD5(mds);
						md.addUserMetadata("md5sum", mds);
					}
					in = new FileInputStream(p);
					md.setContentLength(p.length());
					try {
						PutObjectRequest req = new PutObjectRequest(this.name, objName, p).withMetadata(md);
						this.multiPartUpload(req, p);
						this.checkoutFile(pth);
						this.updateObject(pth, md);
						SDFSLogger.getLog()
								.debug("uploaded=" + f.getPath() + " lm=" + md.getUserMetadata().get("lastmodified"));
					} catch (AmazonS3Exception e1) {
						if (e1.getStatusCode() == 409) {
							try {
								s3Service.deleteObject(this.name, objName);
								s3Service.deleteObject(this.name, objName + mdExt);
								this.uploadFile(f, to, pp, metaData, disableComp);
								return;
							} catch (Exception e2) {
								throw new IOException(e2);
							}
						} else {

							throw new IOException(e1);
						}
					} catch (Exception e1) {
						throw new IOException(e1);
					}
				} else {
					try {
						md.setContentType("binary/octet-stream");
						in = new BufferedInputStream(new FileInputStream(p), 32768);
						byte[] md5Hash = FileHashing.checksuMD5(p.getPath());
						in.close();
						String mds = BaseEncoding.base64().encode(md5Hash);
						md.setContentMD5(mds);
						md.addUserMetadata("md5sum", mds);

						md.setContentLength(p.length());
						PutObjectRequest req = new PutObjectRequest(this.name, objName, p).withMetadata(md);

						multiPartUpload(req, p);
						this.updateObject(pth, md);
						this.checkoutFile(pth);
					} catch (AmazonS3Exception e1) {
						if (e1.getStatusCode() == 409) {
							try {
								s3Service.deleteObject(this.name, objName);
								s3Service.deleteObject(this.name, objName + mdExt);
								this.uploadFile(f, to, pp, metaData, disableComp);
								return;
							} catch (Exception e2) {
								throw new IOException(e2);
							}
						} else {

							throw new IOException(e1);
						}
					} catch (Exception e1) {
						// SDFSLogger.getLog().error("error uploading", e1);
						throw new IOException(e1);
					}
				}
			} finally {
				try {
					if (in != null)
						in.close();
				} finally {
					if (!disableComp) {
						p.delete();
						z.delete();
						e.delete();
					}
				}
			}
		}

	}

	private void multiPartUpload(PutObjectRequest req, File file)
			throws AmazonServiceException, AmazonClientException, InterruptedException {
		// Create a list of UploadPartResponse objects. You get one of these
		// for each part upload.
		if (!this.gcsSigner) {
			List<PartETag> partETags = new ArrayList<PartETag>();

			// Step 1: Initialize.
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(this.name, req.getKey());
			InitiateMultipartUploadResult initResponse = this.s3Service.initiateMultipartUpload(initRequest);

			long contentLength = file.length();
			long partSize = this.transferSize; // Set part size to 10 MB.

			try {
				// Step 2: Upload parts.
				long filePosition = 0;
				for (int i = 1; filePosition < contentLength; i++) {
					// Last part can be less than 10 MB. Adjust part size.
					partSize = Math.min(partSize, (contentLength - filePosition));

					// Create request to upload a part.
					UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(this.name)
							.withKey(req.getKey()).withUploadId(initResponse.getUploadId()).withPartNumber(i)
							.withFileOffset(filePosition).withFile(file).withPartSize(partSize);

					// Upload part and add response to our list.
					partETags.add(this.s3Service.uploadPart(uploadRequest).getPartETag());

					filePosition += partSize;
				}

				// Step 3: Complete.
				CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(this.name, req.getKey(),
						initResponse.getUploadId(), partETags);

				s3Service.completeMultipartUpload(compRequest);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to upload object " + req.getKey(), e);
				s3Service.abortMultipartUpload(
						new AbortMultipartUploadRequest(this.name, req.getKey(), initResponse.getUploadId()));
			}
		} else {
			s3Service.putObject(req);
		}

	}

	private void multiPartDownload(String keyName, File f)
			throws AmazonServiceException, AmazonClientException, InterruptedException, IOException {
		if (this.gcsSigner) {
			S3Object obj = s3Service.getObject(this.name, keyName);
			S3ObjectInputStream is = obj.getObjectContent();
			BufferedInputStream in = new BufferedInputStream(obj.getObjectContent());
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(f));
			IOUtils.copy(in, out);
			out.flush();
			out.close();
			is.abort();
			in.close();
			obj.close();
		} else {
			try {
				Download myDownload = tx.download(this.name, keyName, f);
				myDownload.waitForCompletion();
			} finally {

			}
		}
	}

	@Override
	public void downloadFile(String nm, File to, String pp) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		// this.s3clientLock.readLock().lock();
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
		String haName = EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
		// haName = haName.replaceAll("\\", "/");
		Map<String, String> mp = null;
		byte[] shash = null;
		try {
			if (this.simpleS3) {
				S3Object obj = null;
				SDFSLogger.getLog().debug("downloading " + pp + "/" + haName);
				obj = s3Service.getObject(this.name, pp + "/" + haName);
				BufferedInputStream in = new BufferedInputStream(obj.getObjectContent());
				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(p));
				IOUtils.copy(in, out);
				out.flush();
				out.close();
				obj.getObjectContent().abort();
				in.close();
				mp = this.getUserMetaData(pp + "/" + haName);
				SDFSLogger.getLog().debug("mp sz=" + mp.size());
				try {
					if (obj != null)
						obj.close();
				} catch (Exception e1) {
				}
			} else {
				SDFSLogger.getLog().debug("downloading " + pp + "/" + haName);
				this.multiPartDownload(pp + "/" + haName, p);
				mp = this.getUserMetaData(pp + "/" + haName);
				if (md5sum && mp.containsKey("md5sum")) {
					shash = BaseEncoding.base64().decode(mp.get("md5sum"));
				}
			}
			if (shash != null && !FileUtils.fileValid(p, shash))
				throw new IOException("file " + p.getPath() + " is corrupt");
			boolean encrypt = false;
			boolean lz4compress = false;
			if (mp.containsKey("encrypt")) {
				encrypt = Boolean.parseBoolean(mp.get("encrypt"));
			}
			if (mp.containsKey("lz4compress")) {
				lz4compress = Boolean.parseBoolean(mp.get("lz4compress"));
			}
			byte[] ivb = null;
			if (mp.containsKey("ivspec")) {
				ivb = BaseEncoding.base64().decode(mp.get("ivspec"));
			}
			SDFSLogger.getLog().debug("compress=" + lz4compress + " " + mp.get("lz4compress"));

			if (mp.containsKey("symlink")) {
				if (OSValidator.isWindows())
					throw new IOException("unable to restore symlinks to windows");
				else {
					String spth = EncyptUtils.decString(mp.get("symlink"), encrypt);
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
					if (ivb != null) {
						EncryptUtils.decryptFile(p, e, new IvParameterSpec(ivb));
					} else {
						EncryptUtils.decryptFile(p, e);
					}
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
				BufferedInputStream is = new BufferedInputStream(new FileInputStream(p));
				BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(to));
				IOUtils.copy(is, os);
				os.flush();
				os.close();
				is.close();
				FileUtils.setFileMetaData(to, mp, encrypt);
				SDFSLogger.getLog().debug("updated " + to + " sz=" + to.length());
			}

		} catch (Exception e1) {
			SDFSLogger.getLog().error("unable to download " + nm +" from " + haName );
			throw new IOException(e1);
		} finally {

			p.delete();
			z.delete();
			e.delete();
		}

	}

	@Override
	public boolean exists(String nm, String pp) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		try {

			String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
			// haName.replaceAll("\\", "/");
			boolean exists = false;
			try {
				exists = s3Service.doesObjectExist(this.name, haName);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("not able to check " + haName);
			}
			return exists;
		} catch (Exception e1) {
			throw new IOException(e1);
		}
	}

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		// this.s3clientLock.readLock().lock();
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		try {

			String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
			// haName.replaceAll("\\", "/");
			boolean exists = false;
			try {
				exists = s3Service.doesObjectExist(this.name, haName);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("not able to check " + haName);
			}
			if (exists) {
				String blb = "claims/" + haName + "/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				s3Service.deleteObject(this.name, blb);
				s3Service.deleteObject(this.name, blb + mdExt);
				ObjectListing ol = s3Service.listObjects(this.getName(), "claims/" + haName + "/");
				SDFSLogger.getLog()
						.debug("deleted " + "claims/" + haName + "/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled)
								+ " object claims=" + ol.getObjectSummaries().size());
				if (ol.getObjectSummaries().size() == 0) {
					s3Service.deleteObject(this.name, haName);
					s3Service.deleteObject(this.name, haName + mdExt);
					SDFSLogger.getLog().debug("deleted " + haName);
				} else {
					SDFSLogger.getLog().debug("not deleting " + haName);
				}

			}

		} catch (Exception e1) {
			throw new IOException(e1);
		}

	}

	@Override
	public void renameFile(String from, String to, String pp) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		while (from.startsWith(File.separator))
			from = from.substring(1);
		while (to.startsWith(File.separator))
			to = to.substring(1);
		String fn = EncyptUtils.encString(from, Main.chunkStoreEncryptionEnabled);
		String tn = EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
		try {
			CopyObjectRequest req = new CopyObjectRequest(this.name, pp + "/" + fn, this.name, pp + "/" + tn);
			s3Service.copyObject(req);
			req = new CopyObjectRequest(this.name, pp + "/" + fn + mdExt, this.name, pp + "/" + tn + mdExt);
			s3Service.copyObject(req);
			s3Service.deleteObject(this.name, pp + "/" + fn);
			s3Service.deleteObject(this.name, pp + "/" + fn + mdExt);
		} catch (Exception e1) {
			throw new IOException(e1);
		}
	}

	ObjectListing nck = null;
	List<S3ObjectSummary> nsummaries = null;
	AtomicInteger nobjPos = new AtomicInteger(0);

	public void clearIter() {
		nck = null;
		nobjPos = new AtomicInteger(0);
		nsummaries = null;
	}

	public long getAllObjSummary(String pp, long id) throws IOException {
		try {
			this.clearIter();
			String pfx = pp + "/";
			long t_size = 0;
			long t_compressedsize = 0;
			int _size = 0;
			int _compressedSize = 0;
			String key = "";
			for (S3ObjectSummary summary : S3Objects.withPrefix(s3Service, this.name, pfx)) {
				key = summary.getKey();
				if (!key.endsWith(mdExt)) {
					Map<String, String> md = this.getUserMetaData(key);
					if (md.containsKey("compressedsize")) {
						_compressedSize = Integer.parseInt((String) md.get("compressedsize"));
					}
					if (md.containsKey("size")) {
						_size = Integer.parseInt((String) md.get("size"));
					}
					t_size = t_size + _size;
					t_compressedsize = t_compressedsize + _compressedSize;
				}
			}

			if (t_compressedsize >= 0) {
				HashBlobArchive.setCompressedLength(t_compressedsize);
			}

			if (t_size >= 0) {
				HashBlobArchive.setLength(t_size);
			}
			SDFSLogger.getLog().debug("length = " + t_compressedsize + " " + t_size);
			return 0;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public String getNextName(String pp, long id) throws IOException {
		try {
			String pfx = pp + "/";
			if (nck == null) {
				nck = s3Service.listObjects(this.getName(), pfx);
				nsummaries = nck.getObjectSummaries();
				nobjPos = new AtomicInteger(0);
			} else if (nobjPos.get() == nsummaries.size()) {
				nck = s3Service.listNextBatchOfObjects(nck);

				nsummaries = nck.getObjectSummaries();
				nobjPos = new AtomicInteger(0);
			}

			if (nsummaries.size() == 0) {
				nck = null;
				return null;
			}
			String ky = nsummaries.get(nobjPos.get()).getKey();
			if (!ky.endsWith(mdExt)) {
				if (ky.length() == pfx.length()) {
					nobjPos.incrementAndGet();
					return getNextName(pp, id);
				} else {
					Map<String, String> mp = this.getUserMetaData(nsummaries.get(nobjPos.get()).getKey());
					boolean encrypt = false;
					if (mp.containsKey("encrypt")) {
						encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
					}
					String pt = nsummaries.get(nobjPos.get()).getKey().substring(pfx.length());
					String fname = EncyptUtils.decString(pt, encrypt);
					nobjPos.incrementAndGet();
					return fname;

				}
			} else {
				nobjPos.incrementAndGet();
				return getNextName(pp, id);
			}

		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, Long> getHashMap(long id) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		// SDFSLogger.getLog().info("downloading map for " + id);
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		S3Object kobj = null;
		// this.s3clientLock.readLock().lock();
		try {
			try {
				kobj = s3Service.getObject(this.name, "keys/" + haName);
				kobj.getObjectMetadata();
				s3Service.getObjectMetadata(this.name, "keys/" + haName);
				kobj.getObjectMetadata();
			} catch (AmazonS3Exception e) {
				if (e.getErrorCode().equalsIgnoreCase("NoSuchKey") && Main.chunkStoreEncryptionEnabled) {
					haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled, true);
					// kobj = s3Service.getObject(this.name, "keys/" + haName);
					// kobj.getObjectMetadata();
				}
			}
			String[] ks = this.getStrings(kobj);
			HashMap<String, Long> m = new HashMap<String, Long>(ks.length + 1);
			for (String k : ks) {
				String[] kv = k.split(":");
				m.put(kv[0], Long.parseLong(kv[1]));
			}
			return m;
		} finally {
			// this.s3clientLock.readLock().unlock();
			try {
				kobj.getObjectContent().abort();
				kobj.close();
			} catch (Exception e) {
			}
		}

	}

	@Override
	public boolean checkAccess() {

		Exception e = null;
		for (int i = 0; i < 9; i++) {
			try {
				Map<String, String> obj = this.getUserMetaData(binm);
				obj.get("currentsize");
				return true;
			} catch (Exception _e) {
				e = _e;
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {

				}
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

	@Override
	public synchronized String restoreBlock(long id, byte[] hash) throws IOException {
		SDFSLogger.getLog().info("restoring block " + id);
		if (!Main.retrievalTier.equals("")) {
			String ts = Main.retrievalTier;
			if (ts.equalsIgnoreCase("standard"))
				this.glacierTier = Tier.Standard;
			if (ts.equalsIgnoreCase("expedited"))
				this.glacierTier = Tier.Expedited;
			if (ts.equalsIgnoreCase("bulk"))
				this.glacierTier = Tier.Bulk;
			SDFSLogger.getLog().info("restoring block using retrieval policy: " + this.glacierTier);
		}
		if (id == -1) {
			SDFSLogger.getLog().warn("Hash not found for " + StringUtils.getHexString(hash));
			return null;
		}
		String haName = this.restoreRequests.get(Long.valueOf(id));
		if (haName == null)
			haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		else if (haName.equalsIgnoreCase("InvalidObjectState"))
			return null;
		else {
			return haName;
		}
		SDFSLogger.getLog().info("start restore block loop for " + id);
		Exception _e = null;
		for (int i = 0; i < 9; i++) {
			try {

				RestoreObjectRequest request = new RestoreObjectRequest(this.name, "blocks/" + haName + this.dExt, 30);
				GlacierJobParameters params = new GlacierJobParameters();
				params.setTier(this.glacierTier);
				request.setGlacierJobParameters(params);
				s3Service.restoreObject(request);
				SDFSLogger.getLog().info("restoring block [" + id + "] at [blocks/" + haName + "] from glacier "
						+ this.glacierTier.toString());
				if (blockRestored(haName)) {
					restoreRequests.put(Long.valueOf(id), "InvalidObjectState");

					return null;
				}
				request = new RestoreObjectRequest(this.name, "blocks/" + haName + mdExt, 2);
				request.setGlacierJobParameters(params);
				s3Service.restoreObject(request);
				SDFSLogger.getLog().info("restoring block [" + id + "] at [blocks/" + haName + mdExt + "] from glacier "
						+ this.glacierTier.toString());
				restoreRequests.put(Long.valueOf(id), haName);
				return haName;
			} catch (AmazonS3Exception e) {
				if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
					SDFSLogger.getLog().warn("InvalidObjectState for blocks/" + haName + this.dExt);
					restoreRequests.put(Long.valueOf(id), "InvalidObjectState");
					return null;
				}
				if (e.getErrorCode().equalsIgnoreCase("RestoreAlreadyInProgress")) {
					SDFSLogger.getLog().warn("RestoreAlreadyInProgress for blocks/" + haName + this.dExt);
					restoreRequests.put(Long.valueOf(id), haName);
					return haName;
				} else {
					SDFSLogger.getLog().error(
							"Error while restoring block " + e.getErrorCode() + " id=" + id + " name=blocks/" + haName);
					_e = e;
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {

					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("general exception for blocks/" + haName + this.dExt, e);
				_e = e;
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {

				}
			}
		}
		if (_e != null) {
			SDFSLogger.getLog().error("unalbe tor restore block " + id, _e);
			throw new IOException(_e);
		} else
			return null;

	}

	@Override
	public boolean blockRestored(String id) {

		try {
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/" + id + this.dExt);
			ObjectMetadata momd = s3Service.getObjectMetadata(this.name, "blocks/" + id + mdExt);
			if (omd == null || momd == null) {
				SDFSLogger.getLog().warn("Object with id " + id + " is null");
				return false;
			}
			if (omd.getOngoingRestore() || momd.getOngoingRestore()) {
				SDFSLogger.getLog().warn("Object with id " + id + " is still restoring");
				return false;
			} else {
				SDFSLogger.getLog().warn("Object with id " + id + " preconditions met for glacier restore");
				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while checking block restored", e);
			return false;
		}
	}

	@Override
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
		try {
			BasicAWSCredentials _cred = new BasicAWSCredentials(username, password);
			if (props.containsKey("default-bucket-location")) {
				bucketLocation = RegionUtils.getRegion(props.getProperty("default-bucket-location"));
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
				clientConfig.setProxyPort(Integer.parseInt(props.getProperty("proxy-port")));
			}
			if (props.containsKey("proxy-username")) {
				clientConfig.setProxyUsername(props.getProperty("proxy-username"));
			}
			if (props.containsKey("use-v4-signer")) {
				boolean v4s = Boolean.parseBoolean(props.getProperty("use-v4-signer"));
				if (v4s) {
					clientConfig.setSignerOverride("AWSS3V4SignerType");
				}
			}
			if (props.containsKey("use-basic-signer")) {
				boolean v4s = Boolean.parseBoolean(props.getProperty("use-basic-signer"));
				// if (v4s) {
				// clientConfig.setSignerOverride("S3SignerType");
				// }
			}
			if (props.containsKey("protocol")) {
				String pr = props.getProperty("protocol");
				if (pr.equalsIgnoreCase("http"))
					clientConfig.setProtocol(Protocol.HTTP);
				else
					clientConfig.setProtocol(Protocol.HTTPS);
			}
			if (props.containsKey("default-bucket-location"))
				bucketLocation = RegionUtils.getRegion(props.getProperty("default-bucket-location"));
			s3Service = new AmazonS3Client(_cred, clientConfig);
			if (s3Target != null) {
				TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType) {
						return true;
					}
				};
				SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy,
						SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
				clientConfig.getApacheHttpClientConfig().withSslSocketFactory(sf);
				s3Service.setEndpoint(s3Target);
			}

			if (props.containsKey("disableDNSBucket")) {
				s3Service.setS3ClientOptions(S3ClientOptions.builder().disableChunkedEncoding()
						.setPathStyleAccess(Boolean.parseBoolean(props.getProperty("disableDNSBucket"))).build());
			}
			if (props.containsKey("use-accelerated-mode")) {
				s3Service.setS3ClientOptions(S3ClientOptions.builder()
						.setAccelerateModeEnabled(Boolean.parseBoolean(props.getProperty("use-accelerated-mode")))
						.build());
			}
			if (bucketLocation != null) {
				s3Service.setRegion(bucketLocation);
				System.out.println("bucketLocation=" + bucketLocation.toString());
			}
			s3Service.doesBucketExist("aaa");
			return true;
		} catch (Exception e) {
			System.err.println("Cannot authenticate to provider");
			e.printStackTrace();
			return false;
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
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}

		Map<String, String> md = this.getClaimMetaData(id);
		if (md != null)
			return;
		else {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			md = this.getUserMetaData("keys/" + haName);
			md.put("objects", Integer.toString(claims));
			if (md.containsKey("deleted")) {
				md.remove("deleted");
			}
			if (md.containsKey("deleted-objects")) {
				md.remove("deleted-objects");
			}
			if (md.containsKey("bsize")) {
				HashBlobArchive.addToLength(Integer.parseInt(md.get("bsize")));
			}
			if (md.containsKey("bcompressedsize")) {
				HashBlobArchive.addToCompressedLength(Integer.parseInt(md.get("bcompressedsize")));
			}
			byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();
			ObjectMetadata om = new ObjectMetadata();
			om.setUserMetadata(md);
			om.setContentLength(msg.length);
			try {
				String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(msg).asBytes());
				om.setContentMD5(mds);
				om.addUserMetadata("md5sum", mds);
			} catch (Exception e) {
				throw new IOException(e);
			}
			try {
				PutObjectRequest creq = new PutObjectRequest(this.name, this.getClaimName(id),
						new ByteArrayInputStream(msg), om);
				s3Service.putObject(creq);
				this.updateObject(this.getClaimName(id), om);
			} catch (AmazonS3Exception e1) {
				if (e1.getStatusCode() == 409) {
					try {
						s3Service.deleteObject(this.name, this.getClaimName(id));
						s3Service.deleteObject(name, this.getClaimName(id) + mdExt);
						this.checkoutObject(id, claims);
						return;
					} catch (Exception e2) {
						throw new IOException(e2);
					}
				} else {

					throw new IOException(e1);
				}
			} catch (Exception e1) {
				// SDFSLogger.getLog().error("error uploading", e1);
				throw new IOException(e1);
			}
		}

	}

	@Override
	public boolean objectClaimed(String key) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}

		String pth = "claims/" + key + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		try {
			s3Service.getObjectMetadata(this.name, pth);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	@Override
	public void checkoutFile(String name) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		name = FilenameUtils.separatorsToUnix(name);
		String pth = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		try {
			byte[] b = Long.toString(System.currentTimeMillis()).getBytes();
			ObjectMetadata om = new ObjectMetadata();
			String mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(b).asBytes());
			om.setContentMD5(mds);
			om.addUserMetadata("md5sum", mds);
			om.setContentLength(b.length);
			PutObjectRequest creq = new PutObjectRequest(this.name, pth, new ByteArrayInputStream(b), om);
			s3Service.putObject(creq);
			this.updateObject(pth, om);
		} catch (AmazonS3Exception e1) {
			if (e1.getStatusCode() == 409) {
				try {
					s3Service.deleteObject(this.name, pth);
					s3Service.deleteObject(this.name, pth + mdExt);
					this.checkoutFile(name);
					return;
				} catch (Exception e2) {
					throw new IOException(e2);
				}
			} else {

				throw new IOException(e1);
			}
		} catch (Exception e1) {
			// SDFSLogger.getLog().error("error uploading", e1);
			throw new IOException(e1);
		}
	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		String pth = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		// .name.this.s3clientLock.readLock().lock();
		try {

			s3Service.getObjectMetadata(this.name, pth);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public int getCheckInterval() {
		// TODO Auto-generated method stub
		return this.checkInterval;
	}

	@Override
	public boolean isClustered() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}

		ObjectListing idol = this.s3Service.listObjects(this.getName(), "bucketinfo/");
		Iterator<S3ObjectSummary> iter = idol.getObjectSummaries().iterator();
		ArrayList<RemoteVolumeInfo> al = new ArrayList<RemoteVolumeInfo>();
		while (iter.hasNext()) {
			try {
				String key = iter.next().getKey();
				if (!key.endsWith(mdExt)) {
					SDFSLogger.getLog().debug("key=" + key);
					String vid = key.substring("bucketinfo/".length());
					if (vid.length() > 0) {

						Map<String, String> md = this.getUserMetaData(key);
						long id = EncyptUtils.decHashArchiveName(vid, Main.chunkStoreEncryptionEnabled);

						RemoteVolumeInfo info = new RemoteVolumeInfo();
						info.id = id;
						info.hostname = md.get("hostname");
						info.port = Integer.parseInt(md.get("port"));
						if (md.containsKey("currentcompressedsize"))
							info.compressed = Long.parseLong(md.get("currentcompressedsize"));
						if (md.containsKey("compressedlength"))
							info.compressed = Long.parseLong(md.get("compressedlength"));
						if (md.containsKey("currentlength"))
							info.data = Long.parseLong(md.get("currentlength"));
						if (md.containsKey("currentsize"))
							info.data = Long.parseLong(md.get("currentsize"));
						if (md.containsKey("lastupdated"))
							info.lastupdated = Long.parseLong(md.get("lastupdated"));
						if (md.containsKey("lastupdate"))
							info.lastupdated = Long.parseLong(md.get("lastupdate"));
						info.metaData = md;
						al.add(info);
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to get volume metadata", e);
				throw new IOException(e);
			}

		}
		RemoteVolumeInfo[] ids = new RemoteVolumeInfo[al.size()];
		for (int i = 0; i < al.size(); i++) {
			ids[i] = al.get(i);
		}
		return ids;

	}

	@Override
	public int getMetaDataVersion() {
		// TODO Auto-generated method stub
		return this.mdVersion;
	}

	@Override
	public void removeVolume(long volumeID) throws IOException {
		if (this.closed) {
			throw new IOException("connection closed");
		}
		if (volumeID == Main.DSEID)
			throw new IOException("volume can not remove its self");
		String vid = EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		Map<String, String> obj = null;
		try {
			obj = this.getUserMetaData("bucketinfo/" + vid);

			long tm = Long.parseLong(obj.get("lastupdate"));
			long dur = System.currentTimeMillis() - tm;
			if (dur < (60000 * 2)) {
				throw new IOException("Volume [" + volumeID + "] is currently mounted");
			}

		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to find bucketinfo object", e);
		}
		ck = null;
		String suffix = "/" + vid;
		String prefix = "claims/";
		Iterator<String> iter = this.getNextObjectList("claims/");
		while (iter != null) {
			while (iter.hasNext()) {
				String nm = iter.next();
				if (nm.endsWith(suffix)) {
					s3Service.deleteObject(this.name, nm);
					s3Service.deleteObject(this.name, nm + mdExt);
					String fldr = nm.substring(0, nm.length() - suffix.length());
					SDFSLogger.getLog().debug("deleted " + fldr);
					ObjectListing ol = s3Service.listObjects(this.getName(), fldr + "/");
					if (ol.getObjectSummaries().size() == 0) {
						String fl = fldr.substring(prefix.length());
						s3Service.deleteObject(this.name, fl);
						s3Service.deleteObject(this.name, fl + mdExt);
						SDFSLogger.getLog().debug("deleted " + fl);
					}
				}
			}
			iter = null;
			iter = this.getNextObjectList("claims/");
			if (!iter.hasNext())
				iter = null;
		}
		s3Service.deleteObject(this.name, "bucketinfo/" + vid);
		s3Service.deleteObject(this.name, "bucketinfo/" + vid + mdExt);
		SDFSLogger.getLog().debug("Deleted " + volumeID);
	}

	private void refreshObject(long id) throws IOException {
		String km = "blocks/" + EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		SDFSLogger.getLog().debug("Refreshing " + km);
		CopyObjectRequest creq = new CopyObjectRequest(name, km + mdExt, name, km + mdExt);
		s3Service.copyObject(creq);
		creq = new CopyObjectRequest(name, km + this.dExt, name, km + this.dExt);
		s3Service.copyObject(creq);

	}

	@Override
	public Map<String, String> getBucketInfo() {
		try {
			return this.getUserMetaData(binm);

		} catch (Exception e) {

			SDFSLogger.getLog().warn("unable to update metadata for " + binm, e);
			return null;
		}
	}

	@Override
	public void updateBucketInfo(Map<String, String> md) {

		try {

			ObjectMetadata omd = new ObjectMetadata();
			omd.setUserMetadata(md);
			this.updateObject(binm, omd);
			byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
			String st = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
			md.put("md5sum", st);
			ObjectMetadata nmd = new ObjectMetadata();
			nmd.setUserMetadata(md);
			nmd.setContentMD5(st);
			nmd.setContentLength(sz.length);
			nmd.setUserMetadata(md);
			s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);

		} catch (Exception e) {
			try {
				ObjectMetadata nmd = new ObjectMetadata();
				nmd.setUserMetadata(md);
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(Hashing.md5().hashBytes(sz).asBytes());
				md.put("md5sum", st);
				nmd.setContentMD5(st);
				nmd.setContentLength(sz.length);
				nmd.setUserMetadata(md);

				this.updateObject(binm, nmd);
			} catch (Exception e1) {
				SDFSLogger.getLog().error("unable to update metadata for " + binm, e);
			}
		}
	}

	private void updateObject(String km, ObjectMetadata om) throws IOException {
		Map<String, String> md = om.getUserMetadata();
		CloudMetaData.Builder bl = CloudMetaData.newBuilder();
		bl.setDate(System.currentTimeMillis());
		bl.putAllAttributes(md);
		byte[] b = bl.build().toByteArray();
		om = new ObjectMetadata();
		om.setContentType("binary/octet-stream");
		om.setContentLength(b.length);
		if (md5sum) {
			String mds;
			mds = BaseEncoding.base64().encode(Hashing.md5().hashBytes(b).asBytes());
			om.setContentMD5(mds);
		}
		if (s3Service.doesObjectExist(this.name, km + mdExt + ".cpy")) {
			s3Service.deleteObject(this.name, km + mdExt + ".cpy");
			CopyObjectRequest creq = new CopyObjectRequest(name, km + mdExt, name, km + mdExt + ".cpy")
					.withNewObjectMetadata(om);
			s3Service.copyObject(creq);
			s3Service.deleteObject(this.name, km + mdExt);
			PutObjectRequest req = new PutObjectRequest(this.name, km + mdExt, new ByteArrayInputStream(b), om);
			s3Service.putObject(req);
			s3Service.deleteObject(this.name, km + mdExt + ".cpy");
		} else {
			PutObjectRequest req = new PutObjectRequest(this.name, km + mdExt, new ByteArrayInputStream(b), om);
			s3Service.putObject(req);
		}
	}

	@Override
	public void clearCounters() {
		HashBlobArchive.setLength(0);
		HashBlobArchive.setCompressedLength(0);
	}

	private static class DeleteObject implements Runnable {
		BatchAwsS3ChunkStore st = null;
		Long k;
		HashMap<Long, Integer> odel = null;

		@Override
		public void run() {
			try {
				// String hashString = EncyptUtils.encHashArchiveName(k.longValue(),
				// Main.chunkStoreEncryptionEnabled);

				String name = st.getClaimName(k.longValue());
				if (st.s3Service.doesObjectExist(st.name, name)) {
					if (st.deleteUnclaimed) {
						int cl = st.verifyDelete(k.longValue());
						if (cl == 0) {
							SDFSLogger.getLog().debug("deleted " + k.longValue());
							HashBlobArchive.removeLocalArchive(k.longValue());
						}
					} else {
						Map<String, String> mp = st.getUserMetaData(name);
						for (String s : mp.keySet()) {
							SDFSLogger.getLog().debug(s + " = " + mp.get(s));
						}
						// int objects = Integer.parseInt((String)
						// mp.get("objects"));
						int delobj = 0;
						if (mp.containsKey("deleted-objects"))
							delobj = Integer.parseInt((String) mp.get("deleted-objects"));
						// SDFSLogger.getLog().info("remove requests for " +
						// hashString + "=" + odel.get(k));
						delobj = delobj + odel.get(k);
						// SDFSLogger.getLog().info("deleting " +
						// hashString);
						mp.put("deleted", "true");
						mp.put("deleted-objects", Integer.toString(delobj));
						ObjectMetadata om = st.s3Service.getObjectMetadata(st.name, name);
						om.setUserMetadata(mp);
						String km = st.getClaimName(k.longValue());
						st.updateObject(km, om);
					}
					/*
					 *
					 * } else { try { mp.put("deleted-objects", Integer.toString(delobj));
					 * om.setUserMetadata(mp); String km = null; if (st.clustered) km =
					 * st.getClaimName(k.longValue()); else km = "keys/" + hashString;
					 * st.updateObject(km, om); }catch(Exception e) { if (st.deleteUnclaimed) {
					 * st.verifyDelete(k.longValue()); SDFSLogger.getLog().info("deleted " +
					 * k.longValue()); }else throw e; }
					 *
					 * }
					 */
				} else {
					int claims = 0;
					String[] sar = HashBlobArchive.getStrings(k.longValue()).split(",");
					if (sar != null) {
						for (String ha : sar) {
							byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
							if (HCServiceProxy.getHashesMap().mightContainKey(b, k.longValue()))
								claims++;
						}
						if (claims == 0)
							HashBlobArchive.removeLocalArchive(k.longValue());
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("Unable to delete object " + k, e);
			} finally {
			}

		}

	}

	@Override
	public void timeStampData(long key) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDseSize(long sz) {
		// TODO Auto-generated method stub

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

	private LoadingCache<String, String> fileListers = CacheBuilder.newBuilder().maximumSize(1000)
			.expireAfterAccess(5, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {

				@Override
				public String load(String key) throws Exception {
					throw new IOException("Key Not Found [" + key + "]");
				}

			});

	@Override
	public String[] listFiles(String prefix, int length, String marker) throws IOException {
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(this.getName()).withPrefix(prefix)
				.withDelimiter("/").withMaxKeys(length);
		ListObjectsV2Result result;
		if (fileListers.getIfPresent(marker) != null) {
			try {
				String ct = fileListers.get(marker);
				if (ct.equalsIgnoreCase("DONE")) {
					return new String[0];
				}
				req.setContinuationToken(fileListers.get(marker));
			} catch (ExecutionException e) {
				throw new IOException(e);
			}
		}
		ArrayList<String> al = new ArrayList<String>();
		result = s3Service.listObjectsV2(req);
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			al.add(objectSummary.getKey().substring(prefix.length()));
		}

		String token = "DONE";
		if (result.isTruncated()) {
			token = result.getNextContinuationToken();
		}
		this.fileListers.put(marker, token);
		String[] rStrings = new String[al.size()];
		al.toArray(rStrings);
		return rStrings;
	}

	@Override
	public FileInfoResponse getAttr(String name) throws NullPointerException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean get_move_blob() {
		return false;
	}

	@Override
	public void set_move_blob(boolean status) {
	}
}
