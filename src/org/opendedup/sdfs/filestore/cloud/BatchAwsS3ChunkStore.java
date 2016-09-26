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

import java.io.BufferedInputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.crypto.spec.IvParameterSpec;

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
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
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
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.model.GetObjectRequest;
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
public class BatchAwsS3ChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private static BasicAWSCredentials awsCredentials = null;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	private String name;
	private Region bucketLocation = null;
	AmazonS3Client s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean md5sum = true;
	boolean simpleS3 = false;
	private int glacierDays = 0;
	private int infrequentAccess = 0;
	private boolean clustered = true;
	private ReentrantReadWriteLock s3clientLock = new ReentrantReadWriteLock();
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	private WeakHashMap<Long, String> restoreRequests = new WeakHashMap<Long, String>();
	private int checkInterval = 15000;
	private String binm = "bucketinfo";
	private int mdVersion = 0;

	static {
		try {
			if (!Main.useAim)
				awsCredentials = new BasicAWSCredentials(Main.cloudAccessKey, Main.cloudSecretKey);
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
			SDFSLogger.getLog().info("############ Closing Bucket##################");
			HashBlobArchive.close();

			ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
			Map<String, String> md = omd.getUserMetadata();
			ObjectMetadata nmd = new ObjectMetadata();
			nmd.setUserMetadata(md);
			md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
			md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
			md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
			md.put("lastupdate", Long.toString(System.currentTimeMillis()));
			md.put("hostname", InetAddress.getLocalHost().getHostName());
			md.put("port", Integer.toString(Main.sdfsCliPort));
			byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
			String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
			md.put("md5sum", st);
			nmd.setContentMD5(st);
			nmd.setContentLength(sz.length);
			nmd.setUserMetadata(md);
			try {
				s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
			} catch (AmazonS3Exception e1) {
				if (e1.getStatusCode() == 409) {
					try {
						s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
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
		} catch (Exception e) {
			SDFSLogger.getLog().warn("error while closing bucket " + this.name, e);
		} finally {
			try {
				s3Service.shutdown();
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while closing bucket " + this.name, e);
			}
		}

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException, DataArchivedException {
		return HashBlobArchive.getBlock(hash, start);

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
		this.name = Main.cloudBucket.toLowerCase();
		this.staged_sync_location.mkdirs();
		try {
			if (config.hasAttribute("default-bucket-location")) {
				bucketLocation = RegionUtils.getRegion(config.getAttribute("default-bucket-location"));

			}
			if (config.hasAttribute("connection-check-interval")) {
				this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
			}
			if (config.hasAttribute("block-size")) {
				int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
				HashBlobArchive.MAX_LEN = sz;
			}
			if (config.hasAttribute("allow-sync")) {
				HashBlobArchive.allowSync = Boolean.parseBoolean(config.getAttribute("allow-sync"));
				if (config.hasAttribute("sync-check-schedule")) {
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
			int rsp = 0;
			int wsp = 0;
			if (config.hasAttribute("read-speed")) {
				rsp = Integer.parseInt(config.getAttribute("read-speed"));
			}
			if (config.hasAttribute("write-speed")) {
				wsp = Integer.parseInt(config.getAttribute("write-speed"));
			}
			if (config.hasAttribute("local-cache-size")) {
				long sz = StringUtils.parseSize(config.getAttribute("local-cache-size"));
				HashBlobArchive.setLocalCacheSize(sz);
			}
			if (config.hasAttribute("metadata-version")) {
				this.mdVersion = Integer.parseInt(config.getAttribute("metadata-version"));
			}
			if (config.hasAttribute("map-cache-size")) {
				int sz = Integer.parseInt(config.getAttribute("map-cache-size"));
				HashBlobArchive.MAP_CACHE_SIZE = sz;
			}
			if (config.hasAttribute("io-threads")) {
				int sz = Integer.parseInt(config.getAttribute("io-threads"));
				Main.dseIOThreads = sz;
			}
			if (config.hasAttribute("clustered")) {
				this.clustered = Boolean.parseBoolean(config.getAttribute("clustered"));
			}
			if (config.hasAttribute("delete-unclaimed")) {
				this.deleteUnclaimed = Boolean.parseBoolean(config.getAttribute("delete-unclaimed"));
			}
			if (config.hasAttribute("glacier-archive-days")) {
				this.glacierDays = Integer.parseInt(config.getAttribute("glacier-archive-days"));
				if (this.glacierDays > 0)
					Main.checkArchiveOnRead = true;
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
			if (config.hasAttribute("use-v4-signer")) {
				boolean v4s = Boolean.parseBoolean(config.getAttribute("use-v4-signer"));

				if (v4s) {
					clientConfig.setSignerOverride("AWSS3V4SignerType");
				}
			}
			if (config.hasAttribute("use-basic-signer")) {
				boolean v4s = Boolean.parseBoolean(config.getAttribute("use-basic-signer"));

				if (v4s) {
					clientConfig.setSignerOverride("S3SignerType");
				}
			}

			clientConfig.setMaxConnections(Main.dseIOThreads * 2);
			clientConfig.setConnectionTimeout(10000);
			clientConfig.setSocketTimeout(10000);

			String s3Target = null;
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

			if (s3Target != null && s3Target.toLowerCase().startsWith("https")) {
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
			if (awsCredentials != null)
				s3Service = new AmazonS3Client(awsCredentials, clientConfig);
			else
				s3Service = new AmazonS3Client(new InstanceProfileCredentialsProvider(), clientConfig);
			if (bucketLocation != null) {
				s3Service.setRegion(bucketLocation);
				System.out.println("bucketLocation=" + bucketLocation.toString());
			}
			if (s3Target != null) {
				s3Service.setEndpoint(s3Target);
				System.out.println("target=" + s3Target);
			}
			if (config.hasAttribute("disableDNSBucket")) {
				s3Service.setS3ClientOptions(new S3ClientOptions()
						.withPathStyleAccess(Boolean.parseBoolean(config.getAttribute("disableDNSBucket")))
						.disableChunkedEncoding());
				System.out.println("disableDNSBucket=" + Boolean.parseBoolean(config.getAttribute("disableDNSBucket")));
			}
			if (!s3Service.doesBucketExist(this.name)) {
				s3Service.createBucket(this.name);
				SDFSLogger.getLog().info("created new store " + name);
				ObjectMetadata md = new ObjectMetadata();
				md.addUserMetadata("currentsize", "0");
				md.addUserMetadata("currentcompressedsize", "0");
				md.addUserMetadata("clustered", "true");
				md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
				md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
				md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));

				this.clustered = true;
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				if (md5sum) {
					String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
					md.setContentMD5(mds);
				}
				md.setContentLength(sz.length);
				this.binm = "bucketinfo/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
				s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);
			} else {
				Map<String, String> obj = null;
				ObjectMetadata omd = null;
				try {
					omd = s3Service.getObjectMetadata(this.name, binm);
					obj = omd.getUserMetadata();
					obj.get("currentsize");
				} catch (Exception e) {
					omd = null;
					SDFSLogger.getLog().debug("unable to find bucketinfo object", e);
				}
				if (omd == null) {
					try {
						this.binm = "bucketinfo/"
								+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
						omd = s3Service.getObjectMetadata(this.name, binm);
						obj = omd.getUserMetadata();
						obj.get("currentsize");
					} catch (Exception e) {
						omd = null;
						SDFSLogger.getLog().debug("unable to find bucketinfo object", e);
					}
				}
				if (omd == null) {
					ObjectMetadata md = new ObjectMetadata();
					md.addUserMetadata("currentsize", "0");
					md.addUserMetadata("currentcompressedsize", "0");
					md.addUserMetadata("clustered", "true");
					md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
					md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
					md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));

					this.clustered = true;
					this.binm = "bucketinfo/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
					if (md5sum) {
						String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
						md.setContentMD5(mds);
					}
					md.setContentLength(sz.length);
					s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);
				} else {
					if (obj.containsKey("currentsize")) {
						long cl = Long.parseLong((String) obj.get("currentsize"));
						if (cl >= 0) {
							HashBlobArchive.currentLength.set(cl);

						} else
							SDFSLogger.getLog().warn("The S3 objectstore DSE did not close correctly len=" + cl);
					} else {
						SDFSLogger.getLog().warn(
								"The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}

					if (obj.containsKey("currentcompressedsize")) {
						long cl = Long.parseLong((String) obj.get("currentcompressedsize"));
						if (cl >= 0) {
							HashBlobArchive.compressedLength.set(cl);

						} else
							SDFSLogger.getLog().warn("The S3 objectstore DSE did not close correctly clen=" + cl);
					} else {
						SDFSLogger.getLog().warn(
								"The S3 objectstore DSE did not close correctly. Metadata tag currentsize was not added");
					}
					if (obj.containsKey("clustered")) {
						this.clustered = Boolean.parseBoolean(obj.get("clustered"));
					} else
						this.clustered = false;

					obj.put("clustered", Boolean.toString(this.clustered));
					omd.setUserMetadata(obj);
					try {
						CopyObjectRequest reg = new CopyObjectRequest(this.name, binm, this.name, binm)
								.withNewObjectMetadata(omd);
						s3Service.copyObject(reg);
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to update bucket info in init", e);
						SDFSLogger.getLog().info("created new store " + name);
						ObjectMetadata md = new ObjectMetadata();
						md.addUserMetadata("currentsize", "0");
						md.addUserMetadata("lastupdate", Long.toString(System.currentTimeMillis()));
						md.addUserMetadata("currentcompressedsize", "0");
						md.addUserMetadata("clustered", Boolean.toString(this.clustered));
						md.addUserMetadata("hostname", InetAddress.getLocalHost().getHostName());
						md.addUserMetadata("port", Integer.toString(Main.sdfsCliPort));
						byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
						if (md5sum) {
							String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
							md.setContentMD5(mds);
						}
						md.setContentLength(sz.length);
						s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), md);

					}
				}
			}
			ArrayList<Transition> trs = new ArrayList<Transition>();
			if (this.glacierDays > 0 && s3Target == null) {
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
				BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
						.withId("SDFS Automated Archive Rule for Block Data").withPrefix("blocks/").withTransitions(trs)
						.withStatus(BucketLifecycleConfiguration.ENABLED.toString());
				List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
				rules.add(ruleArchiveAndExpire);

				BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration().withRules(rules);

				// Save configuration.
				s3Service.setBucketLifecycleConfiguration(this.name, configuration);
			} else if (s3Target == null) {
				s3Service.deleteBucketLifecycleConfiguration(this.name);
			}
			HashBlobArchive.init(this);
			HashBlobArchive.setReadSpeed(rsp);
			HashBlobArchive.setWriteSpeed(wsp);
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
		this.s3clientLock.readLock().lock();
		try {
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
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name,sobj.getKey());
			Map<String, String> mp = this.getUserMetaData(omd);
			if (mp.containsKey("md5sum")) {
				try {
					byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));
					byte[] chash;
					chash = ServiceUtils.computeMD5Hash(data);
					if (!Arrays.equals(shash, chash))
						throw new IOException("download corrupt at " + sobj.getKey());
				} catch (NoSuchAlgorithmException e) {
					throw new IOException(e);
				}
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
			if (mp.containsKey("ivpsec"))
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
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	private int getClaimedObjects(S3Object sobj, long id) throws Exception, IOException {

		Map<String, String> mp = this.getUserMetaData(sobj.getObjectMetadata());
		if (!mp.containsKey("encrypt")) {
			mp = this.getUserMetaData(s3Service.getObjectMetadata(this.name, sobj.getKey()));
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
			dl = new MultiDownload(this, "keys/");
			dl.iterationInit(false, "keys/");
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
	public void deleteDuplicate(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
		try {
			s3Service.getObject(this.name, "blocks/" + haName);
			return true;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found") && !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return false;
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	private String getClaimName(long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		return "claims/keys/" + haName + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
	}

	private ObjectMetadata getClaimMetaData(long id) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			ObjectMetadata md = s3Service.getObjectMetadata(this.name, this.getClaimName(id));
			return md;
		} catch (AmazonServiceException e) {
			String errorCode = e.getErrorCode().trim();

			if (!errorCode.equals("404 Not Found") && !errorCode.equals("NoSuchKey")) {
				SDFSLogger.getLog().error("errorcode=[" + errorCode + "]");
				throw e;
			} else
				return null;
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
		try {
			int csz = toIntExact(arc.getFile().length());
			ObjectMetadata md = new ObjectMetadata();
			md.addUserMetadata("size", Integer.toString(arc.uncompressedLength.get()));

			md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
			md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
			md.addUserMetadata("compressedsize", Integer.toString(csz));
			md.addUserMetadata("bsize", Integer.toString(arc.getLen()));
			md.addUserMetadata("objects", Integer.toString(arc.getSz()));
			md.setContentType("binary/octet-stream");
			md.setContentLength(csz);
			if (md5sum) {
				FileInputStream in = new FileInputStream(arc.getFile());
				String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(in));
				md.setContentMD5(mds);
				md.addUserMetadata("md5sum", mds);
				IOUtils.closeQuietly(in);
			}
			PutObjectRequest req = new PutObjectRequest(this.name, "blocks/" + haName,
					new FileInputStream(arc.getFile()), md);

			if (this.simpleS3)
				s3Service.putObject(req);
			else
				this.multiPartUpload(req);
			byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();
			String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(msg));
			md.setContentMD5(mds);
			md.addUserMetadata("md5sum", mds);
			if (this.clustered) {
				md.setContentType("binary/octet-stream");
				md.setContentLength(msg.length);
				PutObjectRequest creq = new PutObjectRequest(this.name, this.getClaimName(id),
						new ByteArrayInputStream(msg), md);

				s3Service.putObject(creq);
			}
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
				mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(hs));
				md.setContentMD5(mds);
				md.addUserMetadata("md5sum", mds);
			}
			req = new PutObjectRequest(this.name, "keys/" + haName, new ByteArrayInputStream(hs), md);
			s3Service.putObject(req);
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to upload " + arc.getID() + " with id " + id, e);
			throw new IOException(e);
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	public byte[] getBytes(long id, int from, int to) throws IOException, DataArchivedException {
		// SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
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
			GetObjectRequest gr = new GetObjectRequest(this.name, "blocks/" + haName);
			gr.setRange(from, to);
			sobj = s3Service.getObject(gr);
			InputStream in = sobj.getObjectContent();
			data = new byte[cl];
			IOUtils.readFully(in, data);
			IOUtils.closeQuietly(in);
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			// mp = this.getUserMetaData(omd);
			/*
			 * try { mp.put("lastaccessed",
			 * Long.toString(System.currentTimeMillis()));
			 * omd.setUserMetadata(mp); CopyObjectRequest req = new
			 * CopyObjectRequest(this.name, "blocks/" + haName, this.name,
			 * "blocks/" + haName) .withNewObjectMetadata(omd);
			 * s3Service.copyObject(req); } catch (Exception e) {
			 * SDFSLogger.getLog().debug("error setting last accessed", e); }
			 */
			/*
			 * if (mp.containsKey("deleted")) { boolean del =
			 * Boolean.parseBoolean((String) mp.get("deleted")); if (del) {
			 * S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);
			 * 
			 * int claims = this.getClaimedObjects(kobj, id);
			 * 
			 * int delobj = 0; if (mp.containsKey("deleted-objects")) { delobj =
			 * Integer.parseInt((String) mp .get("deleted-objects")) - claims;
			 * if (delobj < 0) delobj = 0; } mp.remove("deleted");
			 * mp.put("deleted-objects", Integer.toString(delobj));
			 * mp.put("suspect", "true"); omd.setUserMetadata(mp);
			 * CopyObjectRequest req = new CopyObjectRequest(this.name, "keys/"
			 * + haName, this.name, "keys/" + haName)
			 * .withNewObjectMetadata(omd); s3Service.copyObject(req); int _size
			 * = Integer.parseInt((String) mp.get("size")); int _compressedSize
			 * = Integer.parseInt((String) mp .get("compressedsize"));
			 * HashBlobArchive.currentLength.addAndGet(_size);
			 * HashBlobArchive.compressedLength.addAndGet(_compressedSize);
			 * SDFSLogger.getLog().warn( "Reclaimed [" + claims +
			 * "] blocks marked for deletion"); kobj.close(); } }
			 */
			dtm = (System.currentTimeMillis() - tm) / 1000d;
			bps = (cl / 1024) / dtm;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState"))
				throw new DataArchivedException(id, null);
			else {
				SDFSLogger.getLog().error(
						"unable to get block [" + id + "] at [blocks/" + haName + "] pos " + from + " to " + to, e);
				throw e;

			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				if (sobj != null) {
					sobj.close();
				}
			} catch (Exception e) {

			}
			this.s3clientLock.readLock().unlock();
		}
		return data;
	}

	private void getData(long id, File f) throws Exception {
		// SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
		S3Object sobj = null;
		try {

			long tm = System.currentTimeMillis();
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/" + haName);

			try {
				sobj = s3Service.getObject(this.name, "blocks/" + haName);
			} catch (Exception e) {
				throw new IOException(e);
			}
			int cl = (int) omd.getContentLength();
			if (this.simpleS3) {

				FileOutputStream out = null;
				InputStream in = null;
				try {
					out = new FileOutputStream(f);
					in = sobj.getObjectContent();
					IOUtils.copy(in, out);
					out.flush();

				} catch (Exception e) {
					throw new IOException(e);
				} finally {
					IOUtils.closeQuietly(out);
					IOUtils.closeQuietly(in);

				}
			} else {
				this.multiPartDownload("blocks/" + haName, f);
			}
			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			Map<String, String> mp = this.getUserMetaData(omd);
			if (md5sum && mp.containsKey("md5sum")) {
				byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));

				InputStream in = new FileInputStream(f);
				byte[] chash = ServiceUtils.computeMD5Hash(in);
				IOUtils.closeQuietly(in);
				if (!Arrays.equals(shash, chash))
					throw new IOException("download corrupt at " + id);
			}

			try {
				mp.put("lastaccessed", Long.toString(System.currentTimeMillis()));
				omd.setUserMetadata(mp);
				CopyObjectRequest req = new CopyObjectRequest(this.name, "blocks/" + haName, this.name,
						"blocks/" + haName).withNewObjectMetadata(omd);
				s3Service.copyObject(req);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error setting last accessed", e);
			}
			if (mp.containsKey("deleted")) {
				boolean del = Boolean.parseBoolean((String) mp.get("deleted"));
				if (del) {
					S3Object kobj = s3Service.getObject(this.name, "keys/" + haName);

					int claims = this.getClaimedObjects(kobj, id);

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
					CopyObjectRequest req = new CopyObjectRequest(this.name, "keys/" + haName, this.name,
							"keys/" + haName).withNewObjectMetadata(omd);
					s3Service.copyObject(req);
					int _size = Integer.parseInt((String) mp.get("size"));
					int _compressedSize = Integer.parseInt((String) mp.get("compressedsize"));
					HashBlobArchive.currentLength.addAndGet(_size);
					HashBlobArchive.compressedLength.addAndGet(_compressedSize);
					SDFSLogger.getLog().warn("Reclaimed [" + claims + "] blocks marked for deletion");
					kobj.close();
				}
			}
			dtm = (System.currentTimeMillis() - tm) / 1000d;
			bps = (cl / 1024) / dtm;
		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState"))
				throw new DataArchivedException(id, null);
			else {
				SDFSLogger.getLog().error("unable to get block [" + id + "] at [blocks/" + haName + "]", e);
				throw e;

			}
		} finally {
			try {
				if (sobj != null) {
					sobj.close();
				}
			} catch (Exception e) {

			}
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException, DataArchivedException {
		Exception e = null;
		for (int i = 0; i < 5; i++) {
			try {
				this.getData(id, f);
				return;
			} catch (DataArchivedException e1) {
				throw e1;
			} catch (Exception e1) {
				e = e1;
			}
		}
		if (e != null) {
			SDFSLogger.getLog().error("getnewblob unable to get block", e);
			throw new IOException(e);
		}
	}

	private Map<String, String> getUserMetaData(ObjectMetadata obj) {
		this.s3clientLock.readLock().lock();
		try {
			if (simpleS3) {
				HashMap<String, String> omd = new HashMap<String, String>();
				Set<String> mdk = obj.getRawMetadata().keySet();
				SDFSLogger.getLog().debug("md sz=" + mdk.size());
				for (String k : mdk) {
					if (k.toLowerCase().startsWith(Headers.S3_USER_METADATA_PREFIX)) {
						String key = k.substring(Headers.S3_USER_METADATA_PREFIX.length()).toLowerCase();
						omd.put(key, (String) obj.getRawMetadataValue(k));
					}
					SDFSLogger.getLog().debug("key=" + k + " value=" + obj.getRawMetadataValue(k));
				}
				Map<String, String> zd = obj.getUserMetadata();
				mdk = zd.keySet();
				SDFSLogger.getLog().debug("md sz=" + mdk.size());
				for (String k : mdk) {
					omd.put(k.toLowerCase(), zd.get(k));
					SDFSLogger.getLog().debug("key=" + k.toLowerCase() + " value=" + zd.get(k));
				}
				return omd;
			} else {
				return obj.getUserMetadata();
			}
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	private int verifyDelete(long id) throws IOException, Exception {
		this.s3clientLock.readLock().lock();
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		ObjectMetadata om = null;
		S3Object kobj = null;

		int claims = 0;
		this.s3clientLock.readLock().lock();
		try {
			kobj = s3Service.getObject(this.name, "keys/" + haName);
			claims = this.getClaimedObjects(kobj, id);
			if (claims > 0) {
				if (this.clustered)
					om = this.getClaimMetaData(id);
				else {
					om = s3Service.getObjectMetadata(this.name, "keys/" + haName);
				}
				Map<String, String> mp = this.getUserMetaData(om);
				int delobj = 0;
				if (mp.containsKey("deleted-objects")) {
					delobj = Integer.parseInt((String) mp.get("deleted-objects")) - claims;
					if (delobj < 0)
						delobj = 0;
				}
				mp.remove("deleted");
				mp.put("deleted-objects", Integer.toString(delobj));
				mp.put("suspect", "true");
				om.setUserMetadata(mp);
				String kn = null;
				if (this.clustered)
					kn = this.getClaimName(id);
				else
					kn = "keys/" + haName;
				CopyObjectRequest req = new CopyObjectRequest(this.name, kn, this.name, kn).withNewObjectMetadata(om);
				s3Service.copyObject(req);
				int _size = Integer.parseInt((String) mp.get("size"));
				int _compressedSize = Integer.parseInt((String) mp.get("compressedsize"));
				HashBlobArchive.currentLength.addAndGet(_size);
				HashBlobArchive.compressedLength.addAndGet(_compressedSize);
				SDFSLogger.getLog().warn("Reclaimed [" + claims + "] blocks marked for deletion");

			}

			if (claims == 0) {
				if (!clustered) {
					s3Service.deleteObject(this.name, "blocks/" + haName);
					s3Service.deleteObject(this.name, "keys/" + haName);
					SDFSLogger.getLog().debug("deleted block " + "blocks/" + haName + " id " + id);
				} else {
					s3Service.deleteObject(this.name, this.getClaimName(id));
					ObjectListing ol = s3Service.listObjects(this.getName(), "claims/keys/" + haName);
					if (ol.getObjectSummaries().size() == 0) {
						s3Service.deleteObject(this.name, "blocks/" + haName);
						s3Service.deleteObject(this.name, "keys/" + haName);
						SDFSLogger.getLog().debug("deleted block " + "blocks/" + haName + " id " + id);
					}
				}
			}
		} finally {
			try {
				kobj.close();
			} catch (Exception e) {
			}
			this.s3clientLock.readLock().unlock();
		}
		return claims;
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {
					ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
					Map<String, String> md = omd.getUserMetadata();
					ObjectMetadata nmd = new ObjectMetadata();
					nmd.setUserMetadata(md);
					md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
					md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
					md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
					md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
					md.put("lastupdate", Long.toString(System.currentTimeMillis()));
					md.put("hostname", InetAddress.getLocalHost().getHostName());
					md.put("port", Integer.toString(Main.sdfsCliPort));
					byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
					String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
					md.put("md5sum", st);
					nmd.setContentMD5(st);
					nmd.setContentLength(sz.length);
					nmd.setUserMetadata(md);
					s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
				} catch (Exception e) {
					try {
						ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
						Map<String, String> md = omd.getUserMetadata();
						ObjectMetadata nmd = new ObjectMetadata();
						nmd.setUserMetadata(md);
						md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
						md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
						md.put("currentsize", Long.toString(HashBlobArchive.currentLength.get()));
						md.put("currentcompressedsize", Long.toString(HashBlobArchive.compressedLength.get()));
						md.put("lastupdate", Long.toString(System.currentTimeMillis()));
						md.put("hostname", InetAddress.getLocalHost().getHostName());
						md.put("port", Integer.toString(Main.sdfsCliPort));
						byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
						String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
						md.put("md5sum", st);
						nmd.setContentMD5(st);
						nmd.setContentLength(sz.length);
						nmd.setUserMetadata(md);
						CopyObjectRequest req = new CopyObjectRequest(this.name, binm, this.name, binm)
								.withNewObjectMetadata(nmd);
						s3Service.copyObject(req);
					} catch (Exception e1) {
						SDFSLogger.getLog().error("unable to update metadata for " + binm, e);
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
							ObjectMetadata om = null;
							if (clustered) {
								om = s3Service.getObjectMetadata(this.name, "keys/" + hashString);

							} else {
								om = s3Service.getObjectMetadata(this.name, this.getClaimName(k.longValue()));
							}
							Map<String, String> mp = this.getUserMetaData(om);
							int objects = Integer.parseInt((String) mp.get("objects"));
							int delobj = 0;
							if (mp.containsKey("deleted-objects"))
								delobj = Integer.parseInt((String) mp.get("deleted-objects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							delobj = delobj + odel.get(k);
							if (objects <= delobj) {
								// SDFSLogger.getLog().info("deleting " +
								// hashString);
								int size = Integer.parseInt((String) mp.get("bsize"));
								int compressedSize = Integer.parseInt((String) mp.get("bcompressedsize"));
								HashBlobArchive.currentLength.addAndGet(-1 * size);
								HashBlobArchive.compressedLength.addAndGet(-1 * compressedSize);
								if (this.deleteUnclaimed) {
									this.verifyDelete(k.longValue());
								} else {
									mp.put("deleted", "true");
									mp.put("deleted-objects", Integer.toString(delobj));
									om.setUserMetadata(mp);
									String km = null;
									if (clustered)
										km = this.getClaimName(k.longValue());
									else
										km = "keys/" + hashString;
									CopyObjectRequest req = new CopyObjectRequest(this.name, km, this.name, km)
											.withNewObjectMetadata(om);
									s3Service.copyObject(req);
								}
								HashBlobArchive.removeCache(k.longValue());
							} else {
								mp.put("deleted-objects", Integer.toString(delobj));
								om.setUserMetadata(mp);
								String km = null;
								if (clustered)
									km = this.getClaimName(k.longValue());
								else
									km = "keys/" + hashString;
								CopyObjectRequest req = new CopyObjectRequest(this.name, km, this.name, km)
										.withNewObjectMetadata(om);
								s3Service.copyObject(req);
							}
						} catch (Exception e) {
							SDFSLogger.getLog().warn("Unable to delete object " + hashString, e);
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

	public Iterator<String> getNextObjectList(String prefix) {
		this.s3clientLock.readLock().lock();
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

				if (obj.getKey().length() > prefix.length())
					keys.add(obj.getKey());
			}
			return keys.iterator();
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		this.s3clientLock.readLock().lock();
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
			byte[] ivb = null;
			if (mp.containsKey("ivspec")) {
				ivb = BaseEncoding.base64().decode(mp.get("ivspec"));
			}
			if (mp.containsKey("md5sum")) {
				try {
					byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));
					byte[] chash = ServiceUtils.computeMD5Hash(data);
					if (!Arrays.equals(shash, chash))
						throw new IOException("download corrupt at " + sobj.getKey());
				} catch (NoSuchAlgorithmException e) {
					throw new IOException(e);
				}
			}
			int size = Integer.parseInt(mp.get("size"));
			encrypt = Boolean.parseBoolean(mp.get("encrypt"));

			lz4compress = Boolean.parseBoolean(mp.get("lz4compress"));
			boolean changed = false;

			Long hid = EncyptUtils.decHashArchiveName(sobj.getKey().substring(5), encrypt);
			if (this.clustered)
				mp = s3Service.getObjectMetadata(this.name, this.getClaimName(hid)).getUserMetadata();
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
			SDFSLogger.getLog().debug("reading hashes " + (String) mp.get("objects") + " from " + hid + " encn "
					+ sobj.getKey().substring(5));
			StringTokenizer ht = new StringTokenizer(hast, ",");
			StringResult st = new StringResult();
			st.id = hid;
			st.st = ht;
			if (mp.containsKey("bsize")) {
				HashBlobArchive.currentLength.addAndGet(Integer.parseInt(mp.get("bsize")));
			}
			if (mp.containsKey("bcompressedsize")) {
				HashBlobArchive.compressedLength.addAndGet(Integer.parseInt(mp.get("bcompressedsize")));
			}
			if (changed) {
				try {
					md = sobj.getObjectMetadata();
					md.setUserMetadata(mp);
					String kn = null;
					if (this.clustered)
						kn = this.getClaimName(hid);
					else
						kn = sobj.getKey();
					CopyObjectRequest copyObjectRequest = new CopyObjectRequest(getName(), kn, getName(), kn);
					s3Service.copyObject(copyObjectRequest);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			return st;
		} finally {
			if (sobj != null)
				sobj.close();
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void sync() throws IOException {
		HashBlobArchive.sync();
	}

	@Override
	public void uploadFile(File f, String to, String pp) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			InputStream in = null;
			while (to.startsWith(File.separator))
				to = to.substring(1);

			String pth = pp + "/" + EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
			SDFSLogger.getLog().info("uploading " + f.getPath() + " to " + to + " pth " + pth);
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
					ObjectMetadata md = new ObjectMetadata();
					md.setContentType("binary/octet-stream");
					md.setContentLength(pth.getBytes().length);
					md.setUserMetadata(metaData);
					PutObjectRequest req = new PutObjectRequest(this.name, pth,
							new ByteArrayInputStream(pth.getBytes()), md);
					s3Service.putObject(req);
					if (this.isClustered())
						this.checkoutFile(pth);
				} catch (Exception e1) {
					throw new IOException(e1);
				}
			} else if (isDir) {
				HashMap<String, String> metaData = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
				metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
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
					if (this.isClustered())
						this.checkoutFile(pth);
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
					byte[] ivb = null;
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
					String objName = pth;
					ObjectMetadata md = new ObjectMetadata();
					Map<String, String> umd = FileUtils.getFileMetaData(f, Main.chunkStoreEncryptionEnabled);
					md.setUserMetadata(umd);
					md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
					md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
					if (ivb != null)
						md.addUserMetadata("ivspec", BaseEncoding.base64().encode(ivb));
					md.addUserMetadata("lastmodified", Long.toString(f.lastModified()));
					if (simpleS3) {
						md.setContentType("binary/octet-stream");
						in = new BufferedInputStream(new FileInputStream(p), 32768);
						try {
							if (md5sum) {
								byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
								in.close();
								String mds = BaseEncoding.base64().encode(md5Hash);
								md.setContentMD5(mds);
								md.addUserMetadata("md5sum", mds);
							}

						} catch (NoSuchAlgorithmException e2) {
							SDFSLogger.getLog().error("while hashing", e2);
							throw new IOException(e2);
						}

						in = new FileInputStream(p);
						md.setContentLength(p.length());
						try {
							PutObjectRequest req = new PutObjectRequest(this.name, objName, in, md);
							s3Service.putObject(req);
							if (this.isClustered())
								this.checkoutFile(pth);
							SDFSLogger.getLog().debug(
									"uploaded=" + f.getPath() + " lm=" + md.getUserMetadata().get("lastmodified"));
						} catch (AmazonS3Exception e1) {
							if (e1.getStatusCode() == 409) {
								try {
									s3Service.deleteObject(this.name, objName);
									this.uploadFile(f, to, pp);
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
					} else {
						try {
							md.setContentType("binary/octet-stream");
							in = new BufferedInputStream(new FileInputStream(p), 32768);
							byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
							in.close();
							String mds = BaseEncoding.base64().encode(md5Hash);
							md.setContentMD5(mds);
							md.addUserMetadata("md5sum", mds);
							in = new BufferedInputStream(new FileInputStream(p), 32768);

							md.setContentLength(p.length());
							PutObjectRequest req = new PutObjectRequest(this.name, objName, in, md);
							multiPartUpload(req);
							if (this.isClustered())
								this.checkoutFile(pth);
						} catch (AmazonS3Exception e1) {
							if (e1.getStatusCode() == 409) {
								try {
									s3Service.deleteObject(this.name, objName);
									this.uploadFile(f, to, pp);
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
						p.delete();
						z.delete();
						e.delete();
					}
				}
			}
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	private void multiPartUpload(PutObjectRequest req)
			throws AmazonServiceException, AmazonClientException, InterruptedException {
		TransferManager tx = null;
		try {
			if (awsCredentials != null)
				tx = new TransferManager(awsCredentials);
			else
				tx = new TransferManager(new InstanceProfileCredentialsProvider());
			Upload myUpload = tx.upload(req);
			myUpload.waitForCompletion();
		} finally {
			if (tx != null)
				tx.shutdownNow();
		}

	}

	private void multiPartDownload(String keyName, File f)
			throws AmazonServiceException, AmazonClientException, InterruptedException {
		TransferManager tx = null;
		try {
			if (awsCredentials != null)
				tx = new TransferManager(awsCredentials);
			else
				tx = new TransferManager(new InstanceProfileCredentialsProvider());
			Download myDownload = tx.download(this.name, keyName, f);
			myDownload.waitForCompletion();
		} finally {
			if (tx != null)
				tx.shutdownNow();
		}
	}

	@Override
	public void downloadFile(String nm, File to, String pp) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
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
					in.close();
					ObjectMetadata omd = s3Service.getObjectMetadata(name, pp + "/" + haName);
					mp = this.getUserMetaData(omd);
					SDFSLogger.getLog().debug("mp sz=" + mp.size());
					try {
						if (obj != null)
							obj.close();
					} catch (Exception e1) {
					}
				} else {
					SDFSLogger.getLog().debug("downloading " + pp + "/" + haName);
					this.multiPartDownload(pp + "/" + haName, p);
					ObjectMetadata omd = s3Service.getObjectMetadata(name, pp + "/" + haName);
					mp = this.getUserMetaData(omd);
					if (md5sum && mp.containsKey("md5sum")) {
						shash = BaseEncoding.base64().decode(omd.getUserMetaDataOf("md5sum"));
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
				throw new IOException(e1);
			} finally {

				p.delete();
				z.delete();
				e.delete();
			}
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			while (nm.startsWith(File.separator))
				nm = nm.substring(1);

			try {
				if (this.isClustered()) {
					String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
					String blb = "claims/" + haName + "/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					s3Service.deleteObject(this.name, blb);
					ObjectListing ol = s3Service.listObjects(this.getName(), "claims/" + haName + "/");
					String vid = "claims/volumes/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled) + "/"
							+ haName;
					s3Service.deleteObject(this.name, vid);
					if (ol.getObjectSummaries().size() == 0) {
						s3Service.deleteObject(this.name, haName);
					}
				} else {
					String haName = EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);

					s3Service.deleteObject(this.name, pp + "/" + haName);
				}
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	@Override
	public void renameFile(String from, String to, String pp) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			while (from.startsWith(File.separator))
				from = from.substring(1);
			while (to.startsWith(File.separator))
				to = to.substring(1);
			String fn = EncyptUtils.encString(from, Main.chunkStoreEncryptionEnabled);
			String tn = EncyptUtils.encString(to, Main.chunkStoreEncryptionEnabled);
			try {
				CopyObjectRequest req = new CopyObjectRequest(this.name, pp + "/" + fn, this.name, pp + "/" + tn);
				s3Service.copyObject(req);
				s3Service.deleteObject(this.name, pp + "/" + fn);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} finally {
			this.s3clientLock.readLock().unlock();
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

	public String getNextName(String pp, long id) throws IOException {
		this.s3clientLock.readLock().lock();
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

			if (nsummaries.size() == 0)
				return null;
			ObjectMetadata sobj = null;
			String ky = nsummaries.get(nobjPos.get()).getKey();
			if (ky.length() == pfx.length()) {
				nobjPos.incrementAndGet();
				return getNextName(pp, id);
			} else {
				sobj = s3Service.getObjectMetadata(this.name, nsummaries.get(nobjPos.get()).getKey());
				Map<String, String> mp = this.getUserMetaData(sobj);
				boolean encrypt = false;
				if (mp.containsKey("encrypt")) {
					encrypt = Boolean.parseBoolean((String) mp.get("encrypt"));
				}
				String pt = nsummaries.get(nobjPos.get()).getKey().substring(pfx.length());
				String fname = EncyptUtils.decString(pt, encrypt);
				nobjPos.incrementAndGet();
				return fname;
				/*
				 * this.downloadFile(fname, new File(to.getPath() +
				 * File.separator + fname), pp);
				 */
			}

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public Map<String, Long> getHashMap(long id) throws IOException {

		// SDFSLogger.getLog().info("downloading map for " + id);
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		S3Object kobj = null;
		this.s3clientLock.readLock().lock();
		try {
			kobj = s3Service.getObject(this.name, "keys/" + haName);
			String[] ks = this.getStrings(kobj);
			HashMap<String, Long> m = new HashMap<String, Long>(ks.length + 1);
			for (String k : ks) {
				String[] kv = k.split(":");
				m.put(kv[0], Long.parseLong(kv[1]));
			}

			return m;
		} finally {
			this.s3clientLock.readLock().unlock();
			try {
				kobj.close();
			} catch (Exception e) {
			}
		}

	}

	@Override
	public boolean checkAccess() {
		this.s3clientLock.readLock().lock();
		try {
			Exception e = null;
			for (int i = 0; i < 3; i++) {
				try {
					ObjectMetadata omd = s3Service.getObjectMetadata(this.name, binm);
					Map<String, String> obj = this.getUserMetaData(omd);
					obj.get("currentsize");
					return true;
				} catch (Exception _e) {
					e = _e;
					SDFSLogger.getLog().debug("unable to connect to bucket try " + i + " of 3", e);
				}
			}
			if (e != null)
				SDFSLogger.getLog().warn("unable to connect to bucket try " + 3 + " of 3", e);
			return false;
		} finally {
			this.s3clientLock.readLock().unlock();
		}
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
	public synchronized String restoreBlock(long id, byte[] hash) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			if (id == -1) {
				SDFSLogger.getLog().warn("Hash not found for " + StringUtils.getHexString(hash));
				return null;
			}
			String haName = this.restoreRequests.get(new Long(id));
			if (haName == null)
				haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			else if (haName.equalsIgnoreCase("InvalidObjectState"))
				return null;
			else {
				return haName;
			}
			try {
				RestoreObjectRequest request = new RestoreObjectRequest(this.name, "blocks/" + haName, 2);
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
							"Error while restoring block " + e.getErrorCode() + " id=" + id + " name=blocks/" + haName);
					throw e;
				}
			}
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	@Override
	public boolean blockRestored(String id) {
		this.s3clientLock.readLock().lock();
		try {
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/" + id);
			if (omd.getOngoingRestore())
				return false;
			else
				return true;
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	@Override
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
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
		s3Service.listBuckets();
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
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		this.s3clientLock.readLock().lock();
		try {
			if (!this.clustered)
				throw new IOException("volume is not clustered");
			ObjectMetadata om = this.getClaimMetaData(id);
			if (om != null)
				return;
			else {
				String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
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
					HashBlobArchive.currentLength.addAndGet(Integer.parseInt(md.get("bsize")));
				}
				if (md.containsKey("bcompressedsize")) {
					HashBlobArchive.compressedLength.addAndGet(Integer.parseInt(md.get("bcompressedsize")));
				}
				byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();

				om.setContentLength(msg.length);
				try {
					String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(msg));
					om.setContentMD5(mds);
					om.addUserMetadata("md5sum", mds);
				} catch (Exception e) {
					throw new IOException(e);
				}
				String dd = "claims/volumes/"
						+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled) + "/keys/"
						+ haName;
				try {
					PutObjectRequest creq = new PutObjectRequest(this.name, this.getClaimName(id),
							new ByteArrayInputStream(msg), om);
					s3Service.putObject(creq);
					creq = new PutObjectRequest(this.name, dd, new ByteArrayInputStream(msg), om);
					s3Service.putObject(creq);
				} catch (AmazonS3Exception e1) {
					if (e1.getStatusCode() == 409) {
						try {
							s3Service.deleteObject(this.name, this.getClaimName(id));
							s3Service.deleteObject(this.name, dd);
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
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public boolean objectClaimed(String key) throws IOException {

		if (!this.clustered)
			return true;

		String pth = "claims/" + key + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
		try {
			s3Service.getObjectMetadata(this.name, pth);
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			this.s3clientLock.readLock().unlock();
		}

	}

	@Override
	public void checkoutFile(String name) throws IOException {
		String pth = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		String vid = "claims/volumes/"

				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled) + "/" + name;
		this.s3clientLock.readLock().lock();
		try {
			byte[] b = Long.toString(System.currentTimeMillis()).getBytes();
			ObjectMetadata om = new ObjectMetadata();
			String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(b));
			om.setContentMD5(mds);
			om.addUserMetadata("md5sum", mds);
			om.setContentLength(b.length);
			PutObjectRequest creq = new PutObjectRequest(this.name, pth, new ByteArrayInputStream(b), om);
			s3Service.putObject(creq);
			creq = new PutObjectRequest(this.name, vid, new ByteArrayInputStream(b), om);
			s3Service.putObject(creq);
		} catch (AmazonS3Exception e1) {
			if (e1.getStatusCode() == 409) {
				try {
					s3Service.deleteObject(this.name, pth);
					s3Service.deleteObject(this.name, vid);
					this.checkoutFile(vid);
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
		} finally {
			this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
		String pth = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(volumeID, Main.chunkStoreEncryptionEnabled);
		this.s3clientLock.readLock().lock();
		try {

			s3Service.getObjectMetadata(this.name, pth);
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			this.s3clientLock.readLock().unlock();
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
		return this.clustered;
	}

	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (this.clustered) {
			ObjectListing idol = this.s3Service.listObjects(this.getName(), "bucketinfo/");
			Iterator<S3ObjectSummary> iter = idol.getObjectSummaries().iterator();
			ArrayList<RemoteVolumeInfo> al = new ArrayList<RemoteVolumeInfo>();
			while (iter.hasNext()) {
				try {
					String key = iter.next().getKey();
					SDFSLogger.getLog().info("key=" + key);
					String vid = key.substring("bucketinfo/".length());
					if (vid.length() > 0) {

						ObjectMetadata om = s3Service.getObjectMetadata(this.name, key);
						Map<String, String> md = this.getUserMetaData(om);
						long id = EncyptUtils.decHashArchiveName(vid, Main.chunkStoreEncryptionEnabled);

						RemoteVolumeInfo info = new RemoteVolumeInfo();
						info.id = id;
						info.hostname = md.get("hostname");
						info.port = Integer.parseInt(md.get("port"));
						info.compressed = Long.parseLong(md.get("currentcompressedsize"));
						info.data = Long.parseLong(md.get("currentsize"));
						info.lastupdated = Long.parseLong(md.get("lastupdate"));
						al.add(info);
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
	public int getMetaDataVersion() {
		// TODO Auto-generated method stub
		return this.mdVersion;
	}

}