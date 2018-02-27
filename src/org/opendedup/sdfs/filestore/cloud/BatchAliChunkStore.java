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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.jets3t.service.utils.ServiceUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashExistsException;
import org.opendedup.fsync.SyncFSScheduler;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.StringResult;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.filestore.cloud.utils.FileUtils;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import com.google.common.io.BaseEncoding;

/**
 *
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 *
 */
public class BatchAliChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	private HashSet<Long> refresh = new HashSet<Long>();
	private String name;
	OSS s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean md5sum = true;
	boolean simpleS3 = false;
	private int glacierDays = 0;
	private boolean clustered = true;
	// private ReentrantReadWriteLock s3clientLock = new
	// ReentrantReadWriteLock();
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	private int checkInterval = 15000;
	private String binm = "bucketinfo";
	private int mdVersion = 0;
	private boolean simpleMD;
	private final static String mdExt = ".6442";
	private String accessKey = Main.cloudAccessKey;
	private String secretKey = Main.cloudSecretKey;
	private boolean standAlone = true;

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return true;
	}

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
		return true;
	}

	public BatchAliChunkStore() {

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
		this.closed = true;
		try {
			SDFSLogger.getLog().info("############ Closing Bucket##################");
			if (this.standAlone) {
				HashBlobArchive.close();

			ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
			Map<String, String> md = null;
			if (this.simpleMD)
				md = this.getUserMetaData(binm);
			else
				md = omd.getUserMetadata();
			ObjectMetadata nmd = new ObjectMetadata();
			nmd.setUserMetadata(md);
			md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
			md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
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

				if (this.simpleMD)
					this.updateObject(binm, nmd);
				else
					s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
			} catch (OSSException e1) {
				if (Integer.parseInt(e1.getErrorCode()) == 409) {
					try {
						if (this.simpleMD)
							this.updateObject(binm, nmd);
						else
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
		// TODO Auto-generated method stub
		try {
		RemoteVolumeInfo [] rv = this.getConnectedVolumes();
		long sz = 0;
		for(RemoteVolumeInfo r : rv) {
			sz += r.data;
		}
		return sz;
		}catch(Exception e) {
			SDFSLogger.getLog().warn("unable to get clustered compressed size", e);
		}
		//return HashBlobArchive.getCompressedLength();
		return HashBlobArchive.getLength();
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len,String uuid) throws IOException {
		try {
			return HashBlobArchive.writeBlock(hash, chunk,uuid);
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

	}

	@Override
	public void init(Element config) throws IOException {
		if (config.hasAttribute("bucket-name")) {
			this.name = config.getAttribute("bucket-name").toLowerCase();
		}else {
			this.name = Main.cloudBucket.toLowerCase();
		}
		if (config.hasAttribute("access-key")) {
			this.accessKey = config.getAttribute("access-key");
		}
		if (config.hasAttribute("secret-key")) {
			this.secretKey = config.getAttribute("secret-key");
		}
		this.staged_sync_location.mkdirs();
		try {
			if (config.hasAttribute("connection-check-interval")) {
				this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
			}
			if (this.standAlone) {
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

				if (config.hasAttribute("refresh-blobs"))
					Main.REFRESH_BLOBS = Boolean.parseBoolean(config.getAttribute("refresh-blobs"));
				if (config.hasAttribute("sync-files")) {
					boolean syncf = Boolean.parseBoolean(config.getAttribute("sync-files"));
					if (syncf) {
						new FileReplicationService(this);
					}
				}
			}
			if (config.hasAttribute("simple-metadata")) {
				this.simpleMD = Boolean.parseBoolean(config.getAttribute("simple-metadata"));
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
			if (config.hasAttribute("metadata-version")) {
				this.mdVersion = Integer.parseInt(config.getAttribute("metadata-version"));
			}
			if (this.standAlone && config.hasAttribute("map-cache-size")) {
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

			if (config.hasAttribute("md5-sum")) {
				this.md5sum = Boolean.parseBoolean(config.getAttribute("md5-sum"));
				if (!this.md5sum) {
					System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
					System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
				}

			}
			ClientConfiguration clientConfig = new ClientConfiguration();

			clientConfig.setMaxConnections(Main.dseIOThreads * 2);
			clientConfig.setConnectionTimeout(120000);
			clientConfig.setSocketTimeout(120000);

			String s3Target = null;
			if (config.hasAttribute("user-agent-prefix")) {
				clientConfig.setUserAgent(config.getAttribute("SDFS/" + Main.version + ";" + "user-agent-prefix"));
			}
			if (config.getElementsByTagName("connection-props").getLength() > 0) {
				Element el = (Element) config.getElementsByTagName("connection-props").item(0);
				if (el.hasAttribute("connection-timeout"))
					clientConfig.setConnectionTimeout(Integer.parseInt(el.getAttribute("connection-timeout")));
				if (el.hasAttribute("socket-timeout"))
					clientConfig.setSocketTimeout(Integer.parseInt(el.getAttribute("socket-timeout")));
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
				} else {
					System.out.println("OSS URL Required");
					System.exit(1);
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
			s3Service = new OSSClientBuilder().build(s3Target, this.accessKey, this.secretKey);

			if (s3Target != null) {
				s3Target = s3Target.toLowerCase();
				System.out.println("target=" + s3Target);
			}
			if (!s3Service.doesBucketExist(this.name)) {
				s3Service.createBucket(this.name);
				if (this.standAlone) {
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
				if (this.simpleMD)
					this.updateObject(binm, md);
				}

			} else if(this.standAlone) {
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
					if (this.simpleMD)
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
					if (obj.containsKey("clustered")) {
						this.clustered = Boolean.parseBoolean(obj.get("clustered"));
					} else
						this.clustered = false;

					obj.put("clustered", Boolean.toString(this.clustered));

					try {
						ObjectMetadata omd = new ObjectMetadata();
						omd.setUserMetadata(obj);
						updateObject(binm, omd);
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
						if (this.simpleMD)
							this.updateObject(binm, md);

					}
				}
			}
			if (this.standAlone) {
				HashBlobArchive.init(this);
				HashBlobArchive.setReadSpeed(rsp, false);
				HashBlobArchive.setWriteSpeed(wsp, false);
			}
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

	private String[] getStrings(OSSObject sobj) throws IOException {

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
		Map<String, String> mp = this.getUserMetaData(sobj.getKey());
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

	private int getClaimedObjects(OSSObject sobj, long id) throws Exception, IOException {

		String[] st = this.getStrings(sobj);
		int claims = 0;
		for (String ha : st) {
			byte[] b = BaseEncoding.base64().decode(ha.split(":")[0]);
			if (HCServiceProxy.getHashesMap().mightContainKey(b,id))
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
				if (this.simpleMD) {
					Map<String, String> md = this.getUserMetaData(binm);
					md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
					md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
					md.put("lastupdate", Long.toString(System.currentTimeMillis()));
					md.put("hostname", InetAddress.getLocalHost().getHostName());
					md.put("port", Integer.toString(Main.sdfsCliPort));
					omd = new ObjectMetadata();
					omd.setUserMetadata(md);

				} else {
					omd = s3Service.getObjectMetadata(name, binm);
					Map<String, String> md = omd.getUserMetadata();
					md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
					md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
					md.put("currentsize", Long.toString(HashBlobArchive.getLength()));
					md.put("currentcompressedsize", Long.toString(HashBlobArchive.getCompressedLength()));
					md.put("lastupdate", Long.toString(System.currentTimeMillis()));
					md.put("hostname", InetAddress.getLocalHost().getHostName());
					md.put("port", Integer.toString(Main.sdfsCliPort));
					omd.setUserMetadata(md);

				}
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
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
		try {
		RemoteVolumeInfo [] rv = this.getConnectedVolumes();
		long sz = 0;
		for(RemoteVolumeInfo r : rv) {
			sz += r.compressed;
		}
		return sz;
		}catch(Exception e) {
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
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		try {
			s3Service.getObject(this.name, "blocks/" + haName);
			return true;
		} catch (OSSException e) {
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
		} catch (OSSException e) {
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
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		IOException e = null;
		for (int i = 0; i < 9; i++) {
			try {
				byte[] k = arc.getBytes();
				int csz = toIntExact(k.length);
				ObjectMetadata md = new ObjectMetadata();
				md.addUserMetadata("size", Integer.toString(arc.uncompressedLength.get()));
				md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
				md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
				md.addUserMetadata("compressedsize", Integer.toString(csz));
				md.addUserMetadata("bsize", Integer.toString(arc.getLen()));
				md.addUserMetadata("objects", Integer.toString(arc.getSz()));
				md.addUserMetadata("bcompressedsize", Integer.toString(csz));
				md.setContentType("binary/octet-stream");
				md.setContentLength(csz);
				if (md5sum) {
					ByteArrayInputStream in = new ByteArrayInputStream(k);
					String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(in));
					md.setContentMD5(mds);
					md.addUserMetadata("md5sum", mds);
					IOUtils.closeQuietly(in);
				}
				PutObjectRequest req = new PutObjectRequest(this.name, "blocks/" + haName, new ByteArrayInputStream(k),
						md);

				s3Service.putObject(req);

				if (this.simpleMD)
					this.updateObject("blocks/" + haName, md);
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
					if (this.simpleMD)
						this.updateObject(this.getClaimName(id), md);
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
				if (this.simpleMD)
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
		OSSObject sobj = null;
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
			 * try { mp.put("lastaccessed", Long.toString(System.currentTimeMillis()));
			 * omd.setUserMetadata(mp); CopyObjectRequest req = new
			 * CopyObjectRequest(this.name, "blocks/" + haName, this.name, "blocks/" +
			 * haName) .withNewObjectMetadata(omd); s3Service.copyObject(req); } catch
			 * (Exception e) { SDFSLogger.getLog().debug("error setting last accessed", e);
			 * }
			 */
			/*
			 * if (mp.containsKey("deleted")) { boolean del = Boolean.parseBoolean((String)
			 * mp.get("deleted")); if (del) { OSSObject kobj =
			 * s3Service.getObject(this.name, "keys/" + haName);
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
		} catch (OSSException e) {
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
			// this.s3clientLock.readLock().unlock();
		}
		return data;
	}

	private void getData(long id, File f) throws DataArchivedException, IOException {
		// SDFSLogger.getLog().info("Downloading " + id);
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		OSSObject sobj = null;
		try {
			if (f.exists() && !f.delete()) {
				SDFSLogger.getLog().warn("file already exists! " + f.getPath());
				File nf = new File(f.getPath() + " " + ".old");
				Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			long tm = System.currentTimeMillis();
			ObjectMetadata omd = s3Service.getObjectMetadata(this.name, "blocks/" + haName);
			try {
				sobj = s3Service.getObject(this.name, "blocks/" + haName);
			} catch (OSSException e) {
				if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState"))
					throw new DataArchivedException(id, null);
				else {
					SDFSLogger.getLog().error("unable to get block [" + id + "] at [blocks/" + haName + "]", e);
					throw e;

				}
			} catch (Exception e) {
				throw new IOException(e);
			}
			int cl = (int) omd.getContentLength();

			FileOutputStream out = null;
			InputStream in = null;
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
				IOUtils.closeQuietly(out);
				IOUtils.closeQuietly(in);
			}

			double dtm = (System.currentTimeMillis() - tm) / 1000d;
			double bps = (cl / 1024) / dtm;
			SDFSLogger.getLog().debug("read [" + id + "] at " + bps + " kbps");
			Map<String, String> mp = this.getUserMetaData("blocks/" + haName);
			if (md5sum && mp.containsKey("md5sum")) {
				byte[] shash = BaseEncoding.base64().decode(mp.get("md5sum"));

				in = new FileInputStream(f);
				byte[] chash = null;
				try {
					chash = ServiceUtils.computeMD5Hash(in);
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
					OSSObject kobj = s3Service.getObject(this.name, "keys/" + haName);

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
		} catch (OSSException e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState")) {
				SDFSLogger.getLog().error("invalid object state", e);
				throw new DataArchivedException(id, null);
			} else {
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
			// this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException, DataArchivedException {
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
	public Map<String, String> getBucketInfo() {
		try {
			if (this.simpleMD) {
				return this.getUserMetaData(binm);

			} else {
				ObjectMetadata omd = s3Service.getObjectMetadata(name, binm);
				Map<String, String> md = omd.getUserMetadata();
				return md;
			}
		} catch (Exception e) {

			SDFSLogger.getLog().warn("unable to update metadata for " + binm, e);
			return null;
		}
	}

	@Override
	public void updateBucketInfo(Map<String, String> md) {
		try {
			if (this.simpleMD) {

				ObjectMetadata omd = new ObjectMetadata();
				omd.setUserMetadata(md);
				this.updateObject(binm, omd);
			} else {
				ObjectMetadata nmd = new ObjectMetadata();
				nmd.setUserMetadata(md);
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
				md.put("md5sum", st);
				nmd.setContentMD5(st);
				nmd.setContentLength(sz.length);
				nmd.setUserMetadata(md);
				s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
			}
		} catch (Exception e) {
			try {
				ObjectMetadata nmd = new ObjectMetadata();
				nmd.setUserMetadata(md);
				byte[] sz = Long.toString(System.currentTimeMillis()).getBytes();
				String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
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

	@Override
	public Map<String, String> getUserMetaData(String name) throws IOException {
		// this.s3clientLock.readLock().lock();
		if (this.simpleMD) {
			if (s3Service.doesObjectExist(this.name, name + mdExt)) {
				OSSObject sobj = s3Service.getObject(this.name, name + mdExt);
				ObjectInputStream in = new ObjectInputStream(sobj.getObjectContent());
				try {
					@SuppressWarnings("unchecked")
					Map<String, String> md = (Map<String, String>) in.readObject();
					return md;
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				} finally {
					if (in != null)
						IOUtils.closeQuietly(in);
				}
			} else {
				return new HashMap<String, String>();
			}
		}
		ObjectMetadata obj = s3Service.getObjectMetadata(this.name, name);
		try {
			if (simpleS3) {
				HashMap<String, String> omd = new HashMap<String, String>();
				Map<String, String> zd = obj.getUserMetadata();
				Set<String> mdk = zd.keySet();
				SDFSLogger.getLog().debug("md sz=" + mdk.size());
				for (String k : mdk) {
					omd.put(k.toLowerCase(), zd.get(k));
					SDFSLogger.getLog().debug("key=" + k.toLowerCase() + " value=" + zd.get(k));
				}
				return omd;
			} else {

				Map<String, String> md = obj.getUserMetadata();
				return md;
			}
		} finally {
			// this.s3clientLock.readLock().unlock();
		}

	}

	public int verifyDelete(long id) throws IOException {
		// this.s3clientLock.readLock().lock();
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		// ObjectMetadata om = null;
		OSSObject kobj = null;

		int claims = 0;
		// this.s3clientLock.readLock().lock();

		try {
			kobj = s3Service.getObject(this.name, "keys/" + haName);
			try {
				claims = this.getClaimedObjects(kobj, id);
			} catch (Exception e) {
				throw new IOException(e);
			}
			String name = null;
			if (this.clustered)
				name = this.getClaimName(id);
			else {
				name = "keys/" + haName;
			}
			Map<String, String> mp = this.getUserMetaData(name);

			if (claims == 0) {
				if (!clustered) {
					s3Service.deleteObject(this.name, "blocks/" + haName);
					s3Service.deleteObject(this.name, "keys/" + haName);
					SDFSLogger.getLog().debug("deleted block " + "blocks/" + haName + " id " + id);
					if (this.simpleMD) {
						s3Service.deleteObject(this.name, "blocks/" + haName + mdExt);
						s3Service.deleteObject(this.name, "keys/" + haName + mdExt);
					}
				} else {
					s3Service.deleteObject(this.name, this.getClaimName(id));
					if (this.simpleMD)
						s3Service.deleteObject(this.name, this.getClaimName(id) + mdExt);
					int _size = Integer.parseInt((String) mp.get("size"));
					int _compressedSize = Integer.parseInt((String) mp.get("compressedsize"));
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
					ObjectListing ol = s3Service.listObjects(this.getName(), "claims/keys/" + haName);
					if (ol.getObjectSummaries().size() == 0) {
						s3Service.deleteObject(this.name, "blocks/" + haName);
						s3Service.deleteObject(this.name, "keys/" + haName);
						SDFSLogger.getLog().debug("deleted block " + "blocks/" + haName + " id " + id);
						if (this.simpleMD) {
							s3Service.deleteObject(this.name, "blocks/" + haName + mdExt);
							s3Service.deleteObject(this.name, "keys/" + haName + mdExt);
						}
					}

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
				if(this.standAlone) {
				try {

					if (this.simpleMD) {
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
					} else {
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
						String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
						md.put("md5sum", st);
						nmd.setContentMD5(st);
						nmd.setContentLength(sz.length);
						nmd.setUserMetadata(md);
						s3Service.putObject(this.name, binm, new ByteArrayInputStream(sz), nmd);
					}
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
						String st = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(sz));
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
					SDFSLogger.getLog().info("running object refesh");
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
							SDFSLogger.getLog().error("error in refresh thread", e);
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
				ck = s3Service.listObjects(new ListObjectsRequest(this.getName()).withMarker(ck.getNextMarker()));
			} else {
				return keys.iterator();
			}
			List<OSSObjectSummary> objs = ck.getObjectSummaries();
			for (OSSObjectSummary obj : objs) {

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
		OSSObject sobj = null;
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
			Map<String, String> mp = this.getUserMetaData(key);
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

			Long _hid = EncyptUtils.decHashArchiveName(sobj.getKey().substring(5), encrypt);
			if (this.clustered)
				mp = s3Service.getObjectMetadata(this.name, this.getClaimName(_hid)).getUserMetadata();
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
					if (this.clustered)
						kn = this.getClaimName(hid);
					else
						kn = sobj.getKey();

					this.updateObject(kn, md);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			return st;
		} finally {
			if (sobj != null)
				try {
					sobj.close();
				} catch (Exception e) {
				}
			// this.s3clientLock.readLock().unlock();
		}
	}

	@Override
	public void sync() throws IOException {
		HashBlobArchive.sync();
	}

	@Override
	public void uploadFile(File f, String to, String pp, HashMap<String, String> metaData, boolean disableComp)
			throws IOException {
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
				if (this.simpleMD)
					this.updateObject(pth, md);
				if (this.isClustered())
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
				if (this.isClustered())
					this.checkoutFile(pth);
				if (this.simpleMD)
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
				if (disableComp)
					p = f;
				else {
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
				if (!disableComp) {
					md.addUserMetadata("lz4compress", Boolean.toString(Main.compress));
					md.addUserMetadata("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
					if (ivb != null)
						md.addUserMetadata("ivspec", BaseEncoding.base64().encode(ivb));
					md.addUserMetadata("lastmodified", Long.toString(f.lastModified()));
				}
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
					if (this.simpleMD)
						this.updateObject(pth, md);
					SDFSLogger.getLog()
							.debug("uploaded=" + f.getPath() + " lm=" + md.getUserMetadata().get("lastmodified"));
				} catch (OSSException e1) {

					throw new IOException(e1);
				} catch (Exception e1) {
					// SDFSLogger.getLog().error("error uploading", e1);
					throw new IOException(e1);
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

	@Override
	public void downloadFile(String nm, File to, String pp) throws IOException {
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
		Map<String, String> mp = null;
		try {
			OSSObject obj = null;
			SDFSLogger.getLog().debug("downloading " + pp + "/" + haName);
			obj = s3Service.getObject(this.name, pp + "/" + haName);
			BufferedInputStream in = new BufferedInputStream(obj.getObjectContent());
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(p));
			IOUtils.copy(in, out);
			out.flush();
			out.close();
			in.close();
			mp = this.getUserMetaData(pp + "/" + haName);
			SDFSLogger.getLog().debug("mp sz=" + mp.size());
			try {
				if (obj != null)
					obj.close();
			} catch (Exception e1) {
			}

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

	}

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		// this.s3clientLock.readLock().lock();
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);

		try {
			if (this.isClustered()) {

				String haName = pp + "/" + EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);
				SDFSLogger.getLog().debug("deleting " + haName);
				if (s3Service.doesObjectExist(this.name, haName)) {
					String blb = "claims/" + haName + "/"
							+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
					s3Service.deleteObject(this.name, blb);
					if (this.simpleMD)
						s3Service.deleteObject(this.name, blb + mdExt);
					ObjectListing ol = s3Service.listObjects(this.getName(), "claims/" + haName + "/");
					SDFSLogger.getLog()
							.info("deleted " + "claims/" + haName + "/"
									+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled)
									+ " object claims=" + ol.getObjectSummaries().size());
					if (ol.getObjectSummaries().size() == 0) {
						s3Service.deleteObject(this.name, haName);
						if (this.simpleMD)
							s3Service.deleteObject(this.name, haName + mdExt);
						SDFSLogger.getLog().info("deleted " + haName);
					} else {
						SDFSLogger.getLog().info("not deleting " + haName);
					}

				}
			} else {
				String haName = EncyptUtils.encString(nm, Main.chunkStoreEncryptionEnabled);

				s3Service.deleteObject(this.name, pp + "/" + haName);
				if (this.simpleMD)
					s3Service.deleteObject(this.name, pp + "/" + haName + mdExt);
			}
		} catch (Exception e1) {
			throw new IOException(e1);
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
			CopyObjectRequest req = new CopyObjectRequest(this.name, pp + "/" + fn, this.name, pp + "/" + tn);
			s3Service.copyObject(req);
			req = new CopyObjectRequest(this.name, pp + "/" + fn + mdExt, this.name, pp + "/" + tn + mdExt);
			s3Service.copyObject(req);
			s3Service.deleteObject(this.name, pp + "/" + fn);
			if (this.simpleMD)
				s3Service.deleteObject(this.name, pp + "/" + fn + mdExt);
		} catch (Exception e1) {
			throw new IOException(e1);
		}
	}

	ObjectListing nck = null;
	List<OSSObjectSummary> nsummaries = null;
	AtomicInteger nobjPos = new AtomicInteger(0);

	public void clearIter() {
		nck = null;
		nobjPos = new AtomicInteger(0);
		nsummaries = null;
	}

	public String getNextName(String pp, long id) throws IOException {
		try {
			String pfx = pp + "/";
			if (nck == null) {
				nck = s3Service.listObjects(this.getName(), pfx);
				nsummaries = nck.getObjectSummaries();
				nobjPos = new AtomicInteger(0);
			} else if (nobjPos.get() == nsummaries.size()) {
				nck = s3Service.listObjects(new ListObjectsRequest(this.getName()).withMarker(nck.getNextMarker()));
				nsummaries = nck.getObjectSummaries();
				nobjPos = new AtomicInteger(0);
			}

			if (nsummaries.size() == 0)
				return null;
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
					/*
					 * this.downloadFile(fname, new File(to.getPath() + File.separator + fname),
					 * pp);
					 */
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

		// SDFSLogger.getLog().info("downloading map for " + id);
		String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		OSSObject kobj = null;
		// this.s3clientLock.readLock().lock();
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
			// this.s3clientLock.readLock().unlock();
			try {
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
		return null;

	}

	@Override
	public boolean blockRestored(String id) {

		return false;
	}

	@Override
	public boolean checkAccess(String username, String password, Properties props) throws Exception {
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

		if (!this.clustered)
			throw new IOException("volume is not clustered");
		Map<String, String> md = this.getClaimMetaData(id);
		if (md != null)
			return;
		else {
			md = new HashMap<String, String>();
			md.put("objects", Integer.toString(claims));
			if (md.containsKey("deleted")) {
				md.remove("deleted");
			}
			if (md.containsKey("deleted-objects")) {
				md.remove("deleted-objects");
			}
			if (this.standAlone) {
				if (md.containsKey("bsize")) {
					HashBlobArchive.addToLength(Integer.parseInt(md.get("bsize")));
				}
				if (md.containsKey("bcompressedsize")) {
					HashBlobArchive.addToCompressedLength(Integer.parseInt(md.get("bcompressedsize")));
				}
			}
			byte[] msg = Long.toString(System.currentTimeMillis()).getBytes();
			ObjectMetadata om = new ObjectMetadata();
			om.setUserMetadata(md);
			om.setContentLength(msg.length);
			try {
				String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(msg));
				om.setContentMD5(mds);
				om.addUserMetadata("md5sum", mds);
			} catch (Exception e) {
				throw new IOException(e);
			}
			try {
				PutObjectRequest creq = new PutObjectRequest(this.name, this.getClaimName(id),
						new ByteArrayInputStream(msg), om);
				s3Service.putObject(creq);
				if (this.simpleMD)
					this.updateObject(this.getClaimName(id), om);
			} catch (OSSException e1) {

				throw new IOException(e1);
			} catch (Exception e1) {
				// SDFSLogger.getLog().error("error uploading", e1);
				throw new IOException(e1);
			}
		}

	}

	@Override
	public boolean objectClaimed(String key) throws IOException {

		if (!this.clustered)
			return true;

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
		name = FilenameUtils.separatorsToUnix(name);
		String pth = "claims/" + name + "/"
				+ EncyptUtils.encHashArchiveName(Main.DSEID, Main.chunkStoreEncryptionEnabled);
		// this.s3clientLock.readLock().lock();
		try {
			byte[] b = Long.toString(System.currentTimeMillis()).getBytes();
			ObjectMetadata om = new ObjectMetadata();
			String mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(b));
			om.setContentMD5(mds);
			om.addUserMetadata("md5sum", mds);
			om.setContentLength(b.length);
			PutObjectRequest creq = new PutObjectRequest(this.name, pth, new ByteArrayInputStream(b), om);
			s3Service.putObject(creq);
			if (this.simpleMD)
				this.updateObject(pth, om);
		} catch (OSSException e1) {

			throw new IOException(e1);
		} catch (Exception e1) {
			// SDFSLogger.getLog().error("error uploading", e1);
			throw new IOException(e1);
		}
	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
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
		return this.clustered;
	}

	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (this.clustered) {
			ObjectListing idol = this.s3Service.listObjects(this.getName(), "bucketinfo/");
			Iterator<OSSObjectSummary> iter = idol.getObjectSummaries().iterator();
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
							info.compressed = Long.parseLong(md.get("currentcompressedsize"));
							info.data = Long.parseLong(md.get("currentsize"));
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

	@Override
	public void removeVolume(long volumeID) throws IOException {
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
					if (this.simpleMD)
						s3Service.deleteObject(this.name, nm + mdExt);
					String fldr = nm.substring(0, nm.length() - suffix.length());
					SDFSLogger.getLog().debug("deleted " + fldr);
					ObjectListing ol = s3Service.listObjects(this.getName(), fldr + "/");
					if (ol.getObjectSummaries().size() == 0) {
						String fl = fldr.substring(prefix.length());
						s3Service.deleteObject(this.name, fl);
						if (this.simpleMD)
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
		if (this.simpleMD)
			s3Service.deleteObject(this.name, "bucketinfo/" + vid + mdExt);
		SDFSLogger.getLog().debug("Deleted " + volumeID);
	}

	private void refreshObject(long id) throws IOException {
		String km = "blocks/" + EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
		if (this.simpleMD) {
			CopyObjectRequest creq = new CopyObjectRequest(name, km + mdExt, name, km + mdExt);
			s3Service.copyObject(creq);
			creq = new CopyObjectRequest(name, km, name, km);
			s3Service.copyObject(creq);
		} else {
			try {
				CopyObjectRequest req = new CopyObjectRequest(name, km, name, km);
				s3Service.copyObject(req);
			} catch (OSSException e) {
				CopyObjectRequest req = new CopyObjectRequest(name, km, name, km);
				s3Service.copyObject(req);
			}
		}
	}

	private void updateObject(String km, ObjectMetadata om) throws IOException {
		if (this.simpleMD) {
			Map<String, String> md = om.getUserMetadata();
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(bo);
			o.writeObject(md);
			byte[] b = bo.toByteArray();
			om = new ObjectMetadata();
			om.setContentType("binary/octet-stream");
			om.setContentLength(b.length);
			if (md5sum) {
				String mds;
				try {
					mds = BaseEncoding.base64().encode(ServiceUtils.computeMD5Hash(b));
				} catch (NoSuchAlgorithmException e) {
					throw new IOException(e);
				}
				om.setContentMD5(mds);
			}
			if (s3Service.doesObjectExist(this.name, km + mdExt + ".cpy")) {
				s3Service.deleteObject(this.name, km + mdExt + ".cpy");
				CopyObjectRequest creq = new CopyObjectRequest(name, km + mdExt, name, km + mdExt + ".cpy");
				creq.setNewObjectMetadata(om);
				s3Service.copyObject(creq);
				s3Service.deleteObject(this.name, km + mdExt);
				PutObjectRequest req = new PutObjectRequest(this.name, km + mdExt, new ByteArrayInputStream(b), om);
				s3Service.putObject(req);
				s3Service.deleteObject(this.name, km + mdExt + ".cpy");
			} else {
				PutObjectRequest req = new PutObjectRequest(this.name, km + mdExt, new ByteArrayInputStream(b), om);
				s3Service.putObject(req);
			}

		} else {
			try {
				CopyObjectRequest req = new CopyObjectRequest(name, km, name, km);
				req.setNewObjectMetadata(om);
				s3Service.copyObject(req);
			} catch (OSSException e) {

				CopyObjectRequest req = new CopyObjectRequest(name, km, name, km + ".cpy");
				req.setNewObjectMetadata(om);
				s3Service.copyObject(req);
				s3Service.deleteObject(name, km);
				req = new CopyObjectRequest(name, km + ".cpy", name, km);
				req.setNewObjectMetadata(om);
				s3Service.copyObject(req);
				s3Service.deleteObject(name, km + ".cpy");
			}
		}
	}

	@Override
	public void clearCounters() {
		if (this.standAlone) {
			HashBlobArchive.setLength(0);
			HashBlobArchive.setCompressedLength(0);
		}
	}

	private static class DeleteObject implements Runnable {
		BatchAliChunkStore st = null;
		Long k;
		HashMap<Long, Integer> odel = null;

		@Override
		public void run() {
			try {
				String hashString = EncyptUtils.encHashArchiveName(k.longValue(), Main.chunkStoreEncryptionEnabled);

				String name = null;
				if (!st.clustered) {
					name = "keys/" + hashString;

				} else {
					name = st.getClaimName(k.longValue());
				}
				if (st.s3Service.doesObjectExist(st.name, name)) {
					if (st.deleteUnclaimed) {
						int cl = st.verifyDelete(k.longValue());
						if (cl == 0) {
							SDFSLogger.getLog().debug("deleted " + k.longValue());
							HashBlobArchive.removeCache(k.longValue());
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
						String km = null;
						if (st.clustered)
							km = st.getClaimName(k.longValue());
						else
							km = "keys/" + hashString;
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
		byte [] k = new byte[7];
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
	public long getAllObjSummary(String pp, long id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
