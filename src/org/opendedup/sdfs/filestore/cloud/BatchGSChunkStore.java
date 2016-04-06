package org.opendedup.sdfs.filestore.cloud;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.HashBlobArchiveNoMap;
import org.opendedup.sdfs.filestore.StringResult;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.apache.commons.compress.utils.IOUtils;
import org.bouncycastle.util.Arrays;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.model.StorageBucket;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.GSCredentials;
import org.jets3t.service.utils.ServiceUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
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
public class BatchGSChunkStore implements AbstractChunkStore,
		AbstractBatchStore, Runnable, AbstractCloudFileSync {
	private static GSCredentials awsCredentials = null;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	private String name;
	private String bucketLocation = null;
	GoogleStorageService s3Service = null;
	boolean closed = false;
	boolean deleteUnclaimed = true;
	File staged_sync_location = new File(Main.chunkStore + File.separator
			+ "syncstaged");

	static {
		try {
			awsCredentials = new GSCredentials(Main.cloudAccessKey,
					Main.cloudSecretKey);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			System.out.println("unable to authenticate");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		GSCredentials creds = null;
		try {
			creds = new GSCredentials(awsAccessKey, awsSecretKey);
			RestS3Service s3Service = new RestS3Service(creds);
			s3Service.listAllBuckets();
			s3Service.shutdown();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			return false;
		}
	}

	public static boolean checkBucketUnique(String awsAccessKey,
			String awsSecretKey, String bucketName) {
		GSCredentials creds = null;
		try {
			creds = new GSCredentials(awsAccessKey, awsSecretKey);
			RestS3Service s3Service = new RestS3Service(creds);
			StorageBucket s3Bucket = s3Service.getBucket(bucketName);
			if (s3Bucket == null) {
				s3Bucket = s3Service.createBucket(bucketName);
			}
			s3Service.shutdown();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to create aws bucket", e);
			return false;
		}
	}

	public BatchGSChunkStore() {

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
			HashBlobArchive.close();
			GSObject obj = s3Service.getObject(this.name, "bucketinfo");
			obj.removeMetadata("currentsize");
			obj.addMetadata("currentsize",
					Long.toString(HashBlobArchive.currentLength.get()));
			obj.removeMetadata("currentcompressedsize");
			obj.addMetadata("currentcompressedsize",
					Long.toString(HashBlobArchive.compressedLength.get()));
			s3Service.updateObjectMetadata(this.name, obj);
			obj.closeDataInputStream();
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
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {

	}

	@Override
	public long size() {
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
			GSCredentials bawsCredentials = new GSCredentials(awsAccessKey,
					awsSecretKey);
			GoogleStorageService bs3Service = new GoogleStorageService(
					bawsCredentials);
			GSObject[] obj = bs3Service.listObjects(bucketName);
			for (int i = 0; i < obj.length; i++) {
				bs3Service.deleteObject(bucketName, obj[i].getKey());
				System.out.print(".");
			}
			bs3Service.deleteBucket(bucketName);
			SDFSLogger.getLog().info("Bucket [" + bucketName + "] deleted");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (ServiceException e) {
			SDFSLogger.getLog()
					.warn("Unable to delete bucket " + bucketName, e);
		}
	}

	private String encHashArchiveName(long id, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(Long.toString(id).getBytes());
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return Long.toString(id);
		}
	}

	private long decHashArchiveName(String fname, boolean enc)
			throws IOException {
		if (enc) {
			byte[] encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));

			return Long.parseLong(st);
		} else {
			return Long.parseLong(fname);
		}
	}

	private String encString(String hashes, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(hashes.getBytes());
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return hashes;
		}
	}

	private String decString(String fname, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));
			return st;
		} else {
			return fname;
		}
	}

	@Override
	public void init(Element config) throws IOException {
		this.name = Main.cloudBucket;
		this.staged_sync_location.mkdirs();
		try {
			if (config.hasAttribute("default-bucket-location")) {
				bucketLocation = config.getAttribute("default-bucket-location");
			}

			if (config.hasAttribute("block-size")) {
				int sz = (int) StringUtils.parseSize(config
						.getAttribute("block-size"));
				HashBlobArchive.MAX_LEN = sz;

			}
			if (config.hasAttribute("allow-sync")) {
				HashBlobArchive.allowSync = Boolean.parseBoolean(config
						.getAttribute("allow-sync"));

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
			if (config.hasAttribute("local-cache-size")) {
				long sz = StringUtils.parseSize(config
						.getAttribute("local-cache-size"));
				HashBlobArchive.setLocalCacheSize(sz);
			}
			int rsp = 0;
			int wsp = 0;
			if (config.hasAttribute("read-speed")) {
				rsp = Integer.parseInt(config.getAttribute("read-speed"));
			}
			if (config.hasAttribute("write-speed")) {
				wsp = Integer.parseInt(config.getAttribute("write-speed"));
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
			Jets3tProperties jProps = Jets3tProperties
					.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
			jProps.setProperty("httpclient.max-connections",
					Integer.toString(Main.dseIOThreads * 2));
			jProps.setProperty("s3service.max-thread-count",
					Integer.toString(Main.dseIOThreads * 2));
			jProps.setProperty("s3service.admin-max-thread-count",
					Integer.toString(Main.dseIOThreads * 2));
			jProps.setProperty("httpclient.connection-timeout-ms",
					Integer.toString(10000));
			jProps.setProperty("httpclient.socket-timeout-ms",
					Integer.toString(10000));
			if (config.getElementsByTagName("connection-props").getLength() > 0) {
				Element el = (Element) config.getElementsByTagName(
						"connection-props").item(0);
				NamedNodeMap ls = el.getAttributes();
				for (int i = 0; i < ls.getLength(); i++) {
					jProps.setProperty(ls.item(i).getNodeName(), ls.item(i)
							.getNodeValue());
					SDFSLogger.getLog().info(
							"set aws connection value "
									+ ls.item(i).getNodeName() + " to "
									+ ls.item(i).getNodeValue());
				}
			}

			s3Service = new GoogleStorageService(awsCredentials,
					"SDFS Filesysem", null, jProps);
			StorageBucket s3Bucket = null;
			try {
				s3Bucket = s3Service.getBucket(this.name);
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"Bucket does not exist [" + this.name + "]", e);
			}
			if (s3Bucket == null) {
				if (bucketLocation == null)
					s3Bucket = s3Service.createBucket(this.name);
				else
					s3Bucket = s3Service.createBucket(this.name,
							bucketLocation, null);
				SDFSLogger.getLog().info("created new store " + name);
				GSObject s3Object = new GSObject("bucketinfo");
				s3Object.addMetadata("currentsize", "-1");
				s3Object.addMetadata("currentcompressedsize", "-1");
				s3Service.putObject(this.name, s3Object);
				s3Object.closeDataInputStream();
			} else {
				GSObject obj = null;
				try {

					try {
						obj = s3Service.getObject(this.name, "bucketinfo");
					} catch (Exception e) {
						SDFSLogger.getLog().debug(
								"unable to find bucketinfo object", e);
					}
					if (obj == null) {
						GSObject s3Object = new GSObject("bucketinfo");
						s3Object.addMetadata("currentsize", "0");
						s3Object.addMetadata("currentcompressedsize", "0");
						s3Service.putObject(this.name, s3Object);
						s3Object.closeDataInputStream();

					} else {
						if (obj.containsMetadata("currentsize")) {
							long cl = Long.parseLong((String) obj
									.getMetadata("currentsize"));
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

						if (obj.containsMetadata("currentcompressedsize")) {
							long cl = Long.parseLong((String) obj
									.getMetadata("currentcompressedsize"));
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
						s3Service.updateObjectMetadata(this.name, obj);
					}
				} finally {
					if (obj != null)
						obj.closeDataInputStream();
				}
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

	private StringTokenizer ht = null;
	private long hid = 0;

	@Override
	public ChunkData getNextChunck() throws IOException {

		if (ht != null && ht.hasMoreElements()) {
			return new ChunkData(BaseEncoding.base64().decode(
					ht.nextToken().split(":")[0]), hid);
		}
		try {
			if (objPos >= obj.length) {
				StorageObjectsChunk ck = s3Service.listObjectsChunked(
						this.getName(), "keys/", null, 1000, lastKey, true);

				obj = ck.getObjects();
				if (obj.length == 0)
					return null;
				objPos = 0;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("while doing recovery ", e);
			throw new IOException(e);
		}

		if (objPos < obj.length) {
			StorageObject sobj = null;
			try {
				sobj = s3Service.getObject(this.name, obj[objPos].getKey());

				int cl = (int) sobj.getContentLength();

				byte[] data = new byte[cl];
				try {
					DataInputStream in = new DataInputStream(
							sobj.getDataInputStream());
					in.readFully(data);
				} catch (Exception e) {
					throw new IOException(e);
				} finally {
					sobj.closeDataInputStream();
				}
				lastKey = sobj.getKey();
				boolean encrypt = false;
				boolean compress = false;
				boolean lz4compress = false;
				int size = Integer.parseInt((String) sobj.getMetadata("size"));
				if (sobj.containsMetadata("encrypt")) {
					encrypt = Boolean.parseBoolean((String) sobj
							.getMetadata("encrypt"));
				}
				if (sobj.containsMetadata("compress")) {
					compress = Boolean.parseBoolean((String) sobj
							.getMetadata("compress"));
				} else if (sobj.containsMetadata("lz4compress")) {

					lz4compress = Boolean.parseBoolean((String) sobj
							.getMetadata("lz4compress"));
				}
				boolean changed = false;
				if (sobj.containsMetadata("deleted")) {
					sobj.removeMetadata("deleted");
					changed = true;
				}
				if (sobj.containsMetadata("deleted-objects")) {
					sobj.removeMetadata("deleted-objects");
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

				this.hid = this.decHashArchiveName(sobj.getKey().substring(5),
						encrypt);

				String hast = new String(data);
				SDFSLogger.getLog().debug(
						"reading hashes "
								+ (String) sobj.getMetadata("objects")
								+ " from " + hid);
				ht = new StringTokenizer(hast, ",");
				ChunkData chk = new ChunkData(BaseEncoding.base64().decode(
						ht.nextToken().split(":")[0]), hid);

				if (sobj.containsMetadata("bsize")) {
					HashBlobArchive.currentLength.addAndGet(Integer.parseInt((String) sobj
							.getMetadata("bsize")));
				}
				if (sobj.containsMetadata("bcompressedsize")) {
					HashBlobArchive.compressedLength.addAndGet(Integer
							.parseInt((String) sobj
									.getMetadata("bcompressedsize")));
				}
				if (changed) {
					try {
						this.s3Service.updateObjectMetadata(this.name, sobj);
						String nm = "blocks/" + sobj.getKey().substring(5);
						StorageObject bo = this.s3Service.getObjectDetails(
								this.name, nm);
						bo.removeMetadata("deleted");
						bo.removeMetadata("deleted-objects");
						this.s3Service.updateObjectMetadata(this.name, bo);
						bo.closeDataInputStream();
					} catch (Exception e) {
						throw new IOException(e);
					}
				}
				objPos++;
				return chk;
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					sobj.closeDataInputStream();
				} catch (Exception e) {
				}
			}

		}
		return null;
	}

	private String[] getStrings(StorageObject sobj) throws IOException {
		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;
		int size = Integer.parseInt((String) sobj.getMetadata("size"));
		if (sobj.containsMetadata("encrypt")) {
			encrypt = Boolean
					.parseBoolean((String) sobj.getMetadata("encrypt"));
		}
		if (sobj.containsMetadata("compress")) {
			compress = Boolean.parseBoolean((String) sobj
					.getMetadata("compress"));
		} else if (sobj.containsMetadata("lz4compress")) {

			lz4compress = Boolean.parseBoolean((String) sobj
					.getMetadata("lz4compress"));
		}
		int cl = (int) sobj.getContentLength();

		byte[] data = new byte[cl];
		try {
			DataInputStream in = new DataInputStream(sobj.getDataInputStream());
			in.readFully(data);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			sobj.closeDataInputStream();
		}
		if (encrypt) {
			data = EncryptUtils.decryptCBC(data);
		}
		if (compress)
			data = CompressionUtils.decompressZLIB(data);
		else if (lz4compress) {
			data = CompressionUtils.decompressLz4(data, size);
		}
		String hast = new String(data);
		SDFSLogger.getLog().debug(
				"reading hashes " + (String) sobj.getMetadata("hashes")
						+ " from " + hid);
		String[] st = hast.split(",");
		return st;
	}

	private int getClaimedObjects(StorageObject sobj) throws ServiceException,
			IOException {
		boolean encrypt = false;

		if (sobj.containsMetadata("encrypt")) {
			encrypt = Boolean
					.parseBoolean((String) sobj.getMetadata("encrypt"));
		} else {
			sobj = s3Service.getObjectDetails(this.name, sobj.getName());
			encrypt = Boolean
					.parseBoolean((String) sobj.getMetadata("encrypt"));
		}
		long id = this.decHashArchiveName(sobj.getKey().substring(5), encrypt);
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

	StorageObject[] obj = null;
	int objPos = 0;
	String lastKey = null;

	@Override
	public void iterationInit(boolean deep) {

		try {

			StorageObjectsChunk ck = s3Service.listObjectsChunked(
					this.getName(), "keys/", null, 1000, null);
			HashBlobArchive.compressedLength.set(0);
			HashBlobArchive.currentLength.set(0);
			obj = ck.getObjects();
			this.lastKey = null;
			this.hid = 0;
			this.ht = null;
			objPos = 0;
		} catch (ServiceException e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}

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
		return HashBlobArchive.compressedLength.get();
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(long id) throws IOException {
		String haName = this.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		try {
			return s3Service.isObjectInBucket(this.name, "blocks/" + haName);
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to check fileExists on " + id, e);
			throw new IOException(e);
		}
	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc,long id) throws IOException {
		String haName = this.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);

		try {

			byte[] chunks = arc.getBytes();
			int csz = chunks.length;
			GSObject s3Object = new GSObject("blocks/" + haName);
			s3Object.addMetadata("size", Integer.toString(arc.uncompressedLength.get()));
			s3Object.addMetadata("lz4compress", Boolean.toString(Main.compress));
			s3Object.addMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			s3Object.addMetadata("compressedsize",
					Integer.toString(chunks.length));
			s3Object.addMetadata("bsize", Integer.toString(arc.getLen()));
			s3Object.addMetadata("objects", Integer.toString(arc.getSz()));

			s3Object.setContentType("binary/octet-stream");
			ByteArrayInputStream s3IS = new ByteArrayInputStream(chunks);
			s3Object.setDataInputStream(s3IS);
			s3Object.setContentType("binary/octet-stream");
			s3Object.setContentLength(s3IS.available());
			byte[] md5Hash = ServiceUtils.computeMD5Hash(chunks);
			s3Object.setMd5Hash(md5Hash);
			try {
				s3Service.putObject(this.name, s3Object);
			} finally {
				s3IS.close();
				s3Object.closeDataInputStream();
			}
			byte[] hs = arc.getHashesString().getBytes();
			int sz = hs.length;
			if (Main.compress) {
				hs = CompressionUtils.compressLz4(hs);
			}
			if (Main.chunkStoreEncryptionEnabled) {
				hs = EncryptUtils.encryptCBC(hs);
			}
			s3Object = new GSObject("keys/" + haName);
			s3Object.addMetadata("size", Integer.toString(sz));
			s3Object.addMetadata("lz4compress", Boolean.toString(Main.compress));
			s3Object.addMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			s3Object.addMetadata("compressedsize",
					Integer.toString(chunks.length));
			s3Object.addMetadata("bcompressedsize", Integer.toString(csz));
			s3Object.addMetadata("bsize", Integer.toString(arc.getLen()));
			s3Object.addMetadata("objects", Integer.toString(arc.getSz()));
			s3Object.setContentType("binary/octet-stream");
			s3IS = new ByteArrayInputStream(hs);
			s3Object.setDataInputStream(s3IS);
			s3Object.setContentType("binary/octet-stream");
			md5Hash = ServiceUtils.computeMD5Hash(hs);
			s3Object.setMd5Hash(md5Hash);
			s3Object.setContentLength(s3IS.available());
			try {
				s3Service.putObject(this.name, s3Object);
			} finally {
				s3IS.close();
				s3Object.closeDataInputStream();
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to upload " + arc.getID()+ " with id " +id, e);
			throw new IOException(e);
		} finally {

		}

	}

	private byte[] getData(long id) throws IOException, ServiceException {
		// SDFSLogger.getLog().info("Current readers :" + rr.incrementAndGet());
		String haName = this.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		StorageObject obj = s3Service.getObject(this.name, "blocks/" + haName);

		boolean encrypt = false;
		boolean compress = false;
		boolean lz4compress = false;

		int cl = (int) obj.getContentLength();
		byte[] data = new byte[cl];
		DataInputStream in = new DataInputStream(obj.getDataInputStream());
		in.readFully(data);
		obj.closeDataInputStream();
		try {
			byte[] md5Hash = ServiceUtils.computeMD5Hash(data);
			byte[] lh = ServiceUtils.fromHex(obj.getMd5HashAsHex());
			if (!Arrays.areEqual(md5Hash, lh))
				throw new IOException("download corrupted in transit");
		} catch (NoSuchAlgorithmException e) {

		}
		int size = Integer.parseInt((String) obj.getMetadata("size"));
		if (obj.containsMetadata("encrypt")) {
			encrypt = Boolean.parseBoolean((String) obj.getMetadata("encrypt"));
		}
		if (obj.containsMetadata("compress")) {
			compress = Boolean.parseBoolean((String) obj
					.getMetadata("compress"));
		} else if (obj.containsMetadata("lz4compress")) {
			lz4compress = Boolean.parseBoolean((String) obj
					.getMetadata("lz4compress"));
		}

		if (encrypt)
			data = EncryptUtils.decryptCBC(data);
		if (compress)
			data = CompressionUtils.decompressZLIB(data);
		else if (lz4compress) {
			data = CompressionUtils.decompressLz4(data, size);
		}
		if (obj.containsMetadata("deleted")) {
			boolean del = Boolean.parseBoolean((String) obj
					.getMetadata("deleted"));
			if (del) {
				StorageObject kobj = s3Service.getObjectDetails(this.name,
						"keys/" + haName);
				int claims = this.getClaimedObjects(kobj);

				int delobj = 0;
				if (obj.containsMetadata("deleted-objects")) {
					delobj = Integer.parseInt((String) obj
							.getMetadata("deleted-objects")) - claims;
					if (delobj < 0)
						delobj = 0;
				}
				obj.removeMetadata("deleted");
				obj.removeMetadata("deleted-objects");
				obj.addMetadata("deleted-objects", Integer.toString(delobj));
				obj.removeMetadata("suspect");
				obj.addMetadata("suspect", "true");
				s3Service.updateObjectMetadata(this.name, obj);
				obj.closeDataInputStream();
				int _size = Integer.parseInt((String) obj.getMetadata("size"));
				int _compressedSize = Integer.parseInt((String) obj
						.getMetadata("compressedsize"));
				HashBlobArchive.currentLength.addAndGet(_size);
				HashBlobArchive.compressedLength.addAndGet(_compressedSize);
				kobj.removeMetadata("deleted");
				kobj.removeMetadata("deleted-objects");
				kobj.addMetadata("deleted-objects", Integer.toString(delobj));
				kobj.removeMetadata("suspect");
				kobj.addMetadata("suspect", "true");
				s3Service.updateObjectMetadata(this.name, kobj);
				kobj.closeDataInputStream();
				SDFSLogger.getLog()
						.warn("Reclaimed [" + claims
								+ "] blocks marked for deletion");
			}
		}
		return data;
	}

	@Override
	public byte[] getBytes(long id) throws IOException {

		try {
			return this.getData(id);
		} catch (Exception e) {
			try {
				return this.getData(id);
			} catch (Exception e1) {
				SDFSLogger.getLog().error("unable to fetch id [" + id + "]", e);
				throw new IOException("unable to read " + id);
			}
		}

	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(1000);
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
					long pv = -1;
					for (Long k : iter) {

						pv = k.longValue();
						String hashString = this
								.encHashArchiveName(k.longValue(),
										Main.chunkStoreEncryptionEnabled);
						StorageObject obj = null;
						try {
							obj = s3Service.getObjectDetails(this.name,
									"blocks/" + hashString);
							int objects = Integer.parseInt((String) obj
									.getMetadata("objects"));
							int delobj = 0;
							if (obj.containsMetadata("deleted-objects"))
								delobj = Integer.parseInt((String) obj
										.getMetadata("deleted-objects"));
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							delobj = delobj + odel.get(k);
							if (objects <= delobj) {

								// SDFSLogger.getLog().info("deleting " +
								// hashString);
								int size = Integer.parseInt((String) obj
										.getMetadata("size"));
								int compressedSize = Integer
										.parseInt((String) obj
												.getMetadata("compressedsize"));

								if (this.deleteUnclaimed) {
									s3Service.deleteObject(this.name, "blocks/"
											+ hashString);
									s3Service.deleteObject(this.name, "keys/"
											+ hashString);
								} else {
									obj.removeMetadata("deleted");
									obj.addMetadata("deleted", "true");
									obj.removeMetadata("deleted-objects");
									obj.addMetadata("deleted-objects",
											Integer.toString(delobj));
									s3Service.updateObjectMetadata(this.name,
											obj);
									obj = s3Service.getObjectDetails(this.name,
											"keys/" + hashString);
									obj.removeMetadata("deleted");
									obj.addMetadata("deleted", "true");
									obj.removeMetadata("deleted-objects");
									obj.addMetadata("deleted-objects",
											Integer.toString(delobj));
									s3Service.updateObjectMetadata(this.name,
											obj);
								}
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
							} else {
								// SDFSLogger.getLog().info("updating " +
								// hashString + " sz=" +objects);
								obj.removeMetadata("deleted-objects");
								obj.addMetadata("deleted-objects",
										Integer.toString(delobj));
								s3Service.updateObjectMetadata(this.name, obj);
								obj = s3Service.getObjectDetails(this.name,
										"keys/" + hashString);
								obj.addMetadata("deleted-objects",
										Integer.toString(delobj));
								s3Service.updateObjectMetadata(this.name, obj);
							}

							pv = -1;
						} catch (Exception e) {
							SDFSLogger.getLog().warn(
									"Unable to delete object " + hashString, e);
							if (pv != -1) {
								delLock.lock();
								try {
									if (this.deletes.containsKey(pv)) {
										int sz = this.deletes.get(pv)
												+ odel.get(pv);
										this.deletes.put(pv, sz);
									} else
										this.deletes.put(pv, odel.get(pv));

								} finally {
									delLock.unlock();
								}
							}
						} finally {
							try {
								obj.closeDataInputStream();
							} catch (Exception e) {
							}
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
		String pth = pp + "/"
				+ this.encString(to, Main.chunkStoreEncryptionEnabled);
		
		GSObject s3Object = new GSObject(pth);
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
				s3Object.addMetadata("encrypt",
						Boolean.toString(Main.chunkStoreEncryptionEnabled));
				s3Object.addMetadata("lastmodified", Long.toString(f.lastModified()));
				String slp = EncyptUtils.encString(
						Files.readSymbolicLink(f.toPath()).toFile().getPath(),
						Main.chunkStoreEncryptionEnabled);
				s3Object.addMetadata("symlink", slp);
				s3Object.setContentType("binary/octet-stream");
				s3Object.setContentLength(slp.getBytes().length);
				s3Service.putObject(this.name,s3Object);
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else if (isDir) {
			try {
			HashMap<String, String> metaData = FileUtils.getFileMetaData(f,
					Main.chunkStoreEncryptionEnabled);
			metaData.put("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			metaData.put("lastmodified", Long.toString(f.lastModified()));
			metaData.put("directory", "true");
			s3Object.setContentType("binary/octet-stream");
			s3Object.setContentLength(pth.getBytes().length);
			Set<String> st = metaData.keySet();
			for(String s: st) {
				s3Object.addMetadata(s, metaData.get(s));
			}
			s3Service.putObject(this.name,s3Object);
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
			
			s3Object.addMetadata("lz4compress", Boolean.toString(Main.compress));
			s3Object.addMetadata("encrypt",
					Boolean.toString(Main.chunkStoreEncryptionEnabled));
			s3Object.addMetadata("lastmodified",
					Long.toString(f.lastModified()));
			s3Object.setContentType("binary/octet-stream");
			Map<String, String> umd = FileUtils.getFileMetaData(f,
					Main.chunkStoreEncryptionEnabled);
			Set<String> st = umd.keySet();
			for(String s: st) {
				s3Object.addMetadata(s, umd.get(s));
			}
			
			in = new BufferedInputStream(new FileInputStream(p), 32768);
			try {
				byte[] md5Hash = ServiceUtils.computeMD5Hash(in);
				in.close();
				s3Object.setMd5Hash(md5Hash);
				in = new BufferedInputStream(new FileInputStream(p), 32768);
			} catch (NoSuchAlgorithmException e2) {
				SDFSLogger.getLog().error("while hashing", e2);
				throw new IOException(e2);
			}
			s3Object.setDataInputStream(in);
			s3Object.setContentType("binary/octet-stream");
			s3Object.setContentLength(p.length());
			try {
				s3Service.putObject(this.name, s3Object);
			} catch (ServiceException e1) {
				SDFSLogger.getLog().error("error uploading", e1);
				throw new IOException(e1);
			}
			
		} finally {
			try {
			s3Object.closeDataInputStream();
			}catch(Exception ze) {}
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
		String haName = this.encString(nm, Main.chunkStoreEncryptionEnabled);
		GSObject obj;
		try {

			obj = s3Service.getObject(this.name, pp + "/" + haName);
			BufferedInputStream in = new BufferedInputStream(
					obj.getDataInputStream());
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(p));
			IOUtils.copy(in, out);
			out.flush();
			out.close();
			in.close();
			FileInputStream _in = null;
			try {
				_in = new FileInputStream(p);
				byte[] md5Hash = ServiceUtils.computeMD5Hash(_in);
				byte[] lh = ServiceUtils.fromHex(obj.getMd5HashAsHex());
				if (!Arrays.areEqual(md5Hash, lh))
					throw new IOException("download corrupted in transit");
			} catch (NoSuchAlgorithmException ze) {
				throw new IOException(ze);
			} finally {
				try {
					_in.close();
				} catch (Exception ze) {
				}
			}
			boolean encrypt = false;
			boolean lz4compress = false;
			if (obj.containsMetadata("encrypt")) {
				encrypt = Boolean.parseBoolean((String) obj
						.getMetadata("encrypt"));
			}
			if (obj.containsMetadata("lz4compress")) {
				lz4compress = Boolean.parseBoolean((String) obj
						.getMetadata("lz4compress"));
			}
			if (obj.containsMetadata("symlink")) {
				if (OSValidator.isWindows())
					throw new IOException(
							"unable to restore symlinks to windows");
				else {
					String spth = EncyptUtils.decString(
							(String) obj.getMetadata("symlink"), encrypt);
					Path srcP = Paths.get(spth);
					Path dstP = Paths.get(to.getPath());
					Files.createSymbolicLink(dstP, srcP);
				}
			} else if (obj.containsMetadata("directory")) {
				to.mkdirs();
				Map<String, String> mp = new HashMap<String, String>();
				Set<String> st = obj.getUserMetadataMap().keySet();
				for (String s : st) {
					mp.put(s, (String) obj.getMetadata(s));
				}
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
				obj.closeDataInputStream();
				Map<String, String> mp = new HashMap<String, String>();
				Set<String> st = obj.getUserMetadataMap().keySet();
				for (String s : st) {
					mp.put(s, (String) obj.getMetadata(s));
				}
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
		String haName = this.encString(nm, Main.chunkStoreEncryptionEnabled);
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
		String fn = this.encString(from, Main.chunkStoreEncryptionEnabled);
		String tn = this.encString(to, Main.chunkStoreEncryptionEnabled);
		try {
			s3Service.renameObject(name, pp + "/" + fn, new GSObject(this.name,
					pp + "/" + tn));
		} catch (Exception e1) {
			throw new IOException(e1);
		}

	}

	StorageObject[] sobs;
	int sobjPos = 0;
	String lk = null;

	public void clearIter() {
		sobs = null;
		sobjPos = 0;
		lk = null;
	}

	public String getNextName(String pp) throws IOException {
		try {
			String pfx = pp + "/";
			if (sobs == null) {
				StorageObjectsChunk ck = s3Service.listObjectsChunked(
						this.getName(), pfx, null, 1000, lk, true);
				sobs = ck.getObjects();
				sobjPos = 0;
			} else if (sobjPos == sobs.length) {
				StorageObjectsChunk ck = s3Service.listObjectsChunked(
						this.getName(), pfx, null, 1000, lk, true);

				sobs = ck.getObjects();
				sobjPos = 0;
			}

			if (sobs.length == 0)
				return null;
			StorageObject sobj = null;

			sobj = s3Service
					.getObjectDetails(this.name, sobs[sobjPos].getKey());

			sobj = s3Service.getObjectDetails(this.name, sobj.getKey());
			lk = sobj.getKey();
			boolean encrypt = false;
			if (sobj.containsMetadata("encrypt")) {
				encrypt = Boolean.parseBoolean((String) sobj
						.getMetadata("encrypt"));
			}
			String fname = decString(sobj.getName().substring(pfx.length()),
					encrypt);
			sobjPos++;
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
		String haName = this.encHashArchiveName(id,
				Main.chunkStoreEncryptionEnabled);
		try {
			StorageObject kobj = s3Service.getObject(this.name, "keys/"
					+ haName);
			String[] ks = this.getStrings(kobj);
			HashMap<String, Integer> m = new HashMap<String, Integer>(ks.length);
			for (String k : ks) {
				String[] kv = k.split(":");
				try {
					m.put(kv[0], Integer.parseInt(kv[1]));
				} catch (Exception e) {
					SDFSLogger.getLog().info("corrupt key file [" + k + "]", e);
					throw e;
				}
			}
			kobj.closeDataInputStream();
			return m;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean checkAccess() {
		Exception e = null;
		for (int i = 0; i < 5; i++) {
			GSObject obj = null;
			try {
				obj = s3Service.getObject(this.name, "bucketinfo");
				obj.getMetadata("currentsize");
				return true;
			} catch (Exception _e) {
				e = _e;
				SDFSLogger.getLog().debug(
						"unable to connect to bucket try " + i + " of 3", e);
			} finally {
				try {
					if (obj != null)
						obj.closeDataInputStream();
				} catch (IOException _e) {
				}
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
		return Long.toString(id);

	}

	@Override
	public boolean blockRestored(String id) {
		return true;
	}

	@Override
	public boolean checkAccess(String username, String password,
			Properties props) throws Exception {
		GSCredentials creds = new GSCredentials(username, password);
		if (props.containsKey("default-bucket-location")) {
			bucketLocation = props.getProperty("default-bucket-location");
		}
		Jets3tProperties jProps = Jets3tProperties
				.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
		if (props.containsKey("proxy-host")) {
			jProps.setProperty("httpclient.proxy-host",
					props.getProperty("proxy-host"));
		}
		if (props.containsKey("proxy-domain")) {
			jProps.setProperty("httpclient.proxy-domain",
					props.getProperty("proxy-domain"));
		}
		if (props.containsKey("proxy-user")) {
			jProps.setProperty("httpclient.proxy-user",
					props.getProperty("proxy-user"));
		}
		if (props.containsKey("proxy-password")) {
			jProps.setProperty("httpclient.proxy-password",
					props.getProperty("proxy-password"));
		}
		if (props.containsKey("proxy-port")) {
			jProps.setProperty("httpclient.proxy-port",
					props.getProperty("proxy-port"));
		}
		new GoogleStorageService(creds).listAllBuckets();
		return true;
	}

	@Override
	public void recoverVolumeConfig(String name, File to, String parentPath,
			String accessKey, String secretKey, String bucket, Properties props)
			throws IOException {
		GSCredentials creds = new GSCredentials(accessKey, secretKey);
		if (props.containsKey("default-bucket-location")) {
			bucketLocation = props.getProperty("default-bucket-location");
		}
		Jets3tProperties jProps = Jets3tProperties
				.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
		boolean encrypt = Boolean.parseBoolean(props.getProperty("encrypt",
				"false"));
		String keyStr = null;
		String ivStr = null;
		if (encrypt) {
			keyStr = props.getProperty("key");
			ivStr = props.getProperty("iv");
		}
		if (props.containsKey("proxy-host")) {
			jProps.setProperty("httpclient.proxy-host",
					props.getProperty("proxy-host"));
		}
		if (props.containsKey("proxy-domain")) {
			jProps.setProperty("httpclient.proxy-domain",
					props.getProperty("proxy-domain"));
		}
		if (props.containsKey("proxy-user")) {
			jProps.setProperty("httpclient.proxy-user",
					props.getProperty("proxy-user"));
		}
		if (props.containsKey("proxy-password")) {
			jProps.setProperty("httpclient.proxy-password",
					props.getProperty("proxy-password"));
		}
		if (props.containsKey("proxy-port")) {
			jProps.setProperty("httpclient.proxy-port",
					props.getProperty("proxy-port"));
		}
		try {
			GoogleStorageService gs = new GoogleStorageService(creds);
			GSObject[] objs = gs.listObjects(bucket, "volume/", null);
			for (GSObject obj : objs) {
				GSObject _o = gs.getObject(bucket, obj.getName());
				int cl = (int) _o.getContentLength();
				byte[] data = new byte[cl];
				DataInputStream in = new DataInputStream(
						_o.getDataInputStream());
				in.readFully(data);
				_o.closeDataInputStream();
				String vn = _o.getName().substring("volume/".length());
				Map<String, Object> mp = _o.getUserMetadataMap();
				if (mp.containsKey("encrypt")
						&& Boolean.parseBoolean((String) mp.get("encrypt"))) {
					vn = new String(EncryptUtils.decryptCBC(BaseEncoding
							.base64Url().decode(vn), keyStr, ivStr));
				}
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
					try {
						ByteArrayInputStream zin = new ByteArrayInputStream(
								data);
						BufferedOutputStream out = new BufferedOutputStream(
								new FileOutputStream(p));
						IOUtils.copy(zin, out);
						out.flush();
						out.close();
						zin.close();
						boolean enc = false;
						boolean lz4compress = false;
						if (mp.containsKey("encrypt")
								&& Boolean.parseBoolean((String) mp
										.get("encrypt"))) {
							enc = Boolean.parseBoolean((String) mp
									.get("encrypt"));
						}
						if (mp.containsKey("lz4compress")
								&& Boolean.parseBoolean((String) mp
										.get("lz4compress"))) {
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
		Properties props = new Properties();
		props.setProperty("encrypt", "true");
		props.setProperty("key",
				"EikuwdMNcqGgzetVa+JXAq8BHYzyStSntpRsHIEh+=uFxM015A5CSrz1mhiRz=Kw");
		props.setProperty("iv", "5e9fc8188a743fd49e50913dbb332aeb");
		new BatchGSChunkStore().recoverVolumeConfig("gs", new File(
				"/tmp/test.xml"), "parentPath", "GOOGJPMZZN7SFEC2GQHV",
				"2+xPK378GQyH9G1LJZJGzTfJJMjRLy63GeRKHy6W", "nbu0", props);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringResult getStringResult(String key) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
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
