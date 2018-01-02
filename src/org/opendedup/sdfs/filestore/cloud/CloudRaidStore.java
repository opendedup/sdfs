package org.opendedup.sdfs.filestore.cloud;

import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

import java.io.BufferedInputStream;
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
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.spec.IvParameterSpec;
import javax.ws.rs.core.MediaType;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jets3t.service.utils.ServiceUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashExistsException;
import org.opendedup.ec.ECIO;
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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.base.Supplier;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

public class CloudRaidStore implements AbstractChunkStore, AbstractBatchStore, Runnable, AbstractCloudFileSync {

	String bucketLocation = null;
	private String name;
	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	boolean closed = false;
	boolean deleteUnclaimed = true;
	boolean clustered = true;
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	File ec_stage_location = new File(Main.chunkStore + File.separator + "ecstaged");
	private int checkInterval = 15000;
	private static final int version = 1;
	private boolean azureStore = false;
	private boolean accessStore = false;
	private boolean b2Store = false;
	private boolean atmosStore = false;
	private final static String mdExt = ".6442";
	private ArrayList<AbstractBatchStore> datapools;
	private ArrayList<AbstractBatchStore> metapools;
	private ArrayList<AbstractChunkStore> stores;
	private ECIO ecio = null;

	private static enum RAID {
		MIRROR, EC, STRIPE
	};

	private RAID rl = RAID.MIRROR;
	private int ecn = 0;
	private long partSize = Long.MAX_VALUE;

	// private String bucketLocation = null;
	static {

	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
		return false;
	}

	public CloudRaidStore() {

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

			for (AbstractChunkStore store : this.stores) {
				store.close();
			}
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

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return HashBlobArchive.getLength();
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
	private int mdVersion;

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
		SDFSLogger.getLog().info("Accessing CloudRaid Buckets for bucket " + Main.cloudBucket.toLowerCase());
		try {
			this.name = Main.cloudBucket.toLowerCase();
			this.staged_sync_location.mkdirs();
			this.ec_stage_location.mkdirs();

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
			if (config.hasAttribute("metadata-version")) {
				this.mdVersion = Integer.parseInt(config.getAttribute("metadata-version"));
				SDFSLogger.getLog().info("Set Metadata Version to " + this.mdVersion);
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
			if (config.hasAttribute("raid-level")) {
				int _rl = Integer.parseInt(config.getAttribute("raid-level"));
				if(_rl == 0)
					rl = RAID.MIRROR;
				if(_rl == 1)
					rl = RAID.MIRROR;
				if(_rl == 5)
					rl = RAID.EC;
			}
			if(rl.equals(RAID.EC)) {
				ecn = Integer.parseInt(config.getAttribute("erasure-copies"));
			}
			NodeList pls = config.getElementsByTagName("pool");
			if (pls.getLength() == 0) {
				System.err.println("No Subpools found. Exiting");
				System.exit(254);
			} else {
				for (int i = 0; i < pls.getLength(); i++) {
					Element el = (Element) pls.item(i);
					String chunkClass = el.getAttribute("chunkstore-class");

					AbstractChunkStore cl = (AbstractChunkStore) Class.forName(chunkClass).newInstance();
					cl.init(el);
					this.stores.add(cl);
					AbstractBatchStore bs = (AbstractBatchStore) cl;
					if (el.hasAttribute("metadata-store")) {
						boolean b = Boolean.parseBoolean(el.getAttribute("metadata-store"));
						if (b) {
							this.metapools.add(bs);
						}
					}
					if (el.hasAttribute("data-store")) {
						boolean b = Boolean.parseBoolean(el.getAttribute("data-store"));
						if (b) {
							if (rl.equals(RAID.EC) || rl.equals(RAID.STRIPE)) {
								int pos = Integer.parseInt(el.getAttribute("raid-position"));
								this.datapools.add(pos, bs);
							}
						}
					}
				}
			}
			if(rl.equals(RAID.EC)) {
				ecio = new ECIO(pls.getLength()-ecn,ecn);
			}
			this.partSize = Long.MAX_VALUE / this.datapools.size();
			Map<String, String> md = null;
			long lastUpdated = 0;
			for (AbstractBatchStore st : this.metapools) {
				Map<String, String> _md = st.getBucketInfo();
				if (_md != null) {
					long tm = Long.parseLong(_md.get("lastupdated"));
					if (tm > lastUpdated) {
						lastUpdated = tm;
						md = _md;
					}
				}
			}
			if (md == null) {
				md = new HashMap<String, String>();
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
				md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
				md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
				md.put("clustered", Boolean.toString(this.clustered));
				md.put("hostname", InetAddress.getLocalHost().getHostName());
				if (Main.volume != null) {
					md.put("port", Integer.toString(Main.sdfsCliPort));
				}
				md.put("bucketversion", Integer.toString(version));
				md.put("lastupdated", Long.toString(System.currentTimeMillis()));
				md.put("sdfsversion", Main.version);
				for (AbstractBatchStore st : this.metapools) {
					st.updateBucketInfo(md);
				}

			} else {
				md.put("lastupdated", Long.toString(System.currentTimeMillis()));
				md.put("sdfsversion", Main.version);
				for (AbstractBatchStore st : this.metapools) {
					st.updateBucketInfo(md);
				}
			}
			HashBlobArchive.setLength(sz);
			HashBlobArchive.setCompressedLength(cl);
			Thread thread = new Thread(this);
			thread.start();
			HashBlobArchive.init(this);
			HashBlobArchive.setReadSpeed(rsp, false);
			HashBlobArchive.setWriteSpeed(wsp, false);
			// this.resetCurrentSize();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}

	}

	@SuppressWarnings("deprecation")
	@Override

	public void iterationInit(boolean deep) throws IOException {
		for (AbstractBatchStore b : this.datapools) {
			AbstractChunkStore st = (AbstractChunkStore) b;
			st.iterationInit(deep);
		}
		ht = null;
		hid = 0;

	}

	@Override
	public long getFreeBlocks() {
		return 0;
	}

	private AbstractChunkStore ht = null;
	private int hid = 0;

	@Override
	public synchronized ChunkData getNextChunck() throws IOException {
		if (ht == null) {
			if (this.rl.equals(RAID.STRIPE) && hid >= this.datapools.size())
				return null;
			else if (this.rl.equals(RAID.MIRROR) && hid > 0) {
				return null;
			} else if (this.rl.equals(RAID.EC) && hid > 0) {
				return null;
			} else {
				AbstractBatchStore b = this.datapools.get(hid);
				ht = (AbstractChunkStore) b;
				hid++;
				return this.getNextChunck();
			}
		} else {
			ChunkData cd = ht.getNextChunck();
			if (cd == null) {
				ht = null;
			} else {
				return cd;
			}
		}

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

	Random rnd = new Random();

	private AbstractBatchStore getStore(long id) {
		if (this.rl.equals(RAID.STRIPE)) {
			if (id < 0) {
				id = (id + 1) * -1;
			}
			int part = Math.toIntExact(id / this.partSize);
			return this.datapools.get(part);
		} else {
			return this.datapools.get(rnd.nextInt(this.datapools.size()));
		}
	}

	@Override
	public boolean fileExists(long id) throws IOException {
		try {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			return this.getStore(id).fileExists(id);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}
	ExecutorService uploadExecutor = Executors.newWorkStealingPool();
	
	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		IOException e = null;

		if (this.rl.equals(RAID.MIRROR)) {

			ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
			for (AbstractBatchStore st : this.datapools) {
				Callable<Boolean> task = () -> {
					st.writeHashBlobArchive(arc, id);
					return true;
				};
				ar.add(task);
			}
			uploadExecutor.invokeAll(ar).stream()
			    .map(future -> {
			        try {
			            return future.get();
			        }
			        catch (Exception e1) {
			        	e = new IOException(e1);
			        }
			    });
			if(e != null)
				throw e;
		} else if(this.rl.equals(RAID.STRIPE)) {
			this.getStore(id).writeHashBlobArchive(arc, id);
		} else if (this.rl.equals(RAID.EC)) {
			
			HashMap<String, String> metaData = new HashMap<String, String>();
			metaData.put("size", Integer.toString(arc.uncompressedLength.get()));
			if (Main.compress) {
				metaData.put("lz4compress", "true");
			} else {
				metaData.put("lz4compress", "false");
			}
			long csz = arc.getFile().length();
			if (Main.chunkStoreEncryptionEnabled) {
				metaData.put("encrypt", "true");
			} else {
				metaData.put("encrypt", "false");
			}
			metaData.put("compressedsize", Long.toString(csz));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			metaData.put("objects", Integer.toString(arc.getSz()));
			File[] fls = ecio.encode(arc.getFile(), this.ec_stage_location);
			ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
			for (int i = 0;i< datapools.size();i++) {
				AbstractCloudFileSync st = (AbstractCloudFileSync)datapools.get(i);
				Callable<Boolean> task = () -> {
					st.uploadFile(fls[i], Long.toString(id), "blocks", new HashMap<String,String>(metaData), true);
					return true;
				};
				ar.add(task);
			}
			uploadExecutor.invokeAll(ar).stream()
			    .map(future -> {
			        try {
			            return future.get();
			        }
			        catch (Exception e1) {
			        	e = new IOException(e1);
			        }
			    });
			for(File f: fls) {
				f.delete();
			}
			if(e != null)
				throw e;
			
			byte[] hs = arc.getHashesString().getBytes();
			File f = new File(this.ec_stage_location,id + ".hashes");
			FileOutputStream out = new FileOutputStream(f,false);
			out.write(hs);
			out.flush();
			out.close();
			int sz = hs.length;
			if (Main.compress) {
				hs = CompressionUtils.compressLz4(hs);
			}
			byte[] ivb = PassPhrase.getByteIV();
			if (Main.chunkStoreEncryptionEnabled) {
				hs = EncryptUtils.encryptCBC(hs, new IvParameterSpec(ivb));
			}
			metaData = new HashMap<String,String>();
			metaData.put("size", Integer.toString(sz));
			metaData.put("ivspec", BaseEncoding.base64().encode(ivb));
			metaData.put("lastaccessed", "0");
			metaData.put("lz4compress", Boolean.toString(Main.compress));
			metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
			metaData.put("compressedsize", Long.toString(arc.getFile().length()));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			metaData.put("bcompressedsize", Long.toString(arc.getFile().length()));
			metaData.put("objects", Integer.toString(arc.getSz()));
			ar = new ArrayList<Callable<Boolean>>();
			for (int i = 0;i< ecn;i++) {
				AbstractCloudFileSync st = (AbstractCloudFileSync)datapools.get(i);
				Callable<Boolean> task = () -> {
					st.uploadFile(f, Long.toString(id), "keys", new HashMap<String,String>(metaData), true);
					return true;
				};
				ar.add(task);
			}
			uploadExecutor.invokeAll(ar).stream()
			    .map(future -> {
			        try {
			            return future.get();
			        }
			        catch (Exception e1) {
			        	e = new IOException(e1);
			        }
			    });
			f.delete();
			if(e != null)
				throw e;
		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException {
		try {
			Exception e = null;
			if(rl.equals(RAID.MIRROR) || rl.equals(RAID.STRIPE)) {
				this.getStore(id).getBytes(id,f);
			} else if(rl.equals(RAID.EC)) {
				ArrayList<Callable<File>>ar = new ArrayList<Callable<File>>();
				File [] zk = new File[this.datapools.size()];
				ArrayList<Integer> ear = new ArrayList<Integer>();
				for (int i = 0;i< (this.datapools.size() - ecn);i++) {
					AbstractBatchStore st = datapools.get(i);
					Callable<File> task = () -> {
						try {
						File _f = new File(this.ec_stage_location,id + "." + i);
						st.getBytes(id, _f);
						zk[i] = _f;
						return _f;
						}catch(Exception e1) {
							zk[i] = null;
							SDFSLogger.getLog().warn("unable to return " + id, e1);
							ear.add(i);
						}
					};
					ar.add(task);
				}
				uploadExecutor.invokeAll(ar);
				if(ear.size() >0) {
					if(ear.size() > ecn) {
						throw new IOException("Unable to restore data ec level=" + ecn +" ,missing blocks=" + ear.size());
					} else {
						
						for (int i = this.datapools.size()-ecn;i< (this.datapools.size());i++) {
							AbstractBatchStore st = datapools.get(i);
							
							Callable<File> task = () -> {
								try {
								File _f = new File(this.ec_stage_location,id + "." + i);
								st.getBytes(id, _f);
								zk[i] = _f;
								return _f;
								}catch(Exception e1) {
									zk[i] = null;
									SDFSLogger.getLog().warn("unable to return " + id, e1);
									ear.add(i);
								}
							};
							ar.add(task);
						}
						uploadExecutor.invokeAll(ar);
					}
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch block [" + id + "]", e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
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
						HashBlobArchive.addToLength(-1 * _size);
						HashBlobArchive.addToCompressedLength(-1 * _compressedSize);
						if (HashBlobArchive.getLength() < 0)
							HashBlobArchive.setLength(0);
						if (HashBlobArchive.getCompressedLength() < 0) {
							HashBlobArchive.setCompressedLength(0);
						}
						SDFSLogger.getLog().debug("Current DSE Size  size=" + HashBlobArchive.getLength()
								+ " compressed size=" + HashBlobArchive.getLength());
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
		HashBlobArchive.setCompressedLength(0);
		HashBlobArchive.setLength(0);
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
					md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
					md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
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
		CloudRaidStore st = new CloudRaidStore();
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
					HashBlobArchive.addToLength(_sz);
					HashBlobArchive.addToCompressedLength(_cl);
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
		CloudRaidStore st;

		private DeleteObject(long id, CloudRaidStore cloudRaidStore) {
			this.id = id;
			this.st = cloudRaidStore;
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

	@Override
	public void setDseSize(long sz) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCredentials(String accessKey, String secretKey) {

	}

	@Override
	public boolean isStandAlone() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setStandAlone(boolean standAlone) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setMetaStore(boolean metaStore) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isMetaStore(boolean metaStore) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> getUserMetaData(String name) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getBucketInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateBucketInfo(Map<String, String> md) throws IOException {
		// TODO Auto-generated method stub

	}

}
