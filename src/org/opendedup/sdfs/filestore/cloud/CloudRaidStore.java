package org.opendedup.sdfs.filestore.cloud;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.spec.IvParameterSpec;

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
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import java.util.AbstractMap.SimpleImmutableEntry;

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
	private ArrayList<AbstractBatchStore> datapools = new ArrayList<AbstractBatchStore>();
	private ArrayList<AbstractBatchStore> metapools = new ArrayList<AbstractBatchStore>();
	private ArrayList<AbstractChunkStore> stores = new ArrayList<AbstractChunkStore>();
	private ArrayList<BucketStats> bucketSizes = new ArrayList<BucketStats>();
	private SortedBucketList sbl = new SortedBucketList();
	private ECIO ecio = null;
	private static CloudRaidStore cs = null;

	private static enum RAID {
		MIRROR, EC, STRIPE, CONCAT
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
			try {
			this.setBucketMetaData();
			}catch(Exception e) {
				SDFSLogger.getLog().warn("unable to set bucket metadata", e);
			}
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
		SDFSLogger.getLog().info("Accessing CloudRaid Buckets");
		try {
			this.name = "cloudraid";
			this.staged_sync_location.mkdirs();
			this.ec_stage_location.mkdirs();

			if (config.hasAttribute("block-size")) {
				int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
				HashBlobArchive.MAX_LEN = sz;
			}
			if(config.hasAttribute("backlog-size")) {
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
				String _rl = config.getAttribute("raid-level");
				
				if (_rl.equalsIgnoreCase("STRIPE"))
					rl = RAID.STRIPE;
				if (_rl.equalsIgnoreCase("MIRROR"))
					rl = RAID.MIRROR;
				if (_rl.equalsIgnoreCase("EC"))
					rl = RAID.EC;
				if (_rl.equalsIgnoreCase("CONCAT"))
					rl = RAID.CONCAT;
			}
			if (rl.equals(RAID.EC)) {
				ecn = Integer.parseInt(config.getAttribute("erasure-copies"));
			}
			SDFSLogger.getLog().info("Raid Level Set to " + rl.name());
			NodeList pls = config.getElementsByTagName("pool");
			if (pls.getLength() == 0) {
				System.err.println("No Subpools found. Exiting");
				System.exit(254);
			} else {
				for (int i = 0; i < pls.getLength(); i++) {
					Element el = (Element) pls.item(i);
					String chunkClass = el.getAttribute("chunkstore-class");
					try {
						AbstractChunkStore cl = (AbstractChunkStore) Class.forName(chunkClass).newInstance();
						AbstractBatchStore bs = (AbstractBatchStore) cl;
						bs.setStandAlone(false);
						cl.init(el);
						this.stores.add(cl);

						if (el.hasAttribute("metadata-store")) {
							boolean b = Boolean.parseBoolean(el.getAttribute("metadata-store"));
							if (b) {
								this.metapools.add(bs);
							}
						}
						if (el.hasAttribute("data-store")) {
							boolean b = Boolean.parseBoolean(el.getAttribute("data-store"));
							if (b) {
									byte pos = Byte.parseByte(el.getAttribute("raid-position"));
									long capacity = -1;
									if(el.hasAttribute("capacity")) {
										capacity = StringUtils.parseSize(el.getAttribute("capacity"));
									}
									BucketStats bStat = new BucketStats(pos,capacity,0,el.getAttribute("bucket-name"),chunkClass);
									this.datapools.add(pos, bs);
									this.bucketSizes.add(pos, bStat);
									this.sbl.add(new SimpleImmutableEntry<Byte,BucketStats>(pos, bStat));
							}
						}

					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to connect to subpool " + el.getAttribute("bucket-name") + " for " + chunkClass,
								e);
						boolean b = Boolean.parseBoolean(el.getAttribute("data-store"));
						byte pos = Byte.parseByte(el.getAttribute("raid-position"));
						if (b) {
								this.datapools.add(pos, null);
						}
						long capacity = -1;
						if(el.hasAttribute("capacity")) {
							capacity = StringUtils.parseSize(el.getAttribute("capacity"));
						}
						BucketStats bStat = new BucketStats(pos,capacity,0,el.getAttribute("bucket-name"),chunkClass);
						bStat.connected = false;
						this.bucketSizes.add(pos, bStat);
					}
				}
			}
			if (rl.equals(RAID.EC)) {
				if (this.datapools.size() < (this.datapools.size() - ecn)) {
					System.err.println("Required subpools is " + (this.datapools.size() - ecn) + " total found was"
							+ this.datapools.size());
					System.exit(253);
				} else
					ecio = new ECIO(pls.getLength() - ecn, ecn);
			}
			if (this.metapools.size() == 0) {
				System.err.println("No metadata subpools found");
				System.exit(253);
			}
			this.partSize = Long.MAX_VALUE / this.datapools.size();
			Map<String, String> md = null;
			long lastUpdated = 0;
			for (AbstractBatchStore st : this.metapools) {
				Map<String, String> _md = st.getBucketInfo();
				if (_md != null) {
					try {
						long tm = Long.parseLong(_md.get("lastupdated"));
						if (tm > lastUpdated) {
							lastUpdated = tm;
							md = _md;
						}
					} catch (Exception e) {

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
			for(int i = 0;i< this.datapools.size();i++) {
				if(md.containsKey("subbucket.size." + i)) {
					this.bucketSizes.get(i).usage.set(Long.parseLong(md.get("subbucket.size." + i)));
					SDFSLogger.getLog().info("Set bucket size for " + i + " to " + md.get("subbucket.size." + i));
				}
			}
			HashBlobArchive.setLength(sz);
			HashBlobArchive.setCompressedLength(cl);
			Thread thread = new Thread(this);
			thread.start();
			HashBlobArchive.init(this);
			HashBlobArchive.setReadSpeed(rsp, false);
			HashBlobArchive.setWriteSpeed(wsp, false);
			cs = this;
			// this.resetCurrentSize();
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}

	}

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
			if ((this.rl.equals(RAID.STRIPE) || this.rl.equals(RAID.CONCAT)) && hid >= this.datapools.size())
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
				return null;
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

	Random rnd = new Random();

	private Entry<Integer, AbstractBatchStore> getStore(long id) {
		if(this.rl.equals(RAID.CONCAT)) {
			ByteBuffer bf = ByteBuffer.allocate(8);
			bf.putLong(id);
			bf.flip();
			byte pos = bf.get();
			return new SimpleImmutableEntry<Integer, AbstractBatchStore>(Integer.valueOf(pos),
					this.datapools.get(pos));
		}
		if (this.rl.equals(RAID.STRIPE)) {
			if (id < 0) {
				id = (id + 1) * -1;
			}
			int part = Math.toIntExact(id / this.partSize);
			return new SimpleImmutableEntry<Integer, AbstractBatchStore>(part,
					this.datapools.get(part));
		} else {
			AbstractBatchStore st = null;
			int pos = 0;
			while (st == null) {
				pos = rnd.nextInt(this.datapools.size());
				st = this.datapools.get(pos);
			}
			return new SimpleImmutableEntry<Integer, AbstractBatchStore>(pos, st);
		}
	}

	@Override
	public boolean fileExists(long id) throws IOException {
		try {
			return this.getStore(id).getValue().fileExists(id);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}

	ExecutorService uploadExecutor = Executors.newWorkStealingPool();

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {

		if (this.rl.equals(RAID.MIRROR)) {
			ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
			for (AbstractBatchStore st : this.datapools) {
				if (st == null)
					throw new IOException("unable to write id=" + id + " because not all data pools are up");
				Callable<Boolean> task = () -> {
					try {
						st.writeHashBlobArchive(arc, id);
						return true;
					} catch (IOException e1) {
						SDFSLogger.getLog().warn("unable to write id=" + id, e1);
						return false;
					}
				};
				ar.add(task);
			}
			try {
				final ArrayList<Boolean> al = new ArrayList<Boolean>();
				uploadExecutor.invokeAll(ar).stream().forEach((k) -> {
					try {
						synchronized (al) {
							al.add(k.get());
						}

					} catch (InterruptedException | ExecutionException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				});
				for (Boolean bk : al) {
					if (!bk)
						throw new IOException("unable to get id=" + id);
				}
			} catch (InterruptedException e1) {
				SDFSLogger.getLog().warn("unable to get id=" + id, e1);
				throw new IOException(e1);
			}

		} else if (this.rl.equals(RAID.STRIPE) || this.rl.equals(RAID.CONCAT)) {
			Entry<Integer, AbstractBatchStore> st = this.getStore(id);
			st.getValue().writeHashBlobArchive(arc, id);
			this.bucketSizes.get(st.getKey()).usage.addAndGet(arc.getFile().length());
		} else if (this.rl.equals(RAID.EC)) {
			final HashMap<String, String> metaData = new HashMap<String, String>();
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
			for (int i = 0; i < datapools.size(); i++) {
				final int z = i;
				AbstractCloudFileSync st = (AbstractCloudFileSync) datapools.get(i);
				if (st == null) {
					throw new IOException("unable to write id=" + id + " because not all data pools are up");
				}
				Callable<Boolean> task = () -> {
					try {
						HashMap<String, String> nmet = new HashMap<String, String>(metaData);
						nmet.put("shardsize", Long.toString(fls[z].length()));
						st.uploadFile(fls[z], Long.toString(id), "blocks", nmet, true);
						this.bucketSizes.get(z).usage.addAndGet(fls[z].length());
						return true;
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to write blocks " + id, e);
						return false;
					}
				};
				ar.add(task);
			}
			final ArrayList<Boolean> al = new ArrayList<Boolean>();
			try {
				uploadExecutor.invokeAll(ar).stream().forEach((k) -> {
					try {
						synchronized (al) {
							al.add(k.get());
						}

					} catch (InterruptedException | ExecutionException e1) {
						SDFSLogger.getLog().warn("unable to write blocks " + id, e1);
					}
				});
			} catch (InterruptedException e) {
				SDFSLogger.getLog().warn("unable to write blocks " + id, e);
				throw new IOException(e);
			}
			try {
				for (Boolean bk : al) {
					if (!bk)
						throw new IOException("unable to write id=" + id);
				}
			} finally {
				for (File f : fls) {
					f.delete();
				}
			}

			byte[] hs = arc.getHashesString().getBytes();
			File f = new File(this.ec_stage_location, id + ".hashes");
			FileOutputStream out = new FileOutputStream(f, false);
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
			final HashMap<String, String> _metaData = new HashMap<String, String>();
			_metaData.put("size", Integer.toString(sz));
			_metaData.put("ivspec", BaseEncoding.base64().encode(ivb));
			_metaData.put("lastaccessed", "0");
			_metaData.put("lz4compress", "false");
			_metaData.put("encrypt", Boolean.toString(Main.chunkStoreEncryptionEnabled));
			_metaData.put("compressedsize", Long.toString(arc.getFile().length()));
			_metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			_metaData.put("bcompressedsize", Long.toString(arc.getFile().length()));
			_metaData.put("objects", Integer.toString(arc.getSz()));
			ar = new ArrayList<Callable<Boolean>>();
			for (int i = 0; i < ecn; i++) {
				final int z = i;
				AbstractCloudFileSync st = (AbstractCloudFileSync) datapools.get(i);
				Callable<Boolean> task = () -> {
					try {
						st.uploadFile(f, Long.toString(id), "keys", new HashMap<String, String>(_metaData), true);
						this.bucketSizes.get(z).usage.addAndGet(f.length());
						return true;
					} catch (IOException e) {
						SDFSLogger.getLog().warn("unable to write blocks " + id, e);
						return false;
					}
				};
				ar.add(task);
			}
			al.clear();
			try {
				uploadExecutor.invokeAll(ar).stream().forEach((k) -> {
					try {
						synchronized (al) {
							al.add(k.get());
						}

					} catch (InterruptedException | ExecutionException e1) {
						SDFSLogger.getLog().warn("unable to write blocks " + id, e1);
					}
				});
			} catch (InterruptedException e) {
				SDFSLogger.getLog().warn("unable to write blocks " + id, e);
			}
			try {
				for (Boolean bk : al) {
					if (!bk)
						throw new IOException("unable to write id=" + id);
				}
			} finally {
				f.delete();
			}

		}
	}

	@Override
	public void getBytes(long id, File f) throws IOException {
		try {
			if (rl.equals(RAID.MIRROR) || rl.equals(RAID.STRIPE) || rl.equals(RAID.CONCAT)) {
				this.getStore(id).getValue().getBytes(id, f);
			} else if (rl.equals(RAID.EC)) {
				ArrayList<Callable<File>> ar = new ArrayList<Callable<File>>();
				File[] zk = new File[this.datapools.size()];
				ArrayList<Integer> ear = new ArrayList<Integer>();
				for (int i = 0; i < (this.datapools.size() - ecn); i++) {
					final int z = i;
					AbstractBatchStore st = datapools.get(z);
					if (st == null) {
						zk[z] = null;
						SDFSLogger.getLog().warn("unable to return " + id + " for data pool " + z);
						ear.add(z);
					} else {
						Callable<File> task = () -> {
							try {
								File _f = new File(this.ec_stage_location, id + "." + z);
								SDFSLogger.getLog().info("will write to " + _f.getPath());
								st.getBytes(id, _f);
								zk[z] = _f;
								return _f;
							} catch (Exception e1) {
								zk[z] = null;
								SDFSLogger.getLog().warn("unable to return " + id, e1);
								ear.add(z);
								return null;
							}
						};
						ar.add(task);
					}
				}
				uploadExecutor.invokeAll(ar);
				if (ear.size() > 0) {
					if (ear.size() > ecn) {
						throw new IOException(
								"Unable to restore data ec level=" + ecn + " ,missing blocks=" + ear.size());
					} else {
						final ArrayList<File> fal = new ArrayList<File>();
						for (int i = this.datapools.size() - ecn; i < (this.datapools.size()); i++) {
							AbstractBatchStore st = datapools.get(i);
							final int z = i;
							Callable<File> task = () -> {
								try {
									File _f = new File(this.ec_stage_location, id + "." + z);
									st.getBytes(id, _f);
									zk[z] = _f;
									fal.add(_f);
									return _f;
								} catch (Exception e1) {
									SDFSLogger.getLog().warn("unable to return " + id, e1);
									zk[z] = null;
									return null;
								}
							};
							ar.add(task);
						}
						uploadExecutor.invokeAll(ar);
						if (fal.size() < ear.size()) {
							throw new IOException("Unable to restore data ec blocks read=" + fal.size()
									+ " ,missing blocks=" + ear.size());
						} else {
							ecio.decode(zk, f);
						}
					}
				} else {
					ecio.decode(zk, f);
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch block [" + id + "]", e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}
	}

	@Override
	public void clearCounters() {
		HashBlobArchive.setCompressedLength(0);
		HashBlobArchive.setLength(0);
	}
	
	private void setBucketMetaData() throws IOException {
		Map<String, String> md = null;
		for (AbstractBatchStore cst : this.metapools) {
			try {
				md = cst.getBucketInfo();
				break;
			} catch (Exception e) {
				SDFSLogger.getLog().error("bucket info error", e);
			}
		}
		md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
		md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
		md.put("clustered", Boolean.toString(this.clustered));
		md.put("hostname", InetAddress.getLocalHost().getHostName());
		md.put("lastupdated", Long.toString(System.currentTimeMillis()));
		md.put("bucketversion", Integer.toString(version));
		md.put("sdfsversion", Main.version);
		for (int i = 0; i < this.bucketSizes.size(); i++) {
			md.put("subbucket.size." + i, Long.toString(this.bucketSizes.get(i).usage.get()));
		}

		if (Main.volume != null) {
			md.put("port", Integer.toString(Main.sdfsCliPort));
		}
		final HashMap<String, String> _md = new HashMap<String, String>();
		_md.putAll(md);
		ArrayList<Callable<Boolean>> al = new ArrayList<Callable<Boolean>>();
		for (AbstractBatchStore st : this.metapools) {
			if (st != null) {
				Callable<Boolean> task = () -> {
					try {
						st.updateBucketInfo(_md);
						return true;
					} catch (Exception e) {
						SDFSLogger.getLog().error("error in thread", e);
						return false;
					}
				};
				al.add(task);
			}
		}
		try {
			uploadExecutor.invokeAll(al);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(60000);
				try {

					this.setBucketMetaData();

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
							DeleteObject obj = new DeleteObject(k.longValue(), this);
							executor.execute(obj);

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

	@Override
	public void sync() throws IOException {
		HashBlobArchive.sync();

	}

	@Override
	public void uploadFile(File f, String to, String pp, HashMap<String, String> md, boolean disableComp)
			throws IOException {
		ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
		for (AbstractBatchStore st : this.metapools) {
			AbstractCloudFileSync cst = (AbstractCloudFileSync) st;
			Callable<Boolean> task = () -> {
				try {
					cst.uploadFile(f, to, pp, md, disableComp);
					return true;
				} catch (Exception e) {
					SDFSLogger.getLog().error("error in uploadFile thread", e);
					return false;
				}
			};
			ar.add(task);
		}
		try {
			uploadExecutor.invokeAll(ar).stream().forEachOrdered((k) -> {
				try {
					k.get();
				} catch (Exception e1) {
					SDFSLogger.getLog().error("error in uploadFile thread", e1);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void downloadFile(String nm, File to, String pp) throws IOException {
		if (to.exists())
			throw new IOException("file " + to.getPath() + " exists");
		IOException e = null;
		for (AbstractBatchStore st : this.metapools) {
			try {
				AbstractCloudFileSync cst = (AbstractCloudFileSync) st;
				cst.downloadFile(nm, to, pp);
				return;
			} catch (IOException e1) {
				e = e1;
			}
		}
		if (e != null)
			throw e;

	}

	@Override
	public void deleteFile(String nm, String pp) throws IOException {
		while (nm.startsWith(File.separator))
			nm = nm.substring(1);
		final String _nm = nm;
		try {
			ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
			for (AbstractBatchStore st : this.metapools) {
				AbstractCloudFileSync cst = (AbstractCloudFileSync) st;
				Callable<Boolean> task = () -> {
					try {
						cst.deleteFile(_nm, pp);
						return true;
					} catch (Exception e) {
						SDFSLogger.getLog().error("error in deleteFile thread", e);
						return false;
					}
				};
				ar.add(task);
			}
			uploadExecutor.invokeAll(ar);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void renameFile(String from, String to, String pp) throws IOException {

	}

	public void clearIter() {
		AbstractCloudFileSync cst = (AbstractCloudFileSync) this.metapools.get(0);
		cst.clearIter();
	}

	public String getNextName(String pp, long id) throws IOException {
		AbstractCloudFileSync cst = (AbstractCloudFileSync) this.metapools.get(0);
		return cst.getNextName(pp, id);
	}

	public Map<String, Long> getHashMap(long id) throws IOException {
		IOException e = null;
		if (rl.equals(RAID.STRIPE) || rl.equals(RAID.CONCAT)) {
			return this.getStore(id).getValue().getHashMap(id);
		}
		for (AbstractBatchStore st : this.datapools) {
			if (st != null) {
				try {
					Map<String, Long> hm = st.getHashMap(id);
					if (hm != null) {
						return hm;
					}
				} catch (Exception e1) {
					e = new IOException(e1);
				}
			}
		}
		if (e != null)
			throw e;
		return null;
	}

	@Override
	public boolean checkAccess() {
		int alstatus = 0;
		for (int i = 0; i < datapools.size(); i++) {
			AbstractBatchStore st = this.datapools.get(i);
			if(st != null && st.checkAccess()) {
				this.bucketSizes.get(i).connected = true;
				alstatus++;
			}else {
				this.bucketSizes.get(i).connected = false;
				SDFSLogger.getLog().warn("unable to connect to bucket " + i);
			}
		}
		if(alstatus < this.datapools.size())
			return false;
		else
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
		return true;
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
		return null;
	}

	@Override
	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		return null;
	}

	@Override
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		if (rl.equals(RAID.STRIPE) || rl.equals(RAID.CONCAT)) {
			this.getStore(id).getValue().checkoutObject(id, claims);
		} else {
			ArrayList<Callable<Boolean>> al = new ArrayList<Callable<Boolean>>();
			for (AbstractBatchStore st : this.datapools) {
				Callable<Boolean> task = () -> {
					try {
						st.checkoutObject(id, claims);
						return true;
					} catch (Exception e) {
						SDFSLogger.getLog().error("error in deleteFile thread", e);
						return false;
					}
				};
				al.add(task);
			}
			try {
				final ArrayList<Boolean> ar = new ArrayList<Boolean>();
				uploadExecutor.invokeAll(al).stream().forEach((k) -> {
					synchronized (ar) {
						try {
							ar.add(k.get());
						} catch (InterruptedException | ExecutionException e) {
							ar.add(false);
						}
					}
				});
				for (Boolean b : ar) {
					if (!b) {
						throw new IOException("unable to checkout object " + id);
					}
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

	}

	@Override
	public boolean objectClaimed(String key) throws IOException {
		return true;

	}

	@Override
	public void checkoutFile(String name) throws IOException {
		ArrayList<Callable<Boolean>> al = new ArrayList<Callable<Boolean>>();
		for (AbstractBatchStore st : this.metapools) {
			AbstractCloudFileSync cst = (AbstractCloudFileSync) st;
			Callable<Boolean> task = () -> {
				try {
					cst.checkoutFile(name);
					return true;
				} catch (Exception e) {
					SDFSLogger.getLog().error("error in deleteFile thread", e);
					return false;
				}
			};
			al.add(task);
		}
		try {
			final ArrayList<Boolean> ar = new ArrayList<Boolean>();
			uploadExecutor.invokeAll(al).stream().forEach((k) -> {
				synchronized (ar) {
					try {
						ar.add(k.get());
					} catch (InterruptedException | ExecutionException e) {
						ar.add(false);
					}
				}
			});
			for (Boolean b : ar) {
				if (!b) {
					throw new IOException("unable to checkout object " + name);
				}
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}

	}

	@Override
	public boolean isCheckedOut(String name, long volumeID) throws IOException {
		ArrayList<Callable<Boolean>> al = new ArrayList<Callable<Boolean>>();
		for (AbstractBatchStore st : this.metapools) {
			AbstractCloudFileSync cst = (AbstractCloudFileSync) st;
			Callable<Boolean> task = () -> {
				try {
					return cst.isCheckedOut(name, volumeID);
				} catch (Exception e) {
					SDFSLogger.getLog().error("error in isCheckedOut thread", e);
					return false;
				}
			};
			al.add(task);
		}
		try {
			final ArrayList<Boolean> ar = new ArrayList<Boolean>();
			uploadExecutor.invokeAll(al).stream().forEach((k) -> {
				synchronized (ar) {
					try {
						ar.add(k.get());
					} catch (InterruptedException | ExecutionException e) {
						SDFSLogger.getLog().error("error in isCheckedOut thread", e);
						ar.add(false);
					}
				}
			});
			for (Boolean b : ar) {
				if (!b) {
					return false;
				}
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return true;
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
		AbstractCloudFileSync cst = (AbstractCloudFileSync) this.metapools.get(0);
		RemoteVolumeInfo[] rc = cst.getConnectedVolumes();
		SDFSLogger.getLog().info("connected volume size=" + rc.length);
		return rc;
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

	@Override
	public int verifyDelete(long id) throws IOException {
		if (rl.equals(RAID.STRIPE) || rl.equals(RAID.CONCAT)) {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);
			Entry<Integer, AbstractBatchStore> ent = this.getStore(id);
			Map<String, String> md = ent.getValue().getUserMetaData("blocks/" + haName);
			long csz = 0;
			if (md.containsKey("compressedsize")) {
				csz = Long.parseLong(md.get("compressedsize"));

			}
			int claims = ent.getValue().verifyDelete(id);
			if (claims == 0)
				this.bucketSizes.get(ent.getKey()).usage.addAndGet(-1 * csz);
			return claims;
		} else {
			String haName = EncyptUtils.encHashArchiveName(id, Main.chunkStoreEncryptionEnabled);

			int claims = 0;
			Map<String, Long> data = this.getHashMap(id);
			for (String key : data.keySet()) {
				byte[] b = BaseEncoding.base64().decode(key);
				if (HCServiceProxy.getHashesMap().mightContainKey(b, id))
					claims++;
			}
			if (claims == 0) {
				ArrayList<Callable<Boolean>> ar = new ArrayList<Callable<Boolean>>();
				for (int i = 0; i < datapools.size(); i++) {
					final int z = i;
					final int _ecn = this.ecn;

					if (datapools.get(i) == null) {
						throw new IOException("unable to write id=" + id + " because not all data pools are up");
					}
					Callable<Boolean> task = () -> {
						try {
							AbstractCloudFileSync st = (AbstractCloudFileSync) datapools.get(z);

							Map<String, String> md = datapools.get(z).getUserMetaData("blocks/" + haName);
							long csz = 0;
							if (md.containsKey("compressedsize")) {
								csz = Long.parseLong(md.get("compressedsize"));
							}
							if (md.containsKey("shardsize")) {
								csz = Long.parseLong(md.get("shardsize"));
							}

							st.deleteFile(Long.toString(id), "blocks");
							this.bucketSizes.get(z).usage.addAndGet(-1 * csz);
							if (rl.equals(RAID.MIRROR) || z < _ecn)
								st.deleteFile(Long.toString(id), "keys");
							return true;
						} catch (Exception e) {
							SDFSLogger.getLog().warn("unable to delete block " + id, e);
							return false;
						}
					};
					ar.add(task);
				}
				try {
					final ArrayList<Boolean> al = new ArrayList<Boolean>();
					uploadExecutor.invokeAll(ar).stream().forEach((k) -> {
						try {
							synchronized (al) {
								al.add(k.get());
							}

						} catch (InterruptedException | ExecutionException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					});
					for (Boolean bk : al) {
						if (!bk)
							throw new IOException("unable to delete id=" + id);
					}
				} catch (InterruptedException e1) {
					SDFSLogger.getLog().warn("unable to delete id=" + id, e1);
					throw new IOException(e1);
				}

			}
			return claims;
		}

	}

	Random rand = new Random();

	private long getLongID() throws IOException {
		byte bid = -1;
		if(rl.equals(RAID.CONCAT)) {
			for(int i = 0;i<this.sbl.size();i++) {
				if(this.sbl.getAL().get(i).getValue().connected) {
					bid = this.sbl.getAL().get(i).getKey();
					break;
				}
			}
		}
		if(bid == -1) {
			throw new IOException("no buckets available");
		}
		byte[] k = new byte[7];
		rand.nextBytes(k);
		ByteBuffer bk = ByteBuffer.allocate(8);
		bk.put(bid);
		bk.put(k);
		bk.position(0);
		return bk.getLong();
	}
	
	public static List<BucketStats> getBucketSizes() {
		return cs.bucketSizes;
	}

	@Override
	public long getNewArchiveID() throws IOException {
		
		long pid = this.getLongID();
		while (pid < 100 && this.fileExists(pid))
			pid = this.getLongID();
		return pid;
	}
	
	public static class BucketStats {
		public byte id;
		public AtomicLong capacity;
		public AtomicLong usage;
		public String bucketName;
		public String bucketClass;
		public boolean connected =true;
		
		public BucketStats(byte id,long capacity,long usage,String bucketName,String bucketClass) {
			this.id = id;
			if(capacity <0) {
				this.capacity = new AtomicLong(Long.MAX_VALUE);
			}else {
				this.capacity = new AtomicLong(capacity);
			}
			this.usage = new AtomicLong(usage);
			this.bucketName = bucketName;
			this.bucketClass = bucketClass;
		}
		
		public boolean isConnected() {
			return this.connected;
		}
		
		public long getAvail() {
			if(capacity == null) {
				return Long.MAX_VALUE - usage.get();
			}else 
				return capacity.get() - usage.get();
		}
	}

	@Override
	public long getAllObjSummary(String pp, long id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
