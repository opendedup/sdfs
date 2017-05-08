package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.cloud.MultiDownload;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

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
public class BatchFileChunkStore implements AbstractChunkStore, AbstractBatchStore, Runnable {
	private String name;
	boolean compress = false;
	boolean encrypt = false;

	private HashMap<Long, Integer> deletes = new HashMap<Long, Integer>();
	boolean closed = false;
	boolean deleteUnclaimed = true;
	File staged_sync_location = new File(Main.chunkStore + File.separator + "syncstaged");
	File container_location = new File(Main.chunkStore);
	int checkInterval = 15000;
	public boolean clustered;
	private int mdVersion = 0;

	// private String bucketLocation = null;
	static {

	}

	public static boolean checkAuth(String awsAccessKey, String awsSecretKey) {
		return false;
	}

	public static boolean checkBucketUnique(String awsAccessKey, String awsSecretKey, String bucketName) {
		return false;
	}

	public BatchFileChunkStore() {

	}

	@Override
	public long bytesRead() {
		return 0;
	}

	@Override
	public long bytesWritten() {
		return 0;
	}

	private void updateMD() {
		try {
			// container = pool.borrowObject();
			HashMap<String, String> md = new HashMap<String, String>();
			md.put("currentlength", Long.toString(HashBlobArchive.getLength()));
			md.put("compressedlength", Long.toString(HashBlobArchive.getCompressedLength()));
			FileOutputStream fout = new FileOutputStream(new File(this.container_location, "BucketInfo"));
			ObjectOutputStream oon = new ObjectOutputStream(fout);
			oon.writeObject(md);
			oon.flush();
			oon.close();
			fout.flush();
			fout.close();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error closing container", e);
		}
		try {
			// this.serviceClient.
		} catch (Exception e) {

		}
	}

	@Override
	public void close() {
		this.updateMD();
		this.closed = true;
	}

	public void expandFile(long length) throws IOException {

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException, DataArchivedException {
		byte[] b = HashBlobArchive.getBlock(hash, start);
		return b;
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
		return HashBlobArchive.getLength();
	}

	public void cacheData(long len) throws IOException, DataArchivedException {

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
	int del = 0;

	@Override
	public void deleteChunk(byte[] hash, long start, int len) throws IOException {
		delLock.lock();
		try {
			del++;
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

	public void deleteBucket() throws IOException, InterruptedException {
		try {
			DeleteDir.deleteDirectory(container_location);
		} finally {
			// pool.returnObject(container);
		}
		this.close();
	}

	@Override
	public void init(Element config) throws IOException {
		this.name = "filestore";
		this.staged_sync_location.mkdirs();
		HashBlobArchive.REMOVE_FROM_CACHE = false;
		HashBlobArchive.cacheWrites = false;
		HashBlobArchive.cacheReads = false;
		if (config.hasAttribute("default-bucket-location")) {
			// bucketLocation = config.getAttribute("default-bucket-location");
		}
		if (config.hasAttribute("connection-check-interval")) {
			this.checkInterval = Integer.parseInt(config.getAttribute("connection-check-interval"));
		}
		if (config.hasAttribute("block-size")) {
			int sz = (int) StringUtils.parseSize(config.getAttribute("block-size"));
			HashBlobArchive.MAX_LEN = sz;
		}
		if (config.hasAttribute("delete-unclaimed")) {
			this.deleteUnclaimed = Boolean.parseBoolean(config.getAttribute("delete-unclaimed"));
		}
		if (config.hasAttribute("upload-thread-sleep-time")) {
			int tm = Integer.parseInt(config.getAttribute("upload-thread-sleep-time"));
			HashBlobArchive.THREAD_SLEEP_TIME = tm;
		} else {
			HashBlobArchive.THREAD_SLEEP_TIME = 1000 * 60 * 5;
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
		if (config.hasAttribute("allow-sync")) {
			HashBlobArchive.allowSync = Boolean.parseBoolean(config.getAttribute("allow-sync"));
		}
		if (config.hasAttribute("null-test-only")) {
			HashBlobArchive.DISABLE_WRITE = Boolean.parseBoolean(config.getAttribute("null-test-only"));
		}
		if (config.hasAttribute("map-version")) {
			this.mdVersion = Integer.parseInt(config.getAttribute("map-version"));
		}

		try {
			File f = new File(this.container_location, "BucketInfo");
			if (f.exists()) {
				FileInputStream fin = new FileInputStream(f);
				ObjectInputStream oon = new ObjectInputStream(fin);
				@SuppressWarnings("unchecked")
				HashMap<String, String> md = (HashMap<String, String>) oon.readObject();
				oon.close();
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
				HashBlobArchive.setLength(sz);
				HashBlobArchive.setCompressedLength(cl);
				f.delete();
			}
			this.compress = Main.compress;
			this.encrypt = Main.chunkStoreEncryptionEnabled;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			// if (pool != null)
			// pool.returnObject(container);
		}
		Thread thread = new Thread(this);
		thread.start();
		HashBlobArchive.init(this);
	}

	// Iterator<ListBlobItem> iter = null;

	@Override
	public long getFreeBlocks() {
		return 0;
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
			ChunkData chk = new ChunkData(BaseEncoding.base64().decode(ht.nextToken().split(":")[0]), hid);
			return chk;
		}
	}

	private HashMap<String, String> readHashMap(long id) throws IOException, ClassNotFoundException {
		File _f = new File(HashBlobArchive.getPath(id).getPath() + ".md");
		if (!_f.exists()) {
			HashMap<String, String> hs = new HashMap<String, String>();
			StringTokenizer ht = new StringTokenizer(HashBlobArchive.getStrings(hid), ",");
			hs.put("objects", Integer.toString(ht.countTokens()));
			hs.put("bsize", Long.toString(new File(HashBlobArchive.getPath(id).getPath()).length()));
			this.writeHashMap(hs, id);
		}
		FileInputStream fin = new FileInputStream(_f);
		ObjectInputStream oon = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		HashMap<String, String> md = (HashMap<String, String>) oon.readObject();
		oon.close();
		return md;
	}

	private void writeHashMap(HashMap<String, String> md, long id) throws IOException {
		File _f = new File(HashBlobArchive.getPath(id).getPath() + ".md");
		FileOutputStream fout = new FileOutputStream(_f);
		ObjectOutputStream out = new ObjectOutputStream(fout);
		out.writeObject(md);
		out.flush();
		out.close();
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
		try {
			File f = HashBlobArchive.getPath(id);
			return f.exists();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get id", e);
			throw new IOException(e);
		}

	}

	@Override
	public void writeHashBlobArchive(HashBlobArchive arc, long id) throws IOException {
		try {
			HashMap<String, String> metaData = new HashMap<String, String>();
			metaData.put("objects", Integer.toString(arc.getSz()));
			metaData.put("bsize", Integer.toString(arc.uncompressedLength.get()));
			this.writeHashMap(metaData, id);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write archive " + arc.getID(), e);
			throw new IOException(e);
		} finally {
			// pool.returnObject(container);
		}

	}

	@Override
	public void getBytes(long id, File f) throws IOException {
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				Thread.sleep(5000);
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
					long sz = 0;
					for (Long k : iter) {

						try {
							File blob = HashBlobArchive.getPath(k);
							HashMap<String, String> metaData = null;
							int objs = 0;
							try {
								metaData = this.readHashMap(k);
								objs = Integer.parseInt(metaData.get("objects"));
							} catch (Exception e) {
								metaData = new HashMap<String, String>();
							}
							// SDFSLogger.getLog().info("remove requests for " +
							// hashString + "=" + odel.get(k));
							int delobj = 0;
							if (metaData.containsKey("deleted-objects"))
								delobj = Integer.parseInt((String) metaData.get("deleted-objects"));
							delobj = delobj + odel.get(k);
							SDFSLogger.getLog().debug("updating " + k + " sz=" + objs);
							metaData.put("deleted-objects", Integer.toString(delobj));
							try {
								long z = HashBlobArchive.compactArchive(k);
								sz += z;
								SDFSLogger.getLog().info("remove requests for " + k + "=" + odel.get(k) + " delob=" + delobj
										+ " bsz=" + metaData.get("bsize") + " z=" + z);
								if (blob.exists())
									this.writeHashMap(metaData, k);
								if (z > 0)
									HashBlobArchive.addToLength(-1 * Integer.parseInt(metaData.get("bsize")));

							} catch (Exception e) {

							}
						} catch (Exception e) {
							SDFSLogger.getLog().debug("Unable to delete object " + k, e);
						} finally {
							// pool.returnObject(container);
						}

					}
					SDFSLogger.getLog().info("Compacted [" + iter.size() + "] storaage objects by ["
							+ StorageUnit.of(sz).format(sz) + "]");

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
	public boolean checkAccess() {
		return true;
	}

	@Override
	public Map<String, Long> getHashMap(long id) throws IOException {
		throw new IOException("not supported");
	}

	public int getMetaDataVersion() {
		return this.mdVersion;
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

	public ArrayList<String> traverseCache(File f) {
		ArrayList<String> maps = new ArrayList<String>();
		if (f.isDirectory() && !f.getName().equalsIgnoreCase("outgoing") && !f.getName().equalsIgnoreCase("syncstaged")
				&& !f.getName().equalsIgnoreCase("metadata") && !f.getName().equalsIgnoreCase("blocks")
				&& !f.getName().equalsIgnoreCase("keys") && !f.getName().equalsIgnoreCase("sync")) {
			File[] fs = f.listFiles();

			for (File z : fs) {
				if (z.isDirectory()) {
					maps.addAll(traverseCache(z));
				} else {
					try {
						if (!z.getPath().endsWith(".smap") && !z.getPath().endsWith(".md")
								&& !z.getPath().endsWith(".map")) {
							maps.add(z.getName());
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error("unable to cache " + z.getPath(), e);
					}
				}
			}
		}
		return maps;
	}

	MultiDownload dl = null;
	StringTokenizer ht = null;
	long hid;
	Iterator<String> maps = null;

	@Override
	public void iterationInit(boolean deep) {
		try {
			HashBlobArchive.setLength(0);
			HashBlobArchive.setCompressedLength(0);
			this.ht = null;
			this.hid = 0;
			dl = new MultiDownload(this, "");
			maps = this.traverseCache(new File(Main.chunkStore)).iterator();
			dl.iterationInit(false, "");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}
	}

	boolean fr = true;

	@Override
	public Iterator<String> getNextObjectList(String prefix) throws IOException {
		return maps;

	}

	@Override
	public StringResult getStringResult(String key) throws IOException, InterruptedException {
		Long _hid = Long.parseLong(key);
		File mf = HashBlobArchive.getPath(_hid);
		HashBlobArchive.addToCompressedLength(mf.length());
		try {
			StringTokenizer dht = new StringTokenizer(HashBlobArchive.getStrings(_hid), ",");
			StringResult st = new StringResult();
			st.id = _hid;
			st.st = dht;
			return st;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get strings for " + _hid, e);
			throw e;
		}
	}

	@Override
	public boolean isLocalData() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void checkoutObject(long id, int claims) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean objectClaimed(String key) {
		// TODO Auto-generated method stub
		return true;
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
	public byte[] getBytes(long id, int from, int to) throws IOException, DataArchivedException {
		throw new IOException("funtion not supported");
	}

	@Override
	public void clearCounters() {
		HashBlobArchive.setLength(0);
		HashBlobArchive.setCompressedLength(0);
	}

	@Override
	public void timeStampData(long key) throws IOException {
		// TODO Auto-generated method stub

	}

}