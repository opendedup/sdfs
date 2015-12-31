package org.opendedup.sdfs.filestore;

import java.io.File;






import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Math.toIntExact;
//import objectexplorer.MemoryMeasurer;




import org.apache.commons.io.FileUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.RateLimiter;

import org.opendedup.collections.HashExistsException;

public class HashBlobArchiveNoMap implements Runnable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int id;
	public static int MAX_LEN = 1048576 * 20;
	public static int MAX_HM_SZ = 0;
	public static int MAX_HM_OPSZ = 0;
	// private static int LEN_VARIANCE = 1048576;
	public static int THREAD_SLEEP_TIME = 5000;
	public static int VARIANCE_THREAD_SLEEP_TIME = 2000;
	private static Random r = new Random();
	private static ConcurrentHashMap<Integer, HashBlobArchiveNoMap> rchunks = new ConcurrentHashMap<Integer, HashBlobArchiveNoMap>();
	private static Random rand = new Random();
	private static AbstractBatchStore store = null;
	private boolean writeable = false;
	private static ReentrantReadWriteLock slock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock uploadlock = new ReentrantReadWriteLock();
	private static HashBlobArchiveNoMap archive = null;
	private static File chunk_location;
	private static File staged_chunk_location;
	private static final String VERSION = "1.0";
	public static AtomicInteger ct = new AtomicInteger(0);
	private boolean cached = false;
	public static boolean allowSync = false;
	private boolean compactStaged = false;
	private File f = null;
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private static transient ThreadPoolExecutor executor = null;
	public static AtomicLong currentLength = new AtomicLong(0);
	public static AtomicLong compressedLength = new AtomicLong(0);
	private static long LOCAL_CACHE_SIZE = 209715200;
	public static int MAP_CACHE_SIZE = 200;
	public static ConnectionChecker cc = null;
	public static RateLimiter rrl = null;
	public static RateLimiter wrl = null;
	public static boolean REMOVE_FROM_CACHE = true;
	private static LoadingCache<Integer, HashBlobArchiveNoMap> archives = null;
	private static LoadingCache<Integer, FileChannel> openFiles = null;
	private HashMap<String,Integer> map =new HashMap<String,Integer>(MAX_HM_SZ);
	private static ConcurrentHashMap<Integer, FileChannel> wOpenFiles = new ConcurrentHashMap<Integer, FileChannel>();
	private static boolean closed = false;
	private int blocksz = nextLen();
	public AtomicInteger uncompressedLength = new AtomicInteger(0);

	public static void setLocalCacheSize(long sz) {
		// slock.lock();
		try {
			LOCAL_CACHE_SIZE = sz;
		} finally {
			// slock.unlock();
		}
	}

	public static long getLocalCacheSize() {
		// slock.lock();
		try {
			return LOCAL_CACHE_SIZE;
		} finally {
			// slock.unlock();
		}
	}

	public static void deleteArchive(long id) {
		HashBlobArchiveNoMap har = null;
		File f = getPath(id);
		if (f.exists()) {
			har = new HashBlobArchiveNoMap(f, toIntExact(id));
			har.delete();
		}
	}

	private static long nextSleepTime() {
		int Low = THREAD_SLEEP_TIME - VARIANCE_THREAD_SLEEP_TIME;
		int High = THREAD_SLEEP_TIME + VARIANCE_THREAD_SLEEP_TIME;
		int nxt = r.nextInt(High - Low) + Low;
		return nxt;
	}

	private static int nextLen() {
		int Low = MAX_LEN - (MAX_LEN / 3);
		int High = MAX_LEN;
		int nxt = r.nextInt(High - Low) + Low;
		return nxt;
	}

	// Each archive takes about 608 bytes of ram
	// Each map takes about 260848 bytes of ram for 1854 entries

	public static void init(AbstractBatchStore nstore) throws IOException {
		Lock l = slock.writeLock();
		l.lock();
		try {
			store = nstore;
			chunk_location = new File(Main.chunkStore);
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			staged_chunk_location = new File(Main.chunkStore + File.separator + "outgoing");
			if (!staged_chunk_location.exists()) {
				staged_chunk_location.mkdirs();
			}

			SDFSLogger.getLog()
					.info("############################ Initialied HashBlobArchiveNoMap ##############################");
			SDFSLogger.getLog().info("Version : " + VERSION);
			SDFSLogger.getLog().info("HashBlobArchiveNoMap IO Threads : " + Main.dseIOThreads);
			SDFSLogger.getLog().info("HashBlobArchiveNoMap Max Upload Size : " + MAX_LEN);
			try {
				MAX_HM_SZ = (int) ((MAX_LEN / HashFunctionPool.getHashEngine().getMinLen()) *2);
				MAX_HM_OPSZ = (int) ((MAX_LEN / HashFunctionPool.getHashEngine().getMinLen()));
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (NoSuchProviderException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			SDFSLogger.getLog().info("HashBlobArchiveNoMap Max Map Size : " + MAX_HM_SZ);
			SDFSLogger.getLog().info("HashBlobArchiveNoMap Maximum Local Cache Size : " + LOCAL_CACHE_SIZE);
			SDFSLogger.getLog().info("HashBlobArchiveNoMap Max Thread Sleep Time : " + THREAD_SLEEP_TIME);
			SDFSLogger.getLog().info("HashBlobArchiveNoMap Spool Directory : " + chunk_location.getPath());
			executor = new ThreadPoolExecutor(Main.dseIOThreads + 1, Main.dseIOThreads + 1, 10, TimeUnit.SECONDS,
					worksQueue, executionHandler);

			openFiles = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE)
					.removalListener(new RemovalListener<Integer, FileChannel>() {
						public void onRemoval(RemovalNotification<Integer, FileChannel> removal) {
							try {
								removal.getValue().close();
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close filechannel", e);
							}
						}
					}).concurrencyLevel(64).expireAfterAccess(60, TimeUnit.SECONDS)
					.build(new CacheLoader<Integer, FileChannel>() {
						public FileChannel load(Integer hashid) throws IOException {
							try {

								File lf = new File(getPath(hashid).getPath());
								if (lf.exists()) {
									Path path = Paths.get(getPath(hashid).getPath());
									FileChannel fileChannel = FileChannel.open(path);
									return fileChannel;
								} else
									throw new Exception("unable to find file " + lf.getPath());
							} catch (Exception e) {
								SDFSLogger.getLog().error("unable to fetch hashmap [" + hashid + "]", e);
								throw new IOException("unable to read " + hashid);
							}
						}
					});
			
			buildCache();
			SDFSLogger.getLog().info(
					"############################ HashBlobArchiveNoMap Checking for Archives not uploaded ##############################");
			File[] farchives = staged_chunk_location.listFiles();
			int z = 0;
			int c = 0;
			for (File ar : farchives) {
				if (ar.length() == 0)
					ar.delete();
				else {
					try {
						if (!ar.getName().endsWith(".map")) {
							Integer id = Integer.parseInt(ar.getName());
							HashBlobArchiveNoMap arc = new HashBlobArchiveNoMap(ar, id);
							arc.getSz();
							arc.upload(id);
							z++;
						}
					} catch (Exception e) {
						c++;
						SDFSLogger.getLog().error("unable to upload " + ar.getPath(), e);
					}
				}
			}
			archive = new HashBlobArchiveNoMap(false);

			if (z > 0 || c > 0) {
				SDFSLogger.getLog().info("Uploaded " + z + " archives. Failed to upload " + c + " archives");
			}
			for (int i = 100; i < 1000; i++) {
				File f = new File(chunk_location, Integer.toString(i));
				if (!f.exists())
					f.mkdirs();
			}
			for (int i = -999; i < -99; i++) {
				File f = new File(chunk_location, Integer.toString(i));
				if (!f.exists())
					f.mkdirs();
			}

			cc = new ConnectionChecker(store);
			SDFSLogger.getLog().info("################################# Done Uploading Archives #################");

		} finally {
			l.unlock();
			;
		}
	}

	public static void setReadSpeed(double kbps) {
		Lock l = slock.writeLock();
		l.lock();
		try {
			while (rchunks.size() > 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().error("error while closing ", e);
				}
			}
			if (kbps == 0)
				rrl = null;
			else
				rrl = RateLimiter.create(kbps);
		} finally {
			l.unlock();
		}
	}

	public static void setWriteSpeed(double kbps) {
		Lock l = slock.writeLock();
		l.lock();
		try {
			while (rchunks.size() > 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().error("error while closing ", e);
				}
			}
			if (kbps == 0)
				wrl = null;
			else
				wrl = RateLimiter.create(kbps);
		} finally {
			l.unlock();
		}

	}

	public static double getReadSpeed() {
		if (rrl == null)
			return 0;
		else
			return rrl.getRate();
	}

	public static double getWriteSpeed() {
		if (wrl == null)
			return 0;
		else
			return wrl.getRate();
	}

	public static long getCacheSize() {
		return FileUtils.sizeOfDirectory(chunk_location);
	}

	public static void setCacheSize(long sz) throws IOException {
		Lock l = slock.writeLock();
		l.lock();
		try {
			while (rchunks.size() > 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().error("error while closing ", e);
				}
			}
			long psz = chunk_location.getFreeSpace() + FileUtils.sizeOfDirectory(chunk_location);
			SDFSLogger.getLog().info("Set Cache Size to " + sz);
			if (psz < sz) {
				throw new IOException("Unable to intialize because available cache size of " + psz
						+ " is less than requested local cache of " + LOCAL_CACHE_SIZE);
			}
			LOCAL_CACHE_SIZE = sz;
			buildCache();
		} finally {
			l.unlock();
		}
	}

	public static long writeBlock(byte[] hash, byte[] chunk)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {
		if (closed)
			throw new IOException("Closed");
		Lock l = slock.readLock();
		l.lock();
		ByteBuffer bz = ByteBuffer.allocate(8);
		try {
			try {
				
				int pos =archive.putChunk(hash, chunk);
				bz.putInt(archive.id);
				bz.putInt(pos);
				bz.position(0);
				long k = bz.getLong();
				//SDFSLogger.getLog().info("writing to id=" + archive.id + " at " +pos + " long="+k + " len=" + chunk.length + " hash=" +BaseEncoding.base64().encode(hash));
				return k;
			} catch (HashExistsException e) {
				throw e;
			} catch (ArchiveFullException |  ReadOnlyArchiveException  | NullPointerException e) {
				l.unlock();
				l = slock.writeLock();
				l.lock();
				int pos = 0;
				if (!archive.writeable) {
					archive = new HashBlobArchiveNoMap();
					pos =archive.putChunk(hash, chunk);
					executor.execute(archive);
					bz.putInt(archive.id);
					bz.putInt(pos);
					bz.position(0);
					
					
				}else {
					pos =archive.putChunk(hash, chunk);
					bz.putInt(archive.id);
					bz.putInt(pos);
					bz.position(0);
				}
				long k = bz.getLong();
				//SDFSLogger.getLog().info("writing to id=" + archive.id + " at " +pos + " long="+k);
				return k;
			}
		} finally {
			l.unlock();
		}
	}

	public static void buildCache() throws IOException {
		archives = CacheBuilder.newBuilder().maximumWeight(LOCAL_CACHE_SIZE)
				.weigher(new Weigher<Integer, HashBlobArchiveNoMap>() {
					public int weigh(Integer k, HashBlobArchiveNoMap g) {
						return g.getLen();
					}
				}).removalListener(new RemovalListener<Integer, HashBlobArchiveNoMap>() {
					public void onRemoval(RemovalNotification<Integer, HashBlobArchiveNoMap> removal) {
						removal.getValue().removeCache();
					}
				}).build(new CacheLoader<Integer, HashBlobArchiveNoMap>() {
					public HashBlobArchiveNoMap load(Integer hashid) throws IOException {
						try {
							HashBlobArchiveNoMap har = null;
							File f = getPath(hashid);
							if (!f.exists()) {
								har = new HashBlobArchiveNoMap(hashid);

							} else
								har = new HashBlobArchiveNoMap(f, hashid);
							har.cached = true;
							return har;
						} catch (Exception e) {
							SDFSLogger.getLog().error("unable to fetch block [" + hashid + "]", e);
							throw new IOException("unable to read " + hashid);
						}
					}
				});
		if (REMOVE_FROM_CACHE) {
			SDFSLogger.getLog().info("############################ Caching Local Files ##############################");
			traverseCache(chunk_location);
			/*
			 * for (File ar : farchives) { if (ar.isDirectory() &&
			 * !ar.getName().equalsIgnoreCase("outgoing")) { if(ar.length() ==
			 * 0) ar.delete(); else { try { Long id =
			 * Long.parseLong(ar.getName()); HashBlobArchiveNoMap har = new
			 * HashBlobArchiveNoMap(ar,id); SDFSLogger.getLog().info("Archive Size="
			 * + MemoryMeasurer.measureBytes(har)); archives.put(id, har); }
			 * catch (Exception e) { SDFSLogger.getLog().error(
			 * "unable to upload " + ar.getPath(), e); } } } }
			 */

			long psz = chunk_location.getFreeSpace() + FileUtils.sizeOfDirectory(chunk_location);
			if (psz < LOCAL_CACHE_SIZE) {
				throw new IOException("Unable to intialize because available cache size of " + psz
						+ " is less than requested local cache of " + LOCAL_CACHE_SIZE);
			}
			psz = staged_chunk_location.getFreeSpace() + FileUtils.sizeOfDirectory(staged_chunk_location);
			long csz = ((long) MAX_LEN * (long) (Main.dseIOThreads + 5));
			if (psz < csz) {
				throw new IOException("Unable to intialize because available staging size of " + psz
						+ " is less than requested local cache of " + csz);
			}

			SDFSLogger.getLog().info("Cached " + cAc + " archives.");
		}
	}

	static long cAc = 0;

	public static void traverseCache(File f) {
		if (f.isDirectory() && !f.getName().equalsIgnoreCase("outgoing") && !f.getName().equalsIgnoreCase("syncstaged")
				&& !f.getName().equalsIgnoreCase("metadata") && !f.getName().equalsIgnoreCase("blocks")
				&& !f.getName().equalsIgnoreCase("keys") && !f.getName().equalsIgnoreCase("sync")) {
			File[] fs = f.listFiles();
			for (File z : fs) {
				if (z.isDirectory()) {
					traverseCache(z);
				} else {
					try {
						if (!z.getPath().endsWith(".map") && !z.getPath().endsWith(".smap")
								&& !z.getPath().endsWith(".md")) {
							Integer id = Integer.parseInt(z.getName());
							HashBlobArchiveNoMap har = new HashBlobArchiveNoMap(z, id);
							archives.put(id, har);
							cAc++;
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error("unable to cache " + z.getPath(), e);
					}
				}
			}
		}
	}

	public File getFile() {
		return this.f;
	}

	public static void removeCache(long id) {
		archives.invalidate(id);
	}

	public static byte[] getBlock(byte[] hash, long nd)
			throws ExecutionException, IOException, DataArchivedException {
		ByteBuffer bf = ByteBuffer.allocate(8);
		bf.putLong(nd);
		bf.position(0);
		int hbid = bf.getInt();
		int pos = bf.getInt();
		
		HashBlobArchiveNoMap archive = rchunks.get(hbid);
		if (archive == null)
			archive = archives.get(hbid);
		byte[] z = null;
		try {
			z = archive.getChunk(hash,pos);
		} catch (Exception e) {
			archives.invalidate(hbid);
			archive = archives.get(hbid);
			z = archive.getChunk(hash,pos);
		}
		
		
		//SDFSLogger.getLog().info("reading from id=" + hbid + " at " +pos + " id="+nd + " len=" +z.length + " hash=" +BaseEncoding.base64().encode(hash));
		return z;
	}

	public static void cacheArchive(byte[] hash, long nd)
			throws ExecutionException, IOException, DataArchivedException {
		ByteBuffer bf = ByteBuffer.allocate(8);
		bf.putLong(nd);
		bf.position(0);
		int hbid = bf.getInt();
		HashBlobArchiveNoMap archive = rchunks.get(hbid);
		if (archive == null) {
			archive = archives.get(hbid);
			archive.cacheChunk();
		}
	}

	public static long compactArchive(int nd) throws ExecutionException, IOException {
		HashBlobArchiveNoMap archive = rchunks.get(nd);
		if (archive == null) {
			archive = archives.get(nd);
			return archive.compact();
		} else {
			return 0;
		}
	}

	private HashBlobArchiveNoMap(boolean compact) throws IOException {
		int pid = rand.nextInt();
		while (pid < 100 && store.fileExists(pid))
			pid = rand.nextInt();
		this.id = pid;
		rchunks.put(this.id, this);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("waiting to write " + id + " rchunks sz=" + rchunks.size());
		this.writeable = true;
		this.compactStaged = compact;
		if (!this.compactStaged)
			executor.execute(this);
		f = new File(staged_chunk_location, Long.toString(id));

	}

	private HashBlobArchiveNoMap()
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {
		int pid = rand.nextInt();
		while (pid < 100 && store.fileExists(pid)&& rchunks.containsKey(pid) ) {
			pid = rand.nextInt();
		}
		this.id = pid;
		rchunks.put(this.id, this);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("waiting to write " + id + " rchunks sz=" + rchunks.size());
		this.writeable = true;
		f = new File(staged_chunk_location, Long.toString(id));
		
	}

	protected static File getPath(long id) {
		String st = Long.toString(id);

		File nf = null;
		if (id > -100 && id < 100) {
			nf = new File(chunk_location.getPath() + File.separator + st);
		} else if (id <= -100) {
			String dir = st.substring(0, 4);
			nf = new File(chunk_location.getPath() + File.separator + dir + File.separator + st);
		} else {
			String dir = st.substring(0, 3);
			nf = new File(chunk_location.getPath() + File.separator + dir + File.separator + st);
		}
		return nf;
	}

	private HashBlobArchiveNoMap(int id) throws Exception {
		this.id = id;
		f = getPath(id);

		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Hit Rate = " + archives.stats().hitRate());
	}

	private HashBlobArchiveNoMap(File f, int id) {
		this.id = id;
		this.f = f;
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Hit Rate = " + archives.stats().hitRate());
	}

	public long getID() {
		return this.id;
	}

	AtomicLong np = new AtomicLong();

	private int putChunk(byte[] hash, byte[] chunk)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {
		Lock ul = this.uploadlock.readLock();

		if (ul.tryLock()) {
			try {
				int nz = -1;
				int al = chunk.length;
				if (Main.compress) {
					nz = chunk.length;
					chunk = CompressionUtils.compressLz4(chunk);
				}
				ByteBuffer bf = ByteBuffer.wrap(new byte[4 + chunk.length]);
				bf.putInt(nz);
				bf.put(chunk);
				if (Main.chunkStoreEncryptionEnabled) {
					chunk = EncryptUtils.encryptCBC(bf.array());
				} else {
					chunk = bf.array();
				}
				FileChannel ch = null;
				long cp = -1;
				Lock l = this.lock.writeLock();
				l.lock();
				try {
					if (!this.writeable)
						throw new ReadOnlyArchiveException();
					if (np.get() >= this.blocksz ) {
						this.writeable = false;
						synchronized (this) {
							this.notifyAll();
						}
						throw new ArchiveFullException();
					}
					ch = wOpenFiles.get(this.id);
					if (ch == null) {
						ch = new RandomAccessFile(f, "rw").getChannel();
						wOpenFiles.put(this.id, ch);
					}

					cp = np.get();
					String st = BaseEncoding.base64().encode(hash);
					try {
						if(!map.containsKey(st)){
							map.put(st, (int) cp + 4 + hash.length);
							np.set(cp + 4 + hash.length + 4 + chunk.length);
							
						}else {
							np.set(cp);
							throw new HashExistsException();
						}
					} catch(HashExistsException e) {
						throw e;
					} catch (Exception e) {
						SDFSLogger.getLog().error("error while putting chunk " + this.id, e);
						throw new IOException(e);
					}

				} finally {
					l.unlock();
				}

				ByteBuffer buf = ByteBuffer.wrap(new byte[4 + hash.length + 4 + chunk.length]);
				buf.putInt(hash.length);
				buf.put(hash);
				buf.putInt(chunk.length);
				buf.put(chunk);
				this.uncompressedLength.addAndGet(al);
				HashBlobArchiveNoMap.currentLength.addAndGet(al);
				HashBlobArchiveNoMap.compressedLength.addAndGet(chunk.length);
				buf.position(0);

				// SDFSLogger.getLog().info("writing at " +f.length() + " bl=" +
				// buf.remaining() + "cs=" +chunk.length );
				ch.write(buf, cp);
				int k = (int) (cp + 4 + hash.length);
				return k;
			} finally {
				ul.unlock();
			}
		} else {
			throw new ArchiveFullException();
		}

	}

	public void delete() {
		Lock l = this.lock.writeLock();
		l.lock();
		this.writeable = false;
		this.map = null;
		try {
			FileChannel ch = wOpenFiles.remove(this.id);
			if (ch != null) {
				try {
					ch.close();
				} catch (Exception e) {

				}
			}
			archives.invalidate(this.id);
			openFiles.invalidate(this.id);
			rchunks.remove(this.id);
			SDFSLogger.getLog().debug("removed " + f.getPath());
			f.delete();
			File lf = new File(f.getPath() + ".smap");
			lf.delete();
			rchunks.remove(this.id);
			
		} catch (Exception e) {
			SDFSLogger.getLog().error("error deleting object", e);
		} finally {
			l.unlock();
		}
	}

	private void removeCache() {
		if (REMOVE_FROM_CACHE) {
			Lock l = this.lock.writeLock();
			l.lock();
			Lock ul = this.uploadlock.writeLock();
			ul.lock();
			try {
				SDFSLogger.getLog().debug("removed " + f.getPath());
				openFiles.invalidate(this.id);
				f.delete();
				File lf = new File(f.getPath() + ".map");
				lf.delete();

			} finally {
				ul.unlock();
				l.unlock();

			}
		}
	}

	private void cacheChunk() throws IOException, DataArchivedException {
		RandomAccessFile rf = null;
		Lock l = this.lock.writeLock();

		if (l.tryLock()) {
			try {

				if (!f.exists() || f.length() == 0) {
					try {
						byte[] b = store.getBytes(this.id);

						if (rrl != null) {
							int _sz = 1;
							if (b.length > 1024)
								_sz = b.length / 1024;
							rrl.acquire(_sz);
						}
						rf = new RandomAccessFile(f, "rw");
						rf.write(b);
					} catch (DataArchivedException e) {
						throw e;
					} catch (IOException e) {
						throw e;
					} catch (Exception e) {
						throw new IOException(e);
					} finally {
						try {
							rf.close();
						} catch (Exception e) {

						}
						try {
							// ch.close();
						} catch (Exception e) {

						}

					}
				}

			} finally {
				l.unlock();
			}
		}
	}

	private byte[] getChunk(byte[] hash,int pos) throws IOException, DataArchivedException {
		byte[] ub = null;
		Lock l = this.lock.readLock();
		l.lock();
		int nlen = 0;
		try {
			if (!f.exists() || f.length() == 0) {
				l.unlock();
				l = lock.writeLock();
				l.lock();
				try {
					if (!f.exists() || f.length() == 0) {
						RandomAccessFile rf = null;
						try {
							byte[] b = store.getBytes(this.id);
							if (rrl != null) {
								int _sz = 1;
								if (b.length > 1024)
									_sz = b.length / 1024;
								rrl.acquire(_sz);
							}
							rf = new RandomAccessFile(f, "rw");
							rf.write(b);
						} catch (Exception e) {
							throw e;
						} finally {
							try {
								rf.close();
							} catch (Exception e) {

							}
							try {
								// ch.close();
							} catch (Exception e) {

							}

						}
					}
				} finally {
					l.unlock();
					l = this.lock.readLock();
					l.lock();
				}
			}

			FileChannel ch = null;
			if (!this.cached) {
				ch = wOpenFiles.get(this.id);
				if (ch == null) {
					Path path = Paths.get(getPath(id).getPath());
					ch = FileChannel.open(path);
					wOpenFiles.put(id, ch);
				}
			} else {
				ch = openFiles.get(id);
			}
				if(pos == -1)
					throw new IOException("requested block not found in " + f.getPath());
				// rf.seek(pos - HashFunctionPool.hashLength);
				byte[] h = new byte[4];
				ByteBuffer hb = ByteBuffer.wrap(h);
				try {
					ch.read(hb, pos);
				} catch (Exception e) {
					ch = openFiles.get(id);
					ch.read(hb, pos);
				}
				hb.position(0);
				nlen = hb.getInt();
				ub = new byte[nlen];
				ch.read(ByteBuffer.wrap(ub), pos + 4);
		} catch (ClosedChannelException e) {
			return getChunk(hash,pos);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to read at " + pos + " " + nlen + " flen " + f.length() + " file=" + f.getPath(), e);
			throw new IOException(e);
		} finally {

			l.unlock();
		}
		if (Main.chunkStoreEncryptionEnabled) {
			ub = EncryptUtils.decryptCBC(ub);
		}
		ByteBuffer bf = ByteBuffer.wrap(ub);
		int cpz = bf.getInt();
		byte[] cp = new byte[bf.remaining()];
		bf.get(cp);
		if (cpz > 0) {
			cp = CompressionUtils.decompressLz4(cp, cpz);
		}
		return cp;
	}

	public int getLen() {
		return (int) f.length();
	}

	public int getSz() {
		try {
			
			if (map == null) {
				RandomAccessFile rf = null;
				FileChannel ch = null;
				Lock l = this.lock.readLock();
				l.lock();
				try {
					map = new HashMap<String,Integer>();
					rf = new RandomAccessFile(f, "rw");
					ch = rf.getChannel();
					ByteBuffer buf = ByteBuffer.allocate(4 + 4 + HashFunctionPool.hashLength);
					while (ch.position() < ch.size()) {
						byte[] b = new byte[HashFunctionPool.hashLength];
						buf.position(0);
						ch.read(buf);
						buf.position(0);
						buf.getInt();
						buf.get(b);
						int pos = (int) ch.position() - 4;
						ch.position(ch.position() + buf.getInt());
						map.put(BaseEncoding.base64().encode(b), pos);
					}
				} finally {
					try {
						rf.close();
					} catch (Exception e) {
					}
					try {
						ch.close();
					} catch (Exception e) {
					}
					l.unlock();
				}
			}
			return map.size();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting size", e);
			return -1;
		}
	}

	public byte[] getBytes() throws IOException {
		RandomAccessFile rf = null;
		Lock l = this.lock.readLock();
		l.lock();
		try {
			rf = new RandomAccessFile(f, "rw");
			byte[] b = new byte[(int) f.length()];
			rf.readFully(b);
			return b;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			try {
				rf.close();
			} catch (Exception e) {
			} finally {
				l.unlock();
			}
		}
	}

	public String getHashesString() throws IOException {
		Lock l = this.lock.readLock();
		l.lock();
		try {

			
			StringBuffer sb = new StringBuffer();
			Iterator<String>keys= map.keySet().iterator();
			while(keys.hasNext()) {
				String key = keys.next();
				sb.append(key);
				sb.append(":");
				sb.append(map.get(key));
				if (keys.hasNext())
					sb.append(",");
			}
			String st = sb.toString();
			sb = null;
			return st;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting size", e);
			throw new IOException(e);
		} finally {
			l.unlock();
		}
	}

	public long compact() throws IOException {
		Lock l = this.lock.writeLock();
		try {
			l.lock();
		} finally {
			l.unlock();
		}
		HashBlobArchiveNoMap _har = null;
		long ofl = f.length();
		int blks = 0;
			_har = new HashBlobArchiveNoMap(true);

			RandomAccessFile rf = null;
			FileChannel ch = null;
			l = this.lock.readLock();
			l.lock();
			try {
				map = new HashMap<String,Integer>();
				rf = new RandomAccessFile(f, "rw");
				ch = rf.getChannel();
				ByteBuffer buf = ByteBuffer.allocate(4 + 4 + HashFunctionPool.hashLength);
				ByteBuffer bz = ByteBuffer.allocateDirect(8);
				while (ch.position() < ch.size()) {
					byte[] b = new byte[HashFunctionPool.hashLength];
					buf.position(0);
					ch.read(buf);
					buf.position(0);
					buf.getInt();
					buf.get(b);
					int pos = (int) ch.position()-4;
					long cid = HCServiceProxy.getHashesMap().get(b);
					bz.position(0);
					bz.putLong(cid);
					bz.position(0);
					if (bz.getInt() == id) {
						_har.putChunk(b, this.getChunk(b,pos));
						blks++;
					}
					ch.position(ch.position() + buf.getInt());
					
				}
			if (blks == 0) {
				_har.delete();
				
				return 0;
			} else {
				l = this.lock.readLock();
				try {
					l.lock();
					while (!_har.uploadFile(this.id)) {
						Thread.sleep(100);
					}
				} finally {
					l.unlock();
				}
				l = this.lock.writeLock();
				try {
					l.lock();
					if (_har.f.exists() && _har.f.length() > 0) {
						openFiles.invalidate(this.id);
						rchunks.remove(this.id);
						while (!_har.moveFile(this.id)) {
							Thread.sleep(100);
						}
					} else {

						_har.delete();
						return 0;
					}

				} finally {
					l.unlock();
				}

			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to compact " + id, e);
			HashBlobArchiveNoMap.compressedLength.addAndGet(-1 * _har.f.length());
			HashBlobArchiveNoMap.currentLength.addAndGet(-1 * _har.uncompressedLength.get());
			_har.delete();
			throw new IOException(e);
		} finally {
			rf.close();
		}
		HashBlobArchiveNoMap.compressedLength.addAndGet(-1 * ofl);
		return f.length() - ofl;
	}

	private boolean uploadFile(int nid) throws Exception {
		Lock l = this.lock.readLock();
		l.lock();
		try {
			if (wrl != null) {
				int _sz = 1;
				if (f.length() > 1024)
					_sz = (int) (f.length() / 1024);
				wrl.acquire(_sz);
			}
			store.writeHashBlobArchive(this, nid);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
		} finally {
			l.unlock();
		}
		return true;
	}

	private boolean moveFile(int nid) throws Exception {
		Lock l = this.lock.writeLock();
		l.lock();
		Lock ul = this.uploadlock.writeLock();
		ul.lock();
		try {
			FileChannel ch = wOpenFiles.remove(this.id);
			if (ch != null)
				ch.close();
			File nf = getPath(nid);
			Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
			f = nf;
			File omf = new File(f.toPath() + ".smap");
			if(omf.exists()) {
				File mf = new File(getPath(nid).getPath() + ".smap");
				Files.move(omf.toPath(), mf.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			this.cached = true;
			archives.put(nid, this);

			rchunks.remove(this.id);
			this.id = nid;
			map = null;
			this.writeable = false;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
		} finally {
			ul.unlock();
			l.unlock();

		}
		return true;
	}

	public boolean upload(int nid) {
		
		Lock ul = this.uploadlock.writeLock();
		ul.lock();
		Lock l = this.lock.writeLock();
		l.lock();
		this.writeable = false;
		l.unlock();
		try {
			if (f.exists() && f.length() > 0) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("writing " + id);
				if (!this.uploadFile(nid))
					return false;

				if (!this.moveFile(nid))
					return false;
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote " + id);
			} else if (f.exists()) {
				this.delete();
			} else {
				FileChannel ch = wOpenFiles.remove(this.id);
				if (ch != null) {
					try {
						ch.close();
					} catch (Exception e) {

					}
				}
				
				rchunks.remove(this.id);
			}

		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
		} finally {
			ul.unlock();
		}
		return true;
	}

	@Override
	public void run() {

		try {
			synchronized (this) {
				this.wait(nextSleepTime());
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
		}
		while (!this.upload(this.id)) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {

			}
		}
	}

	private void _sync() throws SyncFailedException, IOException {
		Lock l = this.lock.writeLock();
		l.lock();
		RandomAccessFile rf = null;
		try {
			rf = new RandomAccessFile(f, "rw");
			rf.getFD().sync();
			rf.close();
		} finally {
			try {
				if (rf != null)
					rf.close();
			} finally {
				l.unlock();
			}
		}
	}

	public static void sync() {
		if (allowSync) {
			Lock l = slock.writeLock();
			l.lock();
			try {
				while (rchunks.size() > 0) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						SDFSLogger.getLog().error("error while closing ", e);
					}
				}
			} finally {
				l.unlock();
			}
		} else {
			Lock l = slock.writeLock();
			l.lock();
			try {
				Collection<HashBlobArchiveNoMap> st = rchunks.values();
				for (HashBlobArchiveNoMap ar : st) {
					try {
						ar._sync();
					} catch (SyncFailedException e) {
						SDFSLogger.getLog().warn("unable to sync", e);
					} catch (IOException e) {
						SDFSLogger.getLog().warn("unable to sync", e);
					}
				}
			} finally {
				l.unlock();
			}

		}

	}

	public static void close() {
		closed = true;

		SDFSLogger.getLog().info("Closing HashBlobArchiveNoMap in flush=" + rchunks.size());
		while (rchunks.size() > 0) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				SDFSLogger.getLog().error("error while closing ", e);
			}
		}
		SDFSLogger.getLog().info("Closed HashBlobArchiveNoMap in flush=" + rchunks.size());
		cc.stop();
		openFiles.invalidateAll();
		for (FileChannel ch : wOpenFiles.values()) {
			try {
				ch.close();
			} catch (IOException e) {

			}
		}
		wOpenFiles.clear();
	}

}
