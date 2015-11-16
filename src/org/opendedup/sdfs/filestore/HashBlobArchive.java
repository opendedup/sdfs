package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//import objectexplorer.MemoryMeasurer;








import org.apache.commons.io.FileUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.SimpleByteArrayLongMap;
import org.opendedup.collections.SimpleByteArrayLongMap.KeyValuePair;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.RateLimiter;

import org.opendedup.collections.HashExistsException;

public class HashBlobArchive implements Runnable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long id;
	public static int MAX_LEN = 1048576 * 20;
	public static int MAX_HM_SZ = 0;
	public static int MAX_HM_OPSZ = 0;
	// private static int LEN_VARIANCE = 1048576;
	public static int THREAD_SLEEP_TIME = 5000;
	public static int VARIANCE_THREAD_SLEEP_TIME = 2000;
	private static Random r = new Random();
	private static ConcurrentHashMap<Long, HashBlobArchive> rchunks = new ConcurrentHashMap<Long, HashBlobArchive>();
	private static Random rand = new Random();
	private static AbstractBatchStore store = null;
	private boolean writeable = false;
	private static ReentrantLock slock = new ReentrantLock(true);
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private static HashBlobArchive archive = null;
	private static File chunk_location;
	private static File staged_chunk_location;
	private static final String VERSION = "1.0";
	public static AtomicInteger ct = new AtomicInteger(0);
	private boolean cached = false;
	public static boolean allowSync = false;
	private File f = null;
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private static transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private static transient ThreadPoolExecutor executor = null;
	public static AtomicLong currentLength = new AtomicLong(0);
	public static AtomicLong compressedLength = new AtomicLong(0);
	private static long LOCAL_CACHE_SIZE = 209715200;
	public static int MAP_CACHE_SIZE = 2000000;
	public static ConnectionChecker cc = null;
	public static RateLimiter rrl = null;
	public static RateLimiter wrl = null;
	public static boolean REMOVE_FROM_CACHE = true;
	private static LoadingCache<Long, HashBlobArchive> archives = null;
	private static LoadingCache<Long, SimpleByteArrayLongMap> maps = null;
	private static LoadingCache<Long, FileChannel> openFiles = null;
	private static ConcurrentHashMap<Long, SimpleByteArrayLongMap> wMaps = new ConcurrentHashMap<Long, SimpleByteArrayLongMap>();
	private static ConcurrentHashMap<Long, FileChannel> wOpenFiles = new ConcurrentHashMap<Long, FileChannel>();
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

	public static SimpleByteArrayLongMap getMap(long id) throws ExecutionException {
		return maps.get(id);
	}
	
	public static void deleteArchive(long id) {
		HashBlobArchive har = null;
		File f = getPath(id);
		if (f.exists()) {
			har = new HashBlobArchive(f, id);
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
		slock.lock();
		try {
			store = nstore;
			chunk_location = new File(Main.chunkStore);
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			staged_chunk_location = new File(Main.chunkStore + File.separator
					+ "outgoing");
			if (!staged_chunk_location.exists()) {
				staged_chunk_location.mkdirs();
			}

			SDFSLogger
					.getLog()
					.info("############################ Initialied HashBlobArchive ##############################");
			SDFSLogger.getLog().info("Version : " + VERSION);
			SDFSLogger.getLog().info(
					"HashBlobArchive IO Threads : " + Main.dseIOThreads);
			SDFSLogger.getLog().info(
					"HashBlobArchive Max Upload Size : " + MAX_LEN);
			try {
				MAX_HM_SZ = (int) ((MAX_LEN / HashFunctionPool.getHashEngine()
						.getMinLen()) / .75f);
				MAX_HM_OPSZ = (int) ((MAX_LEN / HashFunctionPool
						.getHashEngine().getMinLen()));
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (NoSuchProviderException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			SDFSLogger.getLog().info(
					"HashBlobArchive Max Map Size : " + MAX_HM_SZ);
			SDFSLogger.getLog().info(
					"HashBlobArchive Maximum Local Cache Size : "
							+ LOCAL_CACHE_SIZE);
			SDFSLogger.getLog().info(
					"HashBlobArchive Max Thread Sleep Time : "
							+ THREAD_SLEEP_TIME);
			SDFSLogger.getLog().info(
					"HashBlobArchive Spool Directory : "
							+ chunk_location.getPath());
			executor = new ThreadPoolExecutor(Main.dseIOThreads + 1,
					Main.dseIOThreads + 1, 10, TimeUnit.SECONDS, worksQueue,
					executionHandler);

			openFiles = CacheBuilder
					.newBuilder()
					.maximumSize(20)
					.removalListener(
							new RemovalListener<Long, FileChannel>() {
								public void onRemoval(
										RemovalNotification<Long, FileChannel> removal) {
									try {
										removal.getValue().close();
									} catch (IOException e) {
										SDFSLogger.getLog().warn("unable to close filechannel");
									}
								}
							}).concurrencyLevel(Main.writeThreads)
					.expireAfterAccess(60, TimeUnit.SECONDS)
					.build(new CacheLoader<Long, FileChannel>() {
						public FileChannel load(Long hashid)
								throws IOException {
							try {

								File lf = new File(getPath(hashid).getPath());
									if (lf.exists()) {
										Path path = Paths.get(getPath(hashid).getPath());
										FileChannel fileChannel = FileChannel.open(path);
										return fileChannel;
									}
									else
										throw new Exception("unable to fine file " + lf.getPath());
							} catch (Exception e) {
								SDFSLogger.getLog().error(
										"unable to fetch hashmap [" + hashid
												+ "]", e);
								throw new IOException("unable to read "
										+ hashid);
							}
						}
					});
			maps = CacheBuilder
					.newBuilder()
					.maximumSize(2000)
					.removalListener(
							new RemovalListener<Long, SimpleByteArrayLongMap>() {
								public void onRemoval(
										RemovalNotification<Long, SimpleByteArrayLongMap> removal) {
									removal.getValue().close();
								}
							}).concurrencyLevel(Main.writeThreads)
					.expireAfterAccess(60, TimeUnit.SECONDS)
					.build(new CacheLoader<Long, SimpleByteArrayLongMap>() {
						public SimpleByteArrayLongMap load(Long hashid)
								throws IOException {
							try {
								SimpleByteArrayLongMap m = null;

								File lf = new File(getPath(hashid).getPath()
										+ ".map");
								try {
									if (lf.exists()) {
										m = new SimpleByteArrayLongMap(lf
												.getPath(), MAX_HM_SZ);
									}
								} catch (Exception e) {
									m = null;
									lf.delete();
									SDFSLogger.getLog()
											.error("unable to read "
													+ lf.getPath(), e);
								}
								if (m == null) {
									Map<String, Integer> _m = store
											.getHashMap(hashid);
									Set<String> keys = _m.keySet();
									m = new SimpleByteArrayLongMap(
											lf.getPath(), MAX_HM_SZ);
									for (String key : keys) {
										m.put(BaseEncoding.base64().decode(key),
												_m.get(key));
									}
								}
								return m;
							} catch (Exception e) {
								SDFSLogger.getLog().error(
										"unable to fetch hashmap [" + hashid
												+ "]", e);
								throw new IOException("unable to read "
										+ hashid);
							}
						}
					});
			buildCache();
			archive = new HashBlobArchive();
			SDFSLogger
					.getLog()
					.info("############################ HashBlobArchive Checking for Archives not uploaded ##############################");
			File[] farchives = staged_chunk_location.listFiles();
			int z = 0;
			int c = 0;
			for (File ar : farchives) {
				if (ar.length() == 0)
					ar.delete();
				else {
					try {
						if (!ar.getName().endsWith(".map")) {
							Long id = Long.parseLong(ar.getName());
							HashBlobArchive arc = new HashBlobArchive(ar, id);
							File lf = new File(getPath(id).getPath() + ".map");
							if (lf.exists()) {
								SimpleByteArrayLongMap m = new SimpleByteArrayLongMap(
										lf.getPath(), MAX_HM_SZ);
								wMaps.put(id, m);
							}
							arc.upload();
							z++;
						}
					} catch (Exception e) {
						c++;
						SDFSLogger.getLog().error(
								"unable to upload " + ar.getPath(), e);
					}
				}
			}

			if (z > 0 || c > 0) {
				SDFSLogger.getLog().info(
						"Uploaded " + z + " archives. Failed to upload " + c
								+ " archives");
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
		} finally {
			slock.unlock();
		}
	}

	public static void setReadSpeed(double kbps) {
		slock.lock();
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
			slock.unlock();
		}
	}

	public static void setWriteSpeed(double kbps) {
		slock.lock();
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
			slock.unlock();
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
		slock.lock();
		try {
			while (rchunks.size() > 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().error("error while closing ", e);
				}
			}
			long psz = chunk_location.getFreeSpace()
					+ FileUtils.sizeOfDirectory(chunk_location);
			SDFSLogger.getLog().info("Set Cache Size to " + sz);
			if (psz < sz) {
				throw new IOException(
						"Unable to intialize because available cache size of "
								+ psz
								+ " is less than requested local cache of "
								+ LOCAL_CACHE_SIZE);
			}
			LOCAL_CACHE_SIZE = sz;
			buildCache();
		} finally {
			slock.unlock();
		}
	}

	public static long writeBlock(byte[] hash, byte[] chunk)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {
		if (closed)
			throw new IOException("Closed");
		slock.lock();
		try {
			try {
				archive.putChunk(hash, chunk);
				return archive.id;
			} catch (HashExistsException e) {
				throw e;
			} catch (ArchiveFullException e) {
				archive = new HashBlobArchive(hash, chunk);
				return archive.id;
			} catch (NullPointerException e) {
				archive = new HashBlobArchive(hash, chunk);
				return archive.id;
			} catch (ReadOnlyArchiveException e) {
				archive = new HashBlobArchive(hash, chunk);
				return archive.id;
			}
		} finally {
			slock.unlock();
		}
	}

	public static void buildCache() throws IOException {
		archives = CacheBuilder.newBuilder().maximumWeight(LOCAL_CACHE_SIZE)
				.concurrencyLevel(Main.writeThreads)
				.weigher(new Weigher<Long, HashBlobArchive>() {
					public int weigh(Long k, HashBlobArchive g) {
						return g.getLen();
					}
				})
				.removalListener(new RemovalListener<Long, HashBlobArchive>() {
					public void onRemoval(
							RemovalNotification<Long, HashBlobArchive> removal) {
						removal.getValue().removeCache();
					}
				}).build(new CacheLoader<Long, HashBlobArchive>() {
					public HashBlobArchive load(Long hashid) throws IOException {
						try {
							HashBlobArchive har = null;
							File f = getPath(hashid);
							if (!f.exists()) {
								har = new HashBlobArchive(hashid);

							} else
								har = new HashBlobArchive(f, hashid);
							har.cached = true;
							return har;
						} catch (Exception e) {
							SDFSLogger.getLog()
									.error("unable to fetch block [" + hashid
											+ "]", e);
							throw new IOException("unable to read " + hashid);
						}
					}
				});
		if (REMOVE_FROM_CACHE) {
			SDFSLogger
					.getLog()
					.info("############################ Caching Local Files ##############################");
			traverseCache(chunk_location);
			/*
			 * for (File ar : farchives) { if (ar.isDirectory() &&
			 * !ar.getName().equalsIgnoreCase("outgoing")) { if(ar.length() ==
			 * 0) ar.delete(); else { try { Long id =
			 * Long.parseLong(ar.getName()); HashBlobArchive har = new
			 * HashBlobArchive(ar,id); SDFSLogger.getLog().info("Archive Size="
			 * + MemoryMeasurer.measureBytes(har)); archives.put(id, har); }
			 * catch (Exception e) { SDFSLogger.getLog().error(
			 * "unable to upload " + ar.getPath(), e); } } } }
			 */

			long psz = chunk_location.getFreeSpace()
					+ FileUtils.sizeOfDirectory(chunk_location);
			if (psz < LOCAL_CACHE_SIZE) {
				throw new IOException(
						"Unable to intialize because available cache size of "
								+ psz
								+ " is less than requested local cache of "
								+ LOCAL_CACHE_SIZE);
			}
			psz = staged_chunk_location.getFreeSpace()
					+ FileUtils.sizeOfDirectory(staged_chunk_location);
			long csz = ((long) MAX_LEN * (long) (Main.dseIOThreads + 5));
			if (psz < csz) {
				throw new IOException(
						"Unable to intialize because available staging size of "
								+ psz
								+ " is less than requested local cache of "
								+ csz);
			}

			SDFSLogger.getLog().info("Cached " + cAc + " archives.");
		}
	}

	static long cAc = 0;

	public static void traverseCache(File f) {
		if (f.isDirectory() && !f.getName().equalsIgnoreCase("outgoing")
				&& !f.getName().equalsIgnoreCase("syncstaged")
				&& !f.getName().equalsIgnoreCase("metadata")
				&& !f.getName().equalsIgnoreCase("blocks")
				&& !f.getName().equalsIgnoreCase("keys")
				&& !f.getName().equalsIgnoreCase("sync")) {
			File[] fs = f.listFiles();
			for (File z : fs) {
				if (z.isDirectory()) {
					traverseCache(z);
				} else {
					try {
						if (!z.getPath().endsWith(".map")
								&& !z.getPath().endsWith(".map1") && !z.getPath().endsWith(".md")) {
							Long id = Long.parseLong(z.getName());
							HashBlobArchive har = new HashBlobArchive(z, id);
							archives.put(id, har);
							cAc++;
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"unable to cache " + z.getPath(), e);
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

	public static byte[] getBlock(byte[] hash, long hbid)
			throws ExecutionException, IOException, DataArchivedException {
		HashBlobArchive archive = rchunks.get(hbid);
		if (archive == null)
			archive = archives.get(hbid);
		byte[] z = null;
		try {
			z = archive.getChunk(hash);
		} catch (Exception e) {
			archives.invalidate(hbid);
			archive = archives.get(hbid);
		}
		z = archive.getChunk(hash);
		return z;
	}

	public static void cacheArchive(byte[] hash, long hbid)
			throws ExecutionException, IOException, DataArchivedException {
		HashBlobArchive archive = rchunks.get(hbid);
		if (archive == null) {
			archive = archives.get(hbid);
			archive.cacheChunk();
		}
	}

	private HashBlobArchive() throws IOException {
		long pid = rand.nextLong();
		while (pid < 100 && store.fileExists(pid))
			pid = rand.nextLong();
		this.id = pid;
		rchunks.put(this.id, this);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"waiting to write " + id + " rchunks sz=" + rchunks.size());
		this.writeable = true;
		f = new File(staged_chunk_location, Long.toString(id));
		wMaps.put(id, new SimpleByteArrayLongMap(new File(
				staged_chunk_location, Long.toString(id) + ".map").getPath(),
				MAX_HM_SZ));
		executor.execute(this);
	}

	private HashBlobArchive(byte[] hash, byte[] chunk) throws IOException,
			ArchiveFullException, ReadOnlyArchiveException {
		long pid = rand.nextLong();
		while (pid < 100 && store.fileExists(pid))
			pid = rand.nextLong();
		this.id = pid;
		rchunks.put(this.id, this);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"waiting to write " + id + " rchunks sz=" + rchunks.size());
		this.writeable = true;
		f = new File(staged_chunk_location, Long.toString(id));
		wMaps.put(id, new SimpleByteArrayLongMap(new File(
				staged_chunk_location, Long.toString(id) + ".map").getPath(),
				MAX_HM_SZ));
		this.putChunk(hash, chunk);
		executor.execute(this);
	}

	protected static File getPath(long id) {
		String st = Long.toString(id);

		File nf = null;
		if (id > -100 && id < 100) {
			nf = new File(chunk_location.getPath() + File.separator + st);
		} else if (id <= -100) {
			String dir = st.substring(0, 4);
			nf = new File(chunk_location.getPath() + File.separator + dir
					+ File.separator + st);
		} else {
			String dir = st.substring(0, 3);
			nf = new File(chunk_location.getPath() + File.separator + dir
					+ File.separator + st);
		}
		return nf;
	}

	private HashBlobArchive(Long id) throws Exception {
		this.id = id;
		f = getPath(id);

		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"Hit Rate = " + archives.stats().hitRate());
	}

	private HashBlobArchive(File f, Long id) {
		this.id = id;
		this.f = f;
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"Hit Rate = " + archives.stats().hitRate());
	}

	public long getID() {
		return this.id;
	}

	
	private void putChunk(byte[] hash, byte[] chunk) throws IOException,
			ArchiveFullException, ReadOnlyArchiveException {
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
		Lock l = this.lock.writeLock();
		l.lock();
		FileChannel ch = wOpenFiles.get(this.id);
		if(ch == null) {
			ch=  new RandomAccessFile(f, "rw").getChannel();
			wOpenFiles.put(id, ch);
		}
		try {
			if (wMaps.get(this.id).containsKey(hash)) {
				throw new HashExistsException();
			}
			
			if (!this.writeable)
				throw new ReadOnlyArchiveException();
			if (this.f.length() >= this.blocksz
					|| wMaps.get(this.id).getCurrentSize() >= MAX_HM_OPSZ) {
				this.writeable = false;
				synchronized (this) {
					this.notifyAll();
				}
				throw new ArchiveFullException();
			}
			ByteBuffer buf = ByteBuffer.allocateDirect(4 + hash.length + 4
					+ chunk.length);
			buf.putInt(hash.length);
			buf.put(hash);
			buf.putInt(chunk.length);
			buf.put(chunk);
			this.uncompressedLength.addAndGet(al);
			HashBlobArchive.currentLength.addAndGet(al);
			HashBlobArchive.compressedLength.addAndGet(chunk.length);
			buf.position(0);
			try {
				wMaps.get(this.id)
						.put(hash, (int) f.length() + 4 + hash.length);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"error while putting chunk " + this.id, e);
				throw new IOException(e);
			}
			// SDFSLogger.getLog().info("writing at " +f.length() + " bl=" +
			// buf.remaining() + "cs=" +chunk.length );
			ch.write(buf, f.length());
		} finally {
			
			l.unlock();
		}
	}
	
	public void delete() {
		Lock l = this.lock.writeLock();
		l.lock();
		try {
			archives.invalidate(this.id);
			maps.invalidate(this.id);
			wMaps.remove(this.id);
			SDFSLogger.getLog().debug("removed " + f.getPath());
			f.delete();
			File lf = new File(f.getPath() + ".map");
			lf.delete();
			maps.invalidate(this.id);
			wMaps.remove(this.id);
		} finally {
			l.unlock();
		}
	}

	private void removeCache() {
		if (REMOVE_FROM_CACHE) {
			Lock l = this.lock.writeLock();
			l.lock();
			try {
				SDFSLogger.getLog().debug("removed " + f.getPath());
				f.delete();
				File lf = new File(f.getPath() + ".map");
				lf.delete();
				maps.invalidate(this.id);
				wMaps.remove(this.id);
				openFiles.invalidate(this.id);
			} finally {
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
						maps.get(this.id);
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

	private byte[] getChunk(byte[] hash) throws IOException,
			DataArchivedException {
		byte[] ub = null;
		Lock l = this.lock.readLock();
		l.lock();
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
			int pos = 0;
			int nlen = 0;
			try {
				FileChannel ch = null;
				if(!this.cached) {
					ch = wOpenFiles.get(this.id);
					if(ch == null) {
						Path path = Paths.get(getPath(id).getPath());
						ch= FileChannel.open(path);
						wOpenFiles.put(id, ch);
					}
				}
				else {
					ch =openFiles.get(id);
				}
				SimpleByteArrayLongMap blockMap = wMaps.get(this.id);
				if (blockMap == null)
					blockMap = maps.get(this.id);
				if (blockMap.containsKey(hash)) {
					pos = blockMap.get(hash);
					try {
						//rf.seek(pos - HashFunctionPool.hashLength);
						byte[] h = new byte[4];
						ByteBuffer hb = ByteBuffer.wrap(h);
						ch.read(hb, pos);
						hb.position(0);
						nlen =hb.getInt();
						ub = new byte[nlen];
						ch.read(ByteBuffer.wrap(ub),pos+4);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Exception,possible data corruption at " + pos
										+ " nlen " + nlen + " flen "
										+ f.length() + " file=" + f.getPath(),
								e);
						throw new IOException("Exception,possible data corruption at " + pos
								+ " nlen " + nlen + " flen "
								+ f.length() + " file=" + f.getPath());
					}
				} else {
					throw new IOException("requested block not found in "
								+ f.getPath());
				}
				/*
				byte[] b = new byte[hash.length];
				ByteBuffer buf = ByteBuffer
						.allocateDirect(4 + 4 + HashFunctionPool.hashLength);
				ch = rf.getChannel();
				while (ch.position() < ch.size()) {
					buf.position(0);
					ch.read(buf);
					buf.position(0);
					buf.getInt();
					buf.get(b);
					pos = (int) ch.position() - 4;
					blockMap.put(hash, pos);
					if (Arrays.equals(b, hash)) {
						nlen = buf.getInt();
						byte[] chk = new byte[nlen];
						ByteBuffer.wrap(chk);
						ch.read(ByteBuffer.wrap(chk));
						return chk;
					} else {
						int _nlen = buf.getInt();
						if ((ch.position() + _nlen) < ch.size())
							ch.position(ch.position() + _nlen);
					}
				}
				buf = null;
				throw new IOException("hash not found");
				*/
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to read at " + pos + " " + nlen + " flen "
								+ f.length() + " file=" + f.getPath(), e);
				throw new IOException(e);
			} finally {
				
			}
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
		SimpleByteArrayLongMap blockMap;
		try {
			blockMap = wMaps.get(this.id);
			if (blockMap == null) {
				RandomAccessFile rf = null;
				FileChannel ch = null;
				Lock l = this.lock.readLock();
				l.lock();
				try {
					blockMap = new SimpleByteArrayLongMap(new File(f.getPath()
							+ ".map").getPath(), HashBlobArchive.MAX_HM_SZ);
					rf = new RandomAccessFile(f, "rw");
					ch = rf.getChannel();
					ByteBuffer buf = ByteBuffer
							.allocate(4 + 4 + HashFunctionPool.hashLength);
					while (ch.position() < ch.size()) {
						byte[] b = new byte[HashFunctionPool.hashLength];
						buf.position(0);
						ch.read(buf);
						buf.position(0);
						buf.getInt();
						buf.get(b);
						int pos = (int) ch.position() - 4;
						blockMap.put(b, pos);
						ch.position(ch.position() + buf.getInt());
					}
					wMaps.put(this.id, blockMap);
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
			return blockMap.getCurrentSize();
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

		try {
			SimpleByteArrayLongMap blockMap = wMaps.get(this.id);

			blockMap.iterInit();
			KeyValuePair p = blockMap.next();
			StringBuffer sb = new StringBuffer();
			while (p != null) {
				sb.append(BaseEncoding.base64().encode(p.getKey()));
				sb.append(":");
				sb.append(Integer.toString(p.getValue()));
				p = blockMap.next();
				if (p != null)
					sb.append(",");
			}
			String st = sb.toString();
			sb = null;
			return st;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting size", e);
			throw new IOException(e);
		}
	}

	public boolean upload() {
		Lock l = this.lock.writeLock();
		l.lock();
		this.writeable = false;
		l.unlock();
		l = this.lock.readLock();
		try {
			if (f.exists() && f.length() > 0) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("writing " + id);
				l.lock();
				try {
					if (wrl != null) {
						int _sz = 1;
						if (f.length() > 1024)
							_sz = (int) (f.length() / 1024);
						wrl.acquire(_sz);
					}
					store.writeHashBlobArchive(this);
				} catch (Exception e) {
					SDFSLogger.getLog().error("error while writing " + this.id,
							e);
					return false;
				} finally {
					l.unlock();
				}
				l = this.lock.writeLock();
				l.lock();
				try {
					FileChannel ch = wOpenFiles.remove(this.id);
					if(ch!= null)
						ch.close();
					File nf = getPath(this.id);
					Files.move(f.toPath(), nf.toPath(),
							StandardCopyOption.REPLACE_EXISTING);
					f = nf;
					SimpleByteArrayLongMap om = wMaps.remove(this.id);
					if(om != null)
					om.close();
					
					File omf = new File(om.getPath());
					File mf = new File(getPath(this.id).getPath() + ".map");
					Files.move(omf.toPath(), mf.toPath(),
							StandardCopyOption.REPLACE_EXISTING);
				} catch (Exception e) {
					SDFSLogger.getLog().error("error while writing " + this.id,
							e);
					return false;
				} finally {
					l.unlock();
				}
				archives.put(this.id, this);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote " + id);
			} else if (f.exists()) {
				l = this.lock.writeLock();
				l.lock();
				try {
					FileChannel ch = wOpenFiles.remove(this.id);
					if(ch!= null)
						ch.close();
					File nf = getPath(this.id);
					nf.delete();
					try {
						SimpleByteArrayLongMap om = wMaps.get(this.id);
						om.close();
					} catch (Exception e) {

					}
					
					nf = new File(getPath(this.id) + ".map");
					if(nf.exists())
					nf.delete();
				} finally {
					l.unlock();
				}
			}
			wOpenFiles.remove(this.id);
			wMaps.remove(this.id);
			rchunks.remove(this.id);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
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
		while (!this.upload()) {
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
			slock.lock();
			try {
				while (rchunks.size() > 0) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						SDFSLogger.getLog().error("error while closing ", e);
					}
				}
			} finally {
				slock.unlock();
			}
		} else {
			slock.lock();
			try {
				Collection<HashBlobArchive> st = rchunks.values();
				for (HashBlobArchive ar : st) {
					try {
						ar._sync();
					} catch (SyncFailedException e) {
						SDFSLogger.getLog().warn("unable to sync", e);
					} catch (IOException e) {
						SDFSLogger.getLog().warn("unable to sync", e);
					}
				}
			} finally {
				slock.unlock();
			}

		}

	}

	public static void close() {
		closed = true;
		
		SDFSLogger.getLog().info(
				"Closing HashBlobArchive in flush=" + rchunks.size());
		while (rchunks.size() > 0) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				SDFSLogger.getLog().error("error while closing ", e);
			}
		}
		SDFSLogger.getLog().info(
				"Closed HashBlobArchive in flush=" + rchunks.size());
		cc.stop();
		maps.invalidateAll();
		openFiles.invalidateAll();
		for(FileChannel ch : wOpenFiles.values()) {
			try {
				ch.close();
			} catch (IOException e) {
				
			}
		}
		wOpenFiles.clear();
	}

}
