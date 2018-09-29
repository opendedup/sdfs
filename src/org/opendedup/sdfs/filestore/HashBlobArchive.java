package org.opendedup.sdfs.filestore;

import static java.lang.Math.toIntExact;

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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.crypto.spec.IvParameterSpec;

//import objectexplorer.MemoryMeasurer;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashExistsException;
import org.opendedup.collections.MapClosedException;
import org.opendedup.collections.SimpleByteArrayLongMap;
import org.opendedup.collections.SimpleByteArrayLongMap.KeyValuePair;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.events.ArchiveSync;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.events.HashBlobArchiveUploaded;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.eventbus.EventBus;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.RateLimiter;

public class HashBlobArchive implements Runnable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long id;
	public static int MAX_LEN = 1048576 * 20;
	public static int MAX_HM_SZ = 0;
	public static int MAX_HM_OPSZ = 0;
	private static double LEN_VARIANCE = .25;
	public static int THREAD_SLEEP_TIME = 5000;
	public static int VARIANCE_THREAD_SLEEP_TIME = 2000;

	IvParameterSpec ivspec = new IvParameterSpec(EncryptUtils.iv);
	private static Random r = new Random();
	private static ConcurrentHashMap<Long, HashBlobArchive> rchunks = new ConcurrentHashMap<Long, HashBlobArchive>();
	private static AbstractBatchStore store = null;
	private boolean writeable = false;
	private static ReentrantReadWriteLock slock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock uploadlock = new ReentrantReadWriteLock();
	private static final ConcurrentHashMap<String, HashBlobArchive> writableArchives = new ConcurrentHashMap<String, HashBlobArchive>();
	private static File chunk_location;
	private static File staged_chunk_location;
	private static int VERSION = 0;
	private boolean cached = false;
	public static boolean allowSync = false;
	private boolean compactStaged = false;
	public static boolean cacheWrites = true;
	public static boolean cacheReads = true;
	public static int offset = 0;
	private File f = null;
	private static EventBus eventUploadBus = new EventBus();
	// private static transient RejectedExecutionHandler executionHandler = new
	// BlockPolicy();
	private static transient BlockingQueue<Runnable> worksQueue = null;
	private static transient ThreadPoolExecutor executor = null;
	private static AtomicLong currentLength = new AtomicLong(0);
	private static AtomicLong compressedLength = new AtomicLong(0);
	private static long LOCAL_CACHE_SIZE = 209715200;
	public static int MAP_CACHE_SIZE = 200;
	public static ConnectionChecker cc = null;
	public static RateLimiter rrl = null;
	public static RateLimiter wrl = null;
	public static boolean REMOVE_FROM_CACHE = true;
	public static boolean DISABLE_WRITE = false;
	public static boolean VERIFY_READS = false;
	public static boolean VERIFY_WRITES = false;
	public static boolean SMART_CACHE = false;
	private static LoadingCache<Long, SimpleByteArrayLongMap> cmaps = null;
	private static LoadingCache<Long, HashBlobArchive> archives = null;
	private static LoadingCache<Long, SimpleByteArrayLongMap> maps = null;
	private static LoadingCache<Long, FileChannel> openFiles = null;
	private static LoadingCache<Long, SimpleByteArrayLongMap> wMaps = null;
	private static LoadingCache<Long, FileChannel> wOpenFiles = null;
	private static boolean closed = false;
	private int blocksz = nextSize();
	private String uuid = "default";
	private boolean syncd = false;
	public static int maxQueueSize = 0;
	public AtomicInteger uncompressedLength = new AtomicInteger(0);
	public static AbstractHashEngine eng = HashFunctionPool.getHashEngine();

	public static void registerEventBus(Object obj) {
		eventUploadBus.register(obj);
	}

	// public FileLock fl = null;

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

	public static boolean isCached(long id) {
		return (archives.getIfPresent(id) != null);
	}

	public static long getCompressedLength() {

		return compressedLength.get();
	}

	public static void setCompressedLength(long val) {

		compressedLength.set(val);
	}

	public static void addToCompressedLength(long val) {

		compressedLength.addAndGet(val);
	}

	public static long getLength() {

		return currentLength.get();
	}

	public static void setLength(long val) {

		currentLength.set(val);
	}

	public static void addToLength(long val) {

		currentLength.addAndGet(val);
	}

	public static SimpleByteArrayLongMap getMap(long id) throws IOException {
		return getRawMap(id);
	}

	public static String getStrings(long id) throws IOException {
		HashBlobArchive har = null;
		File f = getPath(id);
		if (f.exists()) {
			har = new HashBlobArchive(f, id);
			return har.getHashesString();
		} else
			return null;

	}

	static HashSet<String> activeClaims = new HashSet<String>();

	public static void claimBlock(long id) throws IOException {
		IOException e = null;
		synchronized (activeClaims) {
			if (activeClaims.contains(Long.toString(id))) {
				return;
			} else {
				activeClaims.add(Long.toString(id));
			}
		}
		try {
			for (int i = 0; i < 10; i++) {
				try {
					store.checkoutObject(id, 1);
					e = null;
					return;
				} catch (IOException e1) {
					e = e1;
					try {
						Thread.sleep(30 * 1000);
					} catch (InterruptedException e2) {
						// TODO Auto-generated catch block

					} // 30 seconds;
				}
			}
			if (e != null)
				throw e;
		} finally {
			synchronized (activeClaims) {
				activeClaims.remove(Long.toString(id));
			}
		}
	}

	public static void deleteArchive(long id) throws IOException {
		HashBlobArchive har = null;
		File f = getPath(id);
		if (f.exists()) {
			har = new HashBlobArchive(f, id);
			har.delete();
		}
	}

	private static long nextSleepTime() {
		int nxt = -1;
		while (nxt < 1) {
			int Low = THREAD_SLEEP_TIME - VARIANCE_THREAD_SLEEP_TIME;
			int High = THREAD_SLEEP_TIME + VARIANCE_THREAD_SLEEP_TIME;
			nxt = r.nextInt(High - Low) + Low;
		}
		return THREAD_SLEEP_TIME;
	}

	private static int nextSize() {
		int nxt = -1;
		int nsz = (int) ((double) MAX_LEN * LEN_VARIANCE);
		while (nxt < 1) {
			int Low = MAX_LEN - nsz;
			int High = MAX_LEN + nsz;
			nxt = r.nextInt(High - Low) + Low;
		}
		return nxt;
	}

	public static void init(AbstractBatchStore nstore) throws IOException {
		Lock l = slock.writeLock();
		l.lock();
		try {
			store = nstore;
			chunk_location = new File(Main.chunkStore);
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			if (!store.isLocalData())
				staged_chunk_location = new File(Main.chunkStore + File.separator + "outgoing");
			else
				staged_chunk_location = new File(Main.chunkStore);

			if (!staged_chunk_location.exists()) {
				staged_chunk_location.mkdirs();
			}
			if (store.getMetaDataVersion() > 0) {
				offset = 1024;
				VERSION = store.getMetaDataVersion();
			}
			SyncQueue sc = new SyncQueue();
			Thread th = new Thread(sc);
			th.start();
			SDFSLogger.getLog()
					.info("############################ Initialied HashBlobArchive ##############################");
			SDFSLogger.getLog().info("Version : " + VERSION);
			SDFSLogger.getLog().info("HashBlobArchive IO Threads : " + Main.dseIOThreads);
			SDFSLogger.getLog().info("HashBlobArchive Max Upload Size : " + MAX_LEN);
			int msz = (int) ((double) MAX_LEN * LEN_VARIANCE) + MAX_LEN;
			MAX_HM_SZ = (int) ((msz / eng.getMinLen()) / .75f);
			MAX_HM_OPSZ = (int) ((msz / eng.getMinLen()));

			SDFSLogger.getLog().info("HashBlobArchive Max Map Size : " + MAX_HM_SZ);
			SDFSLogger.getLog().info("HashBlobArchive Maximum Local Cache Size : " + LOCAL_CACHE_SIZE);
			SDFSLogger.getLog().info("HashBlobArchive Max Thread Sleep Time : " + THREAD_SLEEP_TIME);
			SDFSLogger.getLog().info("HashBlobArchive Spool Directory : " + chunk_location.getPath());
			SDFSLogger.getLog().info("HashBlobArchive Queue Size : " + maxQueueSize);
			if (maxQueueSize == 0) {
				worksQueue = new SynchronousQueue<Runnable>();
				executor = new ThreadPoolExecutor(1, Main.maxOpenFiles * 2, 1, TimeUnit.MINUTES, worksQueue,
						new ThreadPoolExecutor.CallerRunsPolicy());
			}
			if (maxQueueSize > 0) {
				worksQueue = new LinkedBlockingQueue<>(maxQueueSize);
				executor = new ThreadPoolExecutor(Main.maxOpenFiles * 2, Main.maxOpenFiles * 2, 1, TimeUnit.MINUTES,
						worksQueue, new ThreadPoolExecutor.CallerRunsPolicy());
			}
			if (maxQueueSize < 0) {
				worksQueue = new LinkedBlockingQueue<>();
				executor = new ThreadPoolExecutor(Main.maxOpenFiles * 2, Main.maxOpenFiles * 2, 1, TimeUnit.MINUTES,
						worksQueue, new ThreadPoolExecutor.CallerRunsPolicy());
			}

			openFiles = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE)
					.removalListener(new RemovalListener<Long, FileChannel>() {
						public void onRemoval(RemovalNotification<Long, FileChannel> removal) {
							try {
								boolean fcClosed = false;
								int tries = 0;
								while (!fcClosed) {
									try {
										removal.getValue().close();
										fcClosed = true;
										SDFSLogger.getLog().debug("close " + removal.getKey());
									} catch (ClosedChannelException e) {
										fcClosed = true;
										break;
									} catch (Exception e) {
										if (tries > 100) {
											SDFSLogger.getLog().warn("Unable to close filechannel", e);
											fcClosed = true;
											break;
										} else {
											try {
												Thread.sleep(1000);
											} catch (Exception e1) {

											}
										}
									}
								}
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close filechannel", e);
							}
						}
					}).expireAfterAccess(60, TimeUnit.SECONDS).build(new CacheLoader<Long, FileChannel>() {
						public FileChannel load(Long hashid) throws IOException {
							try {

								File lf = new File(getPath(hashid).getPath());
								if (lf.exists()) {
									Path path = Paths.get(getPath(hashid).getPath());
									FileChannel fc = FileChannel.open(path, StandardOpenOption.WRITE,
											StandardOpenOption.READ);
									SDFSLogger.getLog()
											.debug("opened " + path.toString() + " opensize=" + openFiles.size());
									return fc;
								} else
									throw new Exception("unable to find file " + lf.getPath());
							} catch (Exception e) {
								SDFSLogger.getLog().error("unable to fetch hashmap [" + hashid + "]", e);
								throw new IOException("unable to read " + hashid);
							}
						}
					});
			wOpenFiles = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE * 2)
					.removalListener(new RemovalListener<Long, FileChannel>() {
						public void onRemoval(RemovalNotification<Long, FileChannel> removal) {
							try {
								boolean fcClosed = false;
								int tries = 0;
								while (!fcClosed) {
									try {
										removal.getValue().close();
										fcClosed = true;
										SDFSLogger.getLog().debug("close " + removal.getKey());
									} catch (ClosedChannelException e) {
										fcClosed = true;
										break;
									} catch (Exception e) {
										if (tries > 100) {
											SDFSLogger.getLog().warn("Unable to close filechannel", e);
											fcClosed = true;
											break;
										} else {
											try {
												Thread.sleep(1000);
											} catch (Exception e1) {

											}
										}
									}
								}
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close filechannel", e);
							}
						}
					}).build(new CacheLoader<Long, FileChannel>() {
						public FileChannel load(Long hashid) throws IOException {
							try {
								File lf = new File(getStagedPath(hashid), Long.toString(hashid));
								if (lf.exists()) {
									Path path = Paths.get(lf.getPath());
									FileChannel fc = FileChannel.open(path, StandardOpenOption.WRITE,
											StandardOpenOption.READ);
									SDFSLogger.getLog()
											.debug("opened " + path.toString() + " opensize=" + openFiles.size());
									return fc;
								} else
									throw new Exception("unable to find file " + lf.getPath());
							} catch (Exception e) {
								SDFSLogger.getLog().error("unable to fetch hashmap [" + hashid + "]", e);
								throw new IOException("unable to read " + hashid);
							}
						}
					});
			maps = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE)
					.removalListener(new RemovalListener<Long, SimpleByteArrayLongMap>() {
						public void onRemoval(RemovalNotification<Long, SimpleByteArrayLongMap> removal) {
							try {
								removal.getValue().close();
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close filechannel", e);
							}
						}
					}).expireAfterAccess(60, TimeUnit.SECONDS).build(new CacheLoader<Long, SimpleByteArrayLongMap>() {
						public SimpleByteArrayLongMap load(Long hashid) throws IOException {
							try {
								return getRawMap(hashid);
							} catch (Exception e) {
								throw new IOException(e);
							}
						}
					});
			wMaps = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE)
					.removalListener(new RemovalListener<Long, SimpleByteArrayLongMap>() {
						public void onRemoval(RemovalNotification<Long, SimpleByteArrayLongMap> removal) {
							try {
								removal.getValue().close();
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close filechannel", e);
							}
						}
					}).build(new CacheLoader<Long, SimpleByteArrayLongMap>() {
						public SimpleByteArrayLongMap load(Long hashid) throws IOException {
							try {
								return new SimpleByteArrayLongMap(
										new File(getStagedPath(hashid), Long.toString(hashid) + ".map").getPath(),
										MAX_HM_SZ, VERSION);
							} catch (Exception e) {
								throw new IOException(e);
							}
						}
					});

			cmaps = CacheBuilder.newBuilder().maximumSize(MAP_CACHE_SIZE)
					.removalListener(new RemovalListener<Long, SimpleByteArrayLongMap>() {
						public void onRemoval(RemovalNotification<Long, SimpleByteArrayLongMap> removal) {
							try {
								if (cacheReads || cacheWrites)
									removal.getValue().close();
								else {
									removal.getValue().close();
									removal.getValue().vanish();
								}

							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close map", e);
							}
						}
					}).concurrencyLevel(64).expireAfterAccess(60, TimeUnit.SECONDS)
					.build(new CacheLoader<Long, SimpleByteArrayLongMap>() {
						public SimpleByteArrayLongMap load(Long hashid) throws IOException {
							try {
								return getRawCacheMap(hashid);
							} catch (Exception e) {
								throw new IOException(e);
							}
						}
					});
			buildCache();
			if (!store.isLocalData()) {
				SDFSLogger.getLog().info(
						"############################ HashBlobArchive Checking for Archives not uploaded ##############################");
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
									SimpleByteArrayLongMap m = new SimpleByteArrayLongMap(lf.getPath(), MAX_HM_SZ, -1);
									wMaps.put(id, m);
								}
								executor.execute(arc);
							}
						} catch (Exception e) {
							c++;
							SDFSLogger.getLog().error("unable to upload " + ar.getPath(), e);
							e.printStackTrace();
							System.exit(3);
						}
					}
				}
				// archive = new HashBlobArchive(false, MAX_HM_SZ, -1);

				if (z > 0 || c > 0) {
					SDFSLogger.getLog().info("Uploaded " + z + " archives. Failed to upload " + c + " archives");
				}
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

			cc = new ConnectionChecker(store, store.getCheckInterval());
			SDFSLogger.getLog().info("################################# Done Uploading Archives #################");

		} finally {
			l.unlock();
		}
	}

	public static void setReadSpeed(double kbps, boolean update) {
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
			if (Main.volume != null && update) {
				try {
					Main.volume.writeUpdate();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to update volume", e);
				}
			}
		} finally {
			l.unlock();
		}
	}

	private static SimpleByteArrayLongMap getRawMap(long hashid) throws IOException {
		try {
			SimpleByteArrayLongMap m = null;

			File lf = new File(getPath(hashid).getPath() + ".map");
			try {
				if (lf.exists() && lf.length() > 0) {
					m = new SimpleByteArrayLongMap(lf.getPath(), MAX_HM_SZ, VERSION);
					return m;
				} else {
					SDFSLogger.getLog().debug("could not find " + lf.getPath());
				}

			} catch (Exception e) {
				if (HashBlobArchive.REMOVE_FROM_CACHE)
					lf.delete();

				SDFSLogger.getLog().error("unable to read " + lf.getPath(), e);
				throw new IOException("unable to read " + hashid);
			}
			if (m == null && HashBlobArchive.REMOVE_FROM_CACHE) {
				lf.delete();
				Map<String, Long> _m = store.getHashMap(hashid);
				double z = _m.size() * 1.25;
				int sz = new Long(Math.round(z)).intValue();
				Set<String> keys = _m.keySet();
				m = new SimpleByteArrayLongMap(lf.getPath(), sz, VERSION);
				for (String key : keys) {
					m.put(BaseEncoding.base64().decode(key), _m.get(key));
				}
				return m;
			} else
				throw new IOException("not able to fetch hashmap for " + hashid);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch hashmap [" + hashid + "]", e);
			throw new IOException("unable to read " + hashid);
		}
	}

	private static SimpleByteArrayLongMap getRawCacheMap(long hashid) throws IOException {
		try {
			SimpleByteArrayLongMap m = null;

			File lf = new File(getPath(hashid).getPath() + ".cmap");
			try {
				if (lf.exists() && lf.length() > 0) {
					m = new SimpleByteArrayLongMap(lf.getPath(), MAX_HM_SZ, VERSION);
				} else
					SDFSLogger.getLog().debug("could not find " + lf.getPath());

			} catch (Exception e) {
				m = null;
				lf.delete();
				SDFSLogger.getLog().error("unable to read " + lf.getPath(), e);
			}
			if (m == null) {
				lf.delete();
				m = new SimpleByteArrayLongMap(lf.getPath(), MAX_HM_SZ, VERSION);
			}
			return m;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch hashmap [" + hashid + "]", e);
			throw new IOException("unable to read " + hashid);
		}
	}

	public static void setWriteSpeed(double kbps, boolean update) {
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
			if (Main.volume != null && update) {
				try {
					Main.volume.writeUpdate();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to update volume", e);
				}
			}
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

	public static synchronized void setCacheSize(long sz, boolean update) throws IOException {
		Lock l = slock.writeLock();
		int tries = 0;
		l.lock();
		try {

			while (rchunks.size() > 0) {
				try {
					Thread.sleep(1);
					if (tries == 10) {
						throw new IOException("Unable to set cache in 10 seconds");
					}

				} catch (InterruptedException e) {
					SDFSLogger.getLog().error("error while closing ", e);
					tries++;
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
			if (Main.volume != null && update) {
				try {
					Main.volume.writeUpdate();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to update volume", e);
				}
			}
		} finally {
			l.unlock();
		}
	}

	public static long writeBlock(byte[] hash, byte[] chunk, String uuid)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {
		if (closed)
			throw new IOException("Closed");
		Lock l = slock.readLock();
		l.lock();
		if (uuid == null || uuid.trim() == "") {
			uuid = "default";
		}
		try {
			for (;;) {
				try {
					HashBlobArchive ar = writableArchives.get(uuid);
					ar.putChunk(hash, chunk);
					return ar.id;
				} catch (HashExistsException e) {
					throw e;
				} catch (ArchiveFullException | NullPointerException | ReadOnlyArchiveException e) {
					if (l != null)
						l.unlock();
					l = slock.writeLock();
					l.lock();
					try {
						HashBlobArchive ar = writableArchives.get(uuid);
						if (ar != null && ar.writeable)
							ar.putChunk(hash, chunk);
						else {
							ar = new HashBlobArchive(hash, chunk);
							ar.uuid = uuid;
							writableArchives.put(uuid, ar);
						}
						return ar.id;
					} catch (Exception e1) {
						l.unlock();
						l = null;
					} finally {
						if (l != null)
							l.unlock();
						l = null;
					}
				} catch (Throwable t) {
					SDFSLogger.getLog().error("unable to write", t);
					throw new IOException(t);
				}
			}
		} catch (NullPointerException e) {
			SDFSLogger.getLog().error("unable to write data", e);
			throw new IOException(e);
		} finally {
			if (l != null)
				l.unlock();
		}
	}

	private static AtomicLong cSz = new AtomicLong();

	private boolean reloading = false;

	private static void buildCache() throws IOException {
		archives = CacheBuilder.newBuilder().maximumWeight(LOCAL_CACHE_SIZE)
				.weigher(new Weigher<Long, HashBlobArchive>() {
					public int weigh(Long k, HashBlobArchive g) {
						SDFSLogger.getLog().debug("getting size for " + k + " size=" + Math.toIntExact(g.getFSize()));

						return Math.toIntExact(g.getFSize());
					}
				}).removalListener(new RemovalListener<Long, HashBlobArchive>() {
					public void onRemoval(RemovalNotification<Long, HashBlobArchive> removal) {
						if (!SMART_CACHE || (SMART_CACHE && !removal.getValue().reloading)) {
							SDFSLogger.getLog().debug("removing " + removal.getKey() + " from cache");
							cSz.addAndGet(-1 * removal.getValue().getFSize());
							removal.getValue().removeCache();
						} else {
							removal.getValue().reloading = false;
						}

					}
				}).build(new CacheLoader<Long, HashBlobArchive>() {
					public HashBlobArchive load(Long hashid) throws Exception {
						try {
							HashBlobArchive har = null;
							File f = getPath(hashid);
							if (!f.exists()) {
								har = new HashBlobArchive(hashid);
							} else
								har = new HashBlobArchive(f, hashid);
							har.cached = true;
							cSz.addAndGet(har.getFSize());
							return har;
						} catch (DataArchivedException e) {
							throw e;
						} catch (Exception e) {
							SDFSLogger.getLog().error("unable to fetch block [" + hashid + "]", e);
							throw e;
						}
					}

					public ListenableFuture<HashBlobArchive> reload(final Long key, HashBlobArchive archive) {
						if (SMART_CACHE)
							archive.reloading = true;
						else
							archive.reloading = false;
						ListenableFutureTask<HashBlobArchive> task = ListenableFutureTask
								.create(new Callable<HashBlobArchive>() {
									public HashBlobArchive call() {
										return archive;
									}
								});
						executor.execute(task);
						return task;
					}
				});
		if (REMOVE_FROM_CACHE) {
			SDFSLogger.getLog().info("############################ Caching Local Files ##############################");
			traverseCache(chunk_location);
			/*
			 * for (File ar : farchives) { if (ar.isDirectory() &&
			 * !ar.getName().equalsIgnoreCase("outgoing")) { if(ar.length() == 0)
			 * ar.delete(); else { try { Long id = Long.parseLong(ar.getName());
			 * HashBlobArchive har = new HashBlobArchive(ar,id);
			 * SDFSLogger.getLog().info("Archive Size=" + MemoryMeasurer.measureBytes(har));
			 * archives.put(id, har); } catch (Exception e) { SDFSLogger.getLog().error(
			 * "unable to upload " + ar.getPath(), e); } } } }
			 */

			long psz = chunk_location.getFreeSpace() + FileUtils.sizeOfDirectory(chunk_location);
			if (psz < LOCAL_CACHE_SIZE) {
				throw new IOException("Unable to intialize because available cache size of " + psz
						+ " is less than requested local cache of " + LOCAL_CACHE_SIZE + " for "
						+ chunk_location.getPath());
			}
			psz = staged_chunk_location.getFreeSpace() + FileUtils.sizeOfDirectory(staged_chunk_location);
			long csz = ((long) MAX_LEN * (long) (Main.dseIOThreads + 1));
			if (psz < csz) {
				throw new IOException("Unable to intialize because available staging size of " + psz
						+ " is less than requested local cache of " + csz + " for " + staged_chunk_location.getPath());
			}
			archives.cleanUp();

			SDFSLogger.getLog().info("Cached " + cAc + " archives of size " + cSz.get() + ".");
		}
	}

	static long cAc = 0;

	private static void traverseCache(File f) {
		if (f.isDirectory() && !f.getName().equalsIgnoreCase("outgoing") && !f.getName().equalsIgnoreCase("syncstaged")
				&& !f.getName().equalsIgnoreCase("ecstaged") && !f.getName().equalsIgnoreCase("metadata")
				&& !f.getName().equalsIgnoreCase("blocks") && !f.getName().equalsIgnoreCase("keys")
				&& !f.getName().equalsIgnoreCase("sync")) {
			File[] fs = f.listFiles();
			for (File z : fs) {
				if (z.isDirectory()) {
					traverseCache(z);
				} else {
					try {
						if (!z.getPath().contains(".")) {
							Long id = Long.parseLong(z.getName());
							// HashBlobArchive har = new HashBlobArchive(z, id);
							archives.get(id);
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

	public static byte[] getBlock(byte[] hash, long hbid) throws IOException, DataArchivedException {
		HashBlobArchive archive = rchunks.get(hbid);
		if (archive == null) {
			try {
				archive = archives.get(hbid);
			} catch (ExecutionException e1) {
				if (e1.getCause() instanceof DataArchivedException) {
					SDFSLogger.getLog().info("Data Archived " + hbid);
					throw (DataArchivedException) e1.getCause();
				} else if (e1.getCause() instanceof IOException) {
					SDFSLogger.getLog().warn("unable to get block " + hbid, e1);
					throw (IOException) e1.getCause();
				} else {
					SDFSLogger.getLog().warn("unable to get block " + hbid, e1);
					throw new IOException(e1.getCause());
				}
			}
		}
		byte[] z = null;
		try {
			z = archive.getChunk(hash);
		} catch (Exception e) {
			SDFSLogger.getLog().debug("exception while getting", e);
			archives.invalidate(hbid);
			try {
				archive = archives.get(hbid);
			} catch (ExecutionException e1) {
				if (e1.getCause() instanceof DataArchivedException)
					throw (DataArchivedException) e1.getCause();
				else if (e1.getCause() instanceof IOException)
					throw (IOException) e1.getCause();
				else
					throw new IOException(e1.getCause());
			}
			z = archive.getChunk(hash);
		}
		if (z != null && Main.REFRESH_BLOBS) {
			FileReplicationService.refreshArchive(hbid);
		}
		return z;
	}

	public static void cacheArchive(long hbid) throws ExecutionException, IOException, DataArchivedException {
		HashBlobArchive archive = rchunks.get(hbid);
		if (archive == null) {
			SDFSLogger.getLog().debug("caching " + hbid);
			archive = archives.get(hbid);
		}
		if (archive != null && Main.REFRESH_BLOBS) {
			FileReplicationService.refreshArchive(hbid);
		}
	}

	public static long compactArchive(long hbid) throws ExecutionException, IOException {
		HashBlobArchive archive = rchunks.get(hbid);
		if (archive == null) {
			archive = archives.get(hbid);
			return archive.compact();
		} else {
			return 0;
		}
	}

	private HashBlobArchive(boolean compact, int sz, int bsz) throws IOException {
		if (bsz > 0)
			this.blocksz = bsz;
		this.id = store.getNewArchiveID();

		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("waiting to write " + id + " rchunks sz=" + rchunks.size());
		this.writeable = true;
		this.compactStaged = compact;
		wMaps.put(id, new SimpleByteArrayLongMap(new File(staged_chunk_location, Long.toString(id) + ".map").getPath(),
				sz, VERSION));
		rchunks.put(this.id, this);
		if (!this.compactStaged)
			executor.execute(this);
		f = new File(staged_chunk_location, Long.toString(id));
		if (VERSION > 0) {
			RandomAccessFile zraf = new RandomAccessFile(f, "rw");
			FileChannel zfc = zraf.getChannel();
			ByteBuffer zb = ByteBuffer.allocate(offset);
			zb.putInt(VERSION);
			byte[] biv = PassPhrase.getByteIV();
			zb.put(biv);
			zb.position(0);
			zfc.write(zb);
			this.ivspec = new IvParameterSpec(biv);
			zfc.close();
			try {
				zraf.close();
			} catch (Exception e) {

			}

		}

	}

	private HashBlobArchive(byte[] hash, byte[] chunk)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {

		for (;;) {
			long pid = store.getNewArchiveID();
			while (pid < 100 && store.fileExists(pid) && new File(getStagedPath(pid), Long.toString(pid)).exists()) {
				pid = store.getNewArchiveID();
			}
			File cf = new File(getStagedPath(pid), Main.DSEID + "-" + Long.toString(pid) + ".vol");
			this.id = pid;
			f = new File(getStagedPath(pid), Long.toString(id));
			this.writeable = true;
			cf.setLastModified(System.currentTimeMillis());
			Path path = f.toPath();
			FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ,
					StandardOpenOption.CREATE);

			wOpenFiles.put(id, ch);

			rchunks.put(this.id, this);
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("waiting to write " + id + " rchunks sz=" + rchunks.size());
			wMaps.put(id, new SimpleByteArrayLongMap(new File(getStagedPath(pid), Long.toString(id) + ".map").getPath(),
					MAX_HM_SZ, VERSION));
			if (VERSION > 0) {
				ByteBuffer zb = ByteBuffer.allocate(offset);
				zb.putInt(VERSION);
				byte[] biv = PassPhrase.getByteIV();
				zb.put(biv);
				zb.position(0);

				try {
					ch.write(zb, 0);
				} catch (Exception e) {
					throw new IOException(e);
				}
				this.ivspec = new IvParameterSpec(biv);
			}
			this.putChunk(hash, chunk);

			executor.execute(this);
			break;
		}

	}

	private static File getStagedPath(long id) {
		if (!store.isLocalData())
			return staged_chunk_location;
		else {
			String st = Long.toString(id);

			File nf = null;
			if (id > -100 && id < 100) {
				nf = new File(chunk_location.getPath() + File.separator);
			} else if (id <= -100) {
				String dir = st.substring(0, 4);
				nf = new File(chunk_location.getPath() + File.separator + dir + File.separator);
			} else {
				String dir = st.substring(0, 3);
				nf = new File(chunk_location.getPath() + File.separator + dir + File.separator);
			}
			return nf;
		}
	}

	private Object SYNCHTL = new Object();

	private void syncHashTables() throws IOException {
		synchronized (SYNCHTL) {
			eventUploadBus.post(new ArchiveSync(this.getHashes(), this.id));
		}
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
		if (!nf.getParentFile().exists())
			nf.getParentFile().mkdirs();
		return nf;
	}

	private HashBlobArchive(Long id) throws Exception {
		this.id = id;
		f = getPath(id);
		if (cacheReads || VERSION == 0)
			this.loadData();
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Hit Rate = " + archives.stats().hitRate());
		if (VERSION > 0 && cacheReads) {
			RandomAccessFile zraf = new RandomAccessFile(f, "rw");
			FileChannel zfc = zraf.getChannel();
			ByteBuffer hbuf = ByteBuffer.allocate(offset);
			zfc.read(hbuf);
			zfc.close();
			try {
				zraf.close();
			} catch (Exception e) {

			}
			try {
				zfc.close();
			} catch (Exception e) {

			}
			hbuf.flip();
			hbuf.getInt();
			byte[] b = new byte[16];
			hbuf.get(b);
			this.ivspec = new IvParameterSpec(b);
		} else if (VERSION > 0 && !cacheReads) {
			byte[] b = store.getBytes(this.id, 4, 20);
			this.ivspec = new IvParameterSpec(b);
		}
	}

	private HashBlobArchive(File f, Long id) throws IOException {
		this.id = id;
		this.f = f;
		if (VERSION > 0) {
			RandomAccessFile zraf = new RandomAccessFile(f, "rw");
			FileChannel zfc = zraf.getChannel();
			ByteBuffer hbuf = ByteBuffer.allocate(offset);
			zfc.read(hbuf);
			zfc.close();
			try {
				zraf.close();
			} catch (Exception e) {

			}

			hbuf.flip();
			hbuf.getInt();
			byte[] b = new byte[16];
			hbuf.get(b);
			this.ivspec = new IvParameterSpec(b);

		}
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Hit Rate = " + archives.stats().hitRate());
	}

	public long getID() {
		return this.id;
	}

	private long getFSize() {
		long k = 0;
		if (this.f.exists())
			k = this.f.length();
		return k;
	}

	AtomicLong np = new AtomicLong(offset);

	private void putChunk(byte[] hash, byte[] chunk)
			throws IOException, ArchiveFullException, ReadOnlyArchiveException {

		if (VERIFY_WRITES) {
			byte[] _hash = eng.getHash(chunk);
			if (!Arrays.equals(_hash, hash)) {
				SDFSLogger.getLog().warn("hashes do not match on insert for [" + this.id + "] expected ["
						+ StringUtils.getHexString(hash) + "] recieved [" + StringUtils.getHexString(_hash) + "] ");
			}
		}
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
					chunk = EncryptUtils.encryptCBC(bf.array(), ivspec);
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
					if (np.get() >= this.blocksz
							|| wMaps.get(this.id).getCurrentSize() >= wMaps.get(this.id).getWMaxSz()) {
						this.writeable = false;

						synchronized (LOCK) {
							LOCK.notify();
						}
						throw new ArchiveFullException("archive full");
					}
					try {
						ch = wOpenFiles.get(this.id);
					} catch (ExecutionException e2) {
						throw new IOException(e2);
					}
					if (ch == null) {
						Path path = f.toPath();
						ch = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ,
								StandardOpenOption.CREATE);

						wOpenFiles.put(id, ch);
					}

					cp = np.get();
					np.set(cp + 4 + hash.length + 4 + chunk.length);
					try {
						boolean ins = true;
						if (VERSION == 0) {
							try {
								ins = wMaps.get(this.id).put(hash, (int) cp + 4 + hash.length);
								SDFSLogger.getLog()
										.debug("0 put  len " + chunk.length + " into " + this.id + " " + ins);
							} catch (IllegalStateException e) {
								throw new ArchiveFullException("archive full");
							}
						} else {
							ByteBuffer hf = ByteBuffer.allocate(8);
							hf.putInt((int) cp + 4 + hash.length);
							hf.putInt(chunk.length);
							hf.position(0);
							// int zd = (int) cp + 4 + hash.length;
							try {
								ins = wMaps.get(this.id).put(hash, hf.getLong());
								SDFSLogger.getLog()
										.debug("0 put  len " + chunk.length + " into " + this.id + " " + ins);
							} catch (IllegalStateException e) {
								throw new ArchiveFullException("archive full");
							}
							// SDFSLogger.getLog().info("put " + zd + " len " +
							// chunk.length + " into " +this.id);
						}
						if (!ins) {
							throw new HashExistsException(this.id, hash);
						}
					} catch (ArchiveFullException e1) {
						np.set(cp);
						this.writeable = false;
						synchronized (LOCK) {
							LOCK.notify();
						}
						throw e1;
					} catch (MapClosedException e1) {
						np.set(cp);
						this.writeable = false;
						synchronized (LOCK) {
							LOCK.notify();
						}
						throw new ArchiveFullException("archive closed");
					} catch (HashExistsException e) {
						np.set(cp);
						throw e;
					} catch (Exception e) {
						np.set(cp);
						SDFSLogger.getLog().error("error while putting chunk " + this.id, e);
						throw new IOException(e);
					}

				} catch (ExecutionException e3) {
					throw new IOException(e3);
				} finally {
					l.unlock();
				}
				ByteBuffer buf = ByteBuffer.allocateDirect(4 + hash.length + 4 + chunk.length);
				buf.putInt(hash.length);
				buf.put(hash);
				buf.putInt(chunk.length);
				buf.put(chunk);
				this.uncompressedLength.addAndGet(al);
				buf.position(0);

				// SDFSLogger.getLog().info("writing at " +f.length() + " bl=" +
				// buf.remaining() + "cs=" +chunk.length );
				try {
					ch.write(buf, cp);
				} catch (Exception e) {
					throw new IOException(e);
				}
			} finally {
				ul.unlock();
			}
		} else {
			throw new ArchiveFullException();
		}

	}

	public void delete() {
		if (DISABLE_WRITE)
			return;
		Lock l = this.lock.writeLock();
		l.lock();
		try {
			SDFSLogger.getLog().debug("deleting " + f.getPath());
			wMaps.invalidate(this.id);
			wOpenFiles.invalidate(new Long(id));

			archives.invalidate(this.id);
			SimpleByteArrayLongMap _m = maps.getIfPresent(this.id);
			if (_m != null)
				_m.close();
			maps.invalidate(this.id);
			cmaps.invalidate(this.id);
			FileChannel fc = openFiles.getIfPresent(this.id);
			if (fc != null) {
				try {
					fc.close();
				} catch (Exception e) {

				}
			}
			openFiles.invalidate(this.id);

			rchunks.remove(this.id);
			SDFSLogger.getLog().debug("removed " + f.getPath());
			f.delete();
			File lf = new File(f.getPath() + ".map");
			lf.delete();
			lf = new File(f.getPath() + ".cmap");
			lf.delete();
			lf = new File(f.getPath() + ".md");
			lf.delete();
			rchunks.remove(this.id);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error deleting object", e);
		} finally {
			l.unlock();
		}
	}

	private void removeCache() {
		if (DISABLE_WRITE)
			return;
		if (REMOVE_FROM_CACHE) {

			Lock l = this.lock.writeLock();
			l.lock();
			Lock ul = this.uploadlock.writeLock();
			ul.lock();
			synchronized (f) {
				try {
					SDFSLogger.getLog().debug("removing " + f.getPath());
					SimpleByteArrayLongMap m = maps.getIfPresent(this.id);
					if (m != null) {
						try {
							m.close();
						} catch (Exception e) {
						}
					}
					maps.invalidate(this.id);
					cmaps.invalidate(this.id);
					wMaps.invalidate(this.id);

					openFiles.invalidate(this.id);
					wOpenFiles.invalidate(this.id);
					boolean deleted = f.delete();
					if (!deleted)
						SDFSLogger.getLog().warn("unable to delete " + f.getPath());
					File lf = new File(f.getPath() + ".map");
					lf.delete();
					lf = new File(f.getPath() + ".cmap");
					lf.delete();
					SDFSLogger.getLog().debug("removed " + f.getPath());

				} finally {
					ul.unlock();
					l.unlock();

				}
			}
		}
	}

	private void loadData() throws Exception {
		synchronized (f) {
			/*
			 * if (f.exists() && f.length() > 0) {
			 * SDFSLogger.getLog().warn("file already exists! " + f.getPath()); File nf =
			 * new File(f.getPath() + " " + ".old"); Files.move(f.toPath(), nf.toPath(),
			 * StandardCopyOption.REPLACE_EXISTING); }
			 */
			try {
				SimpleByteArrayLongMap m = maps.getIfPresent(this.id);
				if (m != null) {
					try {
						m.close();
					} catch (Exception e) {
					}
				}

				maps.invalidate(this.id);
				cmaps.invalidate(this.id);
				FileChannel fc = openFiles.getIfPresent(this.id);
				if (fc != null) {
					try {
						fc.close();
					} catch (Exception e) {

					}
				}
				openFiles.invalidate(this.id);
				if (wOpenFiles.getIfPresent(this.id) != null) {
					wMaps.invalidate(this.id);
					fc = wOpenFiles.get(this.id);
					if (fc != null) {
						try {
							fc.close();
						} catch (Exception e) {

						}
					}
				} else {
					f.delete();
					File lf = new File(f.getPath() + ".map");
					lf.delete();
					lf = new File(f.getPath() + ".cmap");
					lf.delete();
				}
				SDFSLogger.getLog().debug("loading " + this.id);
				store.getBytes(this.id, f);
				if (rrl != null) {
					int _sz = 1;
					if (f.length() > 1024)
						_sz = toIntExact(f.length() / 1024);
					rrl.acquire(_sz);
				}

			} catch (Exception e) {
				throw e;
			} finally {

			}
		}
	}

	int smOffset = offset;
	private HashMap<Integer, ReentrantLock> hl = new HashMap<Integer, ReentrantLock>();

	private byte[] getChunk(byte[] hash) throws IOException, DataArchivedException {
		byte[] ub = null;

		long pos = 0;
		int nlen = 0;
		Lock l = this.lock.readLock();
		l.lock();
		try {

			SimpleByteArrayLongMap blockMap = null;
			if (!this.cached)
				blockMap = wMaps.get(this.id);
			if (blockMap == null) {

				SimpleByteArrayLongMap _blockMap = maps.get(this.id);
				if (SMART_CACHE) {
					try {
						blockMap = cmaps.get(this.id);

						pos = blockMap.get(hash);
					} catch (Exception e) {
						synchronized (f) {
							cmaps.invalidate(this.id);
							blockMap = cmaps.get(this.id);
							pos = toIntExact(blockMap.get(hash));
						}
					}
					if (pos == -1) {
						int _pos = -1;
						try {
							_pos = toIntExact(_blockMap.get(hash));
						} catch (Exception e) {
							synchronized (f) {
								maps.invalidate(this.id);
								_blockMap = maps.get(this.id);
								_pos = toIntExact(_blockMap.get(hash));
							}
						}
						ReentrantLock dl = null;
						synchronized (hl) {
							dl = hl.get(_pos);
							if (dl == null) {
								dl = new ReentrantLock();
								hl.put(_pos, dl);
							}
						}
						if (dl.tryLock()) {
							try {
								byte[] lub = store.getBytes(this.id, _pos, _pos + 4);
								ByteBuffer hb = ByteBuffer.wrap(lub);
								nlen = hb.getInt();
								ub = store.getBytes(this.id, _pos + 4, _pos + nlen + 4);

								synchronized (f) {
									blockMap = cmaps.get(this.id);
									pos = blockMap.get(hash);
									if (pos == -1) {
										// SDFSLogger.getLog().info("file = " + f.getPath() + " cpos=" + _pos);
										try {

											FileChannel fc = null;
											if (!f.exists()) {
												fc = FileChannel.open(f.toPath(), StandardOpenOption.WRITE,
														StandardOpenOption.READ, StandardOpenOption.CREATE);
												openFiles.put(id, fc);
												smOffset = offset;
											} else {
												fc = openFiles.get(id);
											}
											ByteBuffer bk = ByteBuffer.allocate(ub.length + 4);
											bk.putInt(nlen);
											bk.put(ub);
											bk.position(0);
											// int wo = -1;
											try {
												fc.write(bk, smOffset);
											} catch (Exception e) {
												openFiles.invalidate(id);
												fc = openFiles.get(id);
												fc.write(bk, smOffset);
											}
											// SDFSLogger.getLog().info(
											// "wrote file = " + f.getPath() + " wo=" + wo + " at=" + smOffset);
											blockMap.put(hash, smOffset);
											// SDFSLogger.getLog().info("put " + StringUtils.getHexString(hash)
											// + " in file = " + f.getPath() + " offset=" + smOffset);
											pos = smOffset;
											smOffset += ub.length + 4;
											archives.refresh(this.id);
										} catch (Exception e) {
											SDFSLogger.getLog().error("unable to write " + f.getPath(), e);
											throw new IOException(e);
										}
									}
								}
							} finally {
								dl.unlock();
								hl.remove(_pos);
							}
						} else {
							dl.lock();
							try {
								SDFSLogger.getLog().debug("dupdownload at " + _pos + " id " + this.id);
							} finally {
								dl.unlock();
							}
						}
					}
				} else {
					blockMap = maps.get(this.id);
				}
			}

			pos = blockMap.get(hash);
			if (pos == -1) {
				synchronized (f) {
					maps.invalidate(this.id);
					blockMap = maps.get(this.id);
					pos = blockMap.get(hash);
					if (pos == -1) {
						SDFSLogger.getLog().warn("requested block not found in " + f.getPath());
						return null;
					} else {
						this.loadData();
					}
				}
			} else if (VERSION == 0) {
				synchronized (f) {
					if (!f.exists()) {
						SDFSLogger.getLog().debug("file does not exist " + f.getPath());
						this.loadData();
					}
				}
				FileChannel ch = null;
				if (!this.cached) {

					ch = wOpenFiles.get(this.id);
					if (ch == null) {
						synchronized (f) {
							if (wOpenFiles.getIfPresent(this.id) == null) {
								Path path = Paths.get(getPath(id).getPath());
								ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
								wOpenFiles.put(id, ch);
							} else {
								ch = wOpenFiles.get(this.id);
							}
						}
					}
				}
				byte[] h = new byte[4];
				ByteBuffer hb = ByteBuffer.wrap(h);
				if (ch == null)
					try {
						ch = openFiles.get(id);
					} catch (Exception e) {
						synchronized (f) {
							try {
								ch = openFiles.get(id);
							} catch (Exception e1) {
								this.loadData();
								ch = openFiles.get(id);
							}
						}
					}
				try {
					ch.read(hb, pos);
				} catch (Exception e) {

					try {
						try {
							ch = openFiles.get(id);
						} catch (Exception e1) {
							SDFSLogger.getLog().warn("file not found during read from " + this.id + ", redownloading");
							synchronized (f) {
								try {
									ch = openFiles.get(id);

								} catch (Exception e2) {
									this.loadData();
									ch = openFiles.get(id);
								}

							}
						}
						synchronized (f) {
							try {
								ch.read(hb, pos);
							} catch (ClosedChannelException e1) {
								openFiles.invalidate(id);
								ch = openFiles.get(id);
								ch.read(hb, pos);
							}
						}
					} catch (ClosedChannelException e1) {
						throw e1;
					} catch (Exception e1) {
						throw new IOException(e1);
					}
				}
				hb.position(0);
				nlen = hb.getInt();
				if (nlen == 0) {

					synchronized (f) {
						try {
							ch = openFiles.get(id);
							hb.position(0);
							ch.read(hb, pos);
							hb.position(0);
							nlen = hb.getInt();
						} catch (Exception e) {
							nlen = 0;
						}
						if (nlen == 0) {
							SDFSLogger.getLog().error("Data length is zero in [" + this.id + "] at [" + this.f.getPath()
									+ "], redownloading...");
							this.loadData();
							ch = openFiles.get(id);
							hb.position(0);
							ch.read(hb, pos);
							hb.position(0);
							nlen = hb.getInt();
						}

					}
				}

				ub = new byte[nlen];
				try {
					synchronized (f) {
						ch.read(ByteBuffer.wrap(ub), pos + 4);
					}
				} catch (ClosedChannelException e) {
					throw e;
				} catch (Exception e) {
					throw new IOException(e);
				}
			} else {
				FileChannel ch = null;
				byte[] h = new byte[8];
				ByteBuffer hb = ByteBuffer.wrap(h);
				hb.putLong(pos);
				hb.position(0);
				int npos = hb.getInt();
				nlen = hb.getInt();
				if (nlen == 0) {
					SDFSLogger.getLog().warn("zero data read from " + this.id + ", redownloading");
					synchronized (f) {
						try {
							ch = openFiles.get(id);
							hb.position(0);
							ch.read(hb, pos);
							hb.position(0);
							npos = hb.getInt();
							nlen = hb.getInt();
						} catch (Exception e) {
							nlen = 0;
						}
						if (nlen == 0) {
							SDFSLogger.getLog().error("Data length is zero in [" + this.id + "] at [" + this.f.getPath()
									+ "], redownloading...");
							this.loadData();
							ch = openFiles.get(id);
							hb.position(0);
							ch.read(hb, pos);
							hb.position(0);
							npos = hb.getInt();
							nlen = hb.getInt();
						}

					}
				}
				if (cacheReads || f.exists()) {

					ub = new byte[nlen];
					if (ch == null)
						ch = openFiles.get(id);
					try {
						ch.read(ByteBuffer.wrap(ub), npos + 4);

					} catch (ClosedChannelException e) {
						throw e;
					} catch (Exception e) {
						throw new IOException(e);
					}
				} else {
					SDFSLogger.getLog().debug("getting " + npos + " nlen " + nlen + " from " + this.id + " pos " + pos);
					ub = store.getBytes(this.id, npos + 4, npos + nlen + 4);
					SDFSLogger.getLog().debug("got " + ub.length);
				}
			}
			// rf.seek(pos - HashFunctionPool.hashLength);

		} catch (ClosedChannelException e) {
			return getChunk(hash);
		} catch (MapClosedException e) {
			maps.invalidate(this.id);
			return getChunk(hash);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to read at " + pos + " " + nlen + " flen " + f.length() + " file="
					+ f.getPath() + " openFiles " + openFiles.size(), e);
			throw new IOException(e);
		} finally {

			l.unlock();
		}
		try {
			if (Main.chunkStoreEncryptionEnabled) {
				ub = EncryptUtils.decryptCBC(ub, ivspec);
			}

			ByteBuffer bf = ByteBuffer.wrap(ub);
			int cpz = bf.getInt();
			byte[] cp = new byte[bf.remaining()];
			bf.get(cp);
			if (cpz > 0) {
				cp = CompressionUtils.decompressLz4(cp, cpz);
			}
			// SDFSLogger.getLog().info("got " + cp.length + " cpz " +cpz);
			if (VERIFY_READS) {
				byte[] _hash = eng.getHash(cp);
				if (!Arrays.equals(_hash, hash)) {
					SDFSLogger.getLog()
							.error("hash mismatch on read  at " + pos + " " + nlen + " flen " + f.length() + " file="
									+ f.getPath() + " expected " + StringUtils.getHexString(hash) + " recieved "
									+ StringUtils.getHexString(_hash));
				}
			}
			return cp;

		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting data ul=" + ub.length + " file=" + this.id + " pos=" + pos, e);
			throw e;
		}
	}

	public int getLen() {
		File mapf = new File(getPath(id).getPath() + ".map");
		int ml = 0;
		if (mapf.exists())
			ml = (int) mapf.length();
		if (f != null && f.exists())
			ml += (int) f.length();
		return ml;
	}

	public int getSz() {
		SimpleByteArrayLongMap blockMap = null;
		Lock l = this.lock.readLock();
		l.lock();
		try {
			if (!this.cached)
				blockMap = wMaps.get(this.id);
			if (blockMap == null)
				blockMap = maps.get(this.id);
			return blockMap.getCurrentSize();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting size", e);
			return -1;
		} finally {
			l.unlock();
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

	Object iterlock = new Object();

	public String getHashesString() throws IOException {
		Lock l = this.lock.readLock();
		l.lock();
		synchronized (iterlock) {
			try {
				SimpleByteArrayLongMap blockMap = null;
				if (!this.cached)
					blockMap = wMaps.get(this.id);
				if (blockMap == null)
					blockMap = maps.get(this.id);
				
				blockMap.iterInit();
				KeyValuePair p = blockMap.next();
				StringBuffer sb = new StringBuffer();
				while (p != null) {
					sb.append(BaseEncoding.base64().encode(p.getKey()));
					sb.append(":");
					sb.append(Long.toString(p.getValue()));
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
			} finally {
				l.unlock();
			}
		}
	}

	public ArrayList<byte[]> getHashes() throws IOException {
		Lock l = this.lock.readLock();
		l.lock();
		synchronized (iterlock) {
			try {
				SimpleByteArrayLongMap blockMap = null;
				if (!this.cached)
					blockMap = wMaps.get(this.id);
				if (blockMap == null)
					blockMap = maps.get(this.id);

				blockMap.iterInit();
				KeyValuePair p = blockMap.next();
				ArrayList<byte[]> al = new ArrayList<byte[]>();
				while (p != null) {
					al.add(p.getKey());
					p = blockMap.next();

				}

				return al;
			} catch (Exception e) {
				SDFSLogger.getLog().error("error getting size", e);
				throw new IOException(e);
			} finally {
				l.unlock();
			}
		}
	}

	public long compact() throws IOException {
		if (DISABLE_WRITE || this.writeable)
			return 0;
		Lock l = this.lock.writeLock();
		try {
			l.lock();
			SimpleByteArrayLongMap blockMap = null;
			if (this.cached)
				try {
					blockMap = wMaps.get(this.id);
				} catch (ExecutionException e) {
					SDFSLogger.getLog().error("error getting archive map " + this.id, e);
				}
			if (blockMap != null)
				return 0;
		} finally {
			l.unlock();
		}
		HashBlobArchive _har = null;
		long ofl = f.length();
		int blks = 0;
		try {
			SimpleByteArrayLongMap _m = null;
			try {
				_m = getRawMap(this.id);
				if (_m == null)
					return 0;
			} catch (Exception e) {
				return 0;
			}
			ArrayList<KeyValuePair> ar = new ArrayList<KeyValuePair>();
			try {
				_m.iterInit();
				KeyValuePair p = _m.next();
				while (p != null) {
					if (HCServiceProxy.getHashesMap().mightContainKey(p.getKey(), this.id)) {
						ar.add(p);
						blks++;
					} else {
						SDFSLogger.getLog().debug("nk [" + StringUtils.getHexString(p.getKey()) + "] ");
					}
					p = _m.next();
				}
			} finally {
				try {
					_m.close();
				} catch (Exception e) {

				}
			}

			/*
			 * SDFSLogger.getLog() .debug("compacting " + this.id + " at " + f.getPath() +
			 * " by " + blks + " to " + _har.f.getPath() + " to len= " + new
			 * File(_har.f.getPath() + ".map").length() + " ms=" +
			 * wMaps.get(_har.id).getCurrentSize() + " ml= " + _mf.length());
			 */
			if (blks == 0) {
				this.delete();
				HashBlobArchive.addToCompressedLength(-1 * ofl);
				return -1 * ofl;
			} else {
				int ssz = (int) ((ar.size() * 2) + 5);
				_har = new HashBlobArchive(true, ssz, (int) ofl);
				for (KeyValuePair _p : ar) {
					try {
						byte ck[] = this.getChunk(_p.getKey());
						SDFSLogger.getLog().debug("[" + StringUtils.getHexString(_p.getKey()) + "] " + ck.length);
						_har.putChunk(_p.getKey(), ck);
					} catch (HashExistsException e) {
						SDFSLogger.getLog().debug("hash already inserted");
					}
				}
				l = this.lock.writeLock();
				try {
					l.lock();
					if (_har.f.exists() && (_har.f.length() - offset) > 0) {

						wMaps.invalidate(this.id);
						SimpleByteArrayLongMap _ma = maps.getIfPresent(this.id);
						if (_ma != null)
							_ma.close();
						maps.invalidate(this.id);
						FileChannel fc = openFiles.getIfPresent(this.id);
						if (fc != null) {
							try {
								fc.close();
							} catch (Exception e) {

							}
						}
						openFiles.invalidate(this.id);
						rchunks.remove(this.id);
						int trys = 0;
						while (!_har.moveFile(this.id)) {
							Thread.sleep(100);
							trys++;
							if (trys > 4)
								throw new IOException();
						}
						HashBlobArchive.addToCompressedLength(_har.f.length());
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
			if (_har != null) {
				HashBlobArchive.addToCompressedLength(-1 * _har.f.length());
				HashBlobArchive.addToLength(-1 * _har.uncompressedLength.get());
				if (_har.id != this.id)
					_har.delete();
			}

			// _har.delete();
			throw new IOException(e);
		}
		HashBlobArchive.addToCompressedLength(-1 * ofl);

		return f.length() - ofl;
	}

	// Random nd = new Random();
	private boolean uploadFile(long nid) throws Exception {
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
			eventUploadBus.post(new HashBlobArchiveUploaded(this));
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
		} finally {
			l.unlock();
		}
		return true;
	}

	private boolean moveFile(long nid) throws Exception {
		Lock l = this.lock.writeLock();
		l.lock();
		Lock ul = this.uploadlock.writeLock();
		ul.lock();
		try {

			maps.invalidate(this.id);
			rchunks.remove(this.id);
			archives.invalidate(this.id);
			wMaps.invalidate(this.id);
			File mf = new File(getPath(nid).getPath() + ".map");
			File omf = new File(f.getPath() + ".map");
			File cmf = new File(getPath(nid).getPath() + ".cmap");
			if (f.exists() && f.length() > 0) {
				wOpenFiles.invalidate(this.id);
				openFiles.invalidate(this.id);
				File nf = getPath(nid);
				Files.move(f.toPath(), nf.toPath(), StandardCopyOption.REPLACE_EXISTING);
				SDFSLogger.getLog().debug("moving " + omf.getPath() + " sz=" + omf.length() + " to " + mf.getPath()
						+ " sz=" + mf.length());
				Files.move(omf.toPath(), mf.toPath(), StandardCopyOption.REPLACE_EXISTING);
				Files.copy(mf.toPath(), cmf.toPath(), StandardCopyOption.REPLACE_EXISTING);
				this.cached = true;
				f = nf;
				archives.put(nid, this);
				this.id = nid;
				File cf = new File(staged_chunk_location, Main.DSEID + "-" + Long.toString(nid) + ".vol");
				cf.delete();

			}
			if(REMOVE_FROM_CACHE) {
				omf.delete();
				cmf.delete();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while moving file " + this.id, e);
			return false;
		} finally {
			ul.unlock();
			l.unlock();
		}
		return true;
	}

	private Object MVCHTL = new Object();

	private void moveDeleteArchive() {
		synchronized (MVCHTL) {
			if (!REMOVE_FROM_CACHE || cacheWrites) {
				try {
					if (!this.moveFile(id))
						return;
				} catch (Exception e) {
					SDFSLogger.getLog().debug("Exception" + e + "while moving archive");
				}
			} else {
				SDFSLogger.getLog().debug("deleting " + f);
				this.delete();
			}
		}
	}

	public String getUUID() {
		return this.uuid;
	}

	private boolean uploaded;

	private boolean upload(long nid) {

		if (this.compactStaged)
			return true;
		Lock l = this.lock.writeLock();
		l.lock();
		try {
			this.writeable = false;
			if (this.uuid != null)
				writableArchives.remove(this.uuid);
		} finally {
			l.unlock();
		}
		Lock ul = this.uploadlock.writeLock();
		ul.lock();
		SDFSLogger.getLog().debug("uploading " + this.id);
		try {
			if (this.uploaded)
				return true;
			if (f.exists() && (f.length() - offset) > 0) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("writing " + id);
				if (!this.uploadFile(nid))
					return false;
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote " + id);
			} else if (f.exists() && !this.cached) {
				this.delete();

				SimpleByteArrayLongMap om = wMaps.get(this.id);
				if (om != null) {
					try {
						om.close();
						om.vanish();
					} catch (Exception e) {

					}
				}
				wMaps.invalidate(this.id);
				new File(getStagedPath(this.id), Long.toString(this.id) + ".map").delete();
			} else {
				wOpenFiles.invalidate(this.id);
				SimpleByteArrayLongMap om = wMaps.get(this.id);
				if (om != null) {
					try {
						om.close();
						om.vanish();
					} catch (Exception e) {

					}
				}
				wMaps.invalidate(this.id);
				rchunks.remove(this.id);

				new File(getStagedPath(this.id), Long.toString(this.id) + ".map").delete();
			}
			this.uploaded = true;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
			return false;
		} finally {
			ul.unlock();
		}
		if (f.exists()) {
			HashBlobArchive.addToCompressedLength(f.length());
			HashBlobArchive.addToLength(uncompressedLength.get());
			// SDFSLogger.getLog().info("size=" + k + " uc=" + z + " fs=" +
			// f.length() + " ufs=" + uncompressedLength.get());
		}
		SDFSLogger.getLog().debug("uloaded " + id + " cachesize = " + worksQueue.size());
		return true;
	}

	final Object LOCK = new Object();

	@Override

	public void run() {
		long tm = System.currentTimeMillis();
		try {
			synchronized (LOCK) {
				LOCK.wait(nextSleepTime());
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while writing " + this.id, e);
		}

		long dur = System.currentTimeMillis() - tm;
		SDFSLogger.getLog().debug(dur + " len=" + f.length());
		if (this.compactStaged)
			return;
		while (!this.upload(this.id)) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {

			}
		}
		try {
			SyncQueue.syncQueue.put(this);
		} catch (InterruptedException e1) {

		}
	}

	private void _sync() throws SyncFailedException, IOException {
		Lock l = this.lock.writeLock();
		l.lock();
		try {
			if (!this.cached) {

				wOpenFiles.invalidate(this.id);

				SimpleByteArrayLongMap blockMap;
				try {
					blockMap = wMaps.get(this.id);
				} catch (ExecutionException e) {
					throw new IOException(e);
				}
				if (blockMap != null)
					blockMap.sync();
			}
		} finally {
			l.unlock();
		}
	}

	public static void sync(String uuid) {
		if (maxQueueSize == 0) {
			if (uuid == null || uuid.trim() == "")
				uuid = "default";

			HashBlobArchive ar = writableArchives.get(uuid);
			if (ar != null) {
				synchronized (ar.LOCK) {
					ar.LOCK.notify();
				}
			}
		}
	}

	public static void sync() {
		if (allowSync) {
			Lock l = slock.writeLock();
			l.lock();
			try {
				Collection<HashBlobArchive> st = rchunks.values();
				for (HashBlobArchive ar : st) {
					synchronized (ar) {
						ar.LOCK.notify();
					}
				}
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
			Collection<HashBlobArchive> st = null;
			Lock l = slock.writeLock();
			l.lock();
			try {
				st = rchunks.values();
			}finally {
				l.unlock();
			}
			HashSet<HashBlobArchive> bk = new HashSet<HashBlobArchive>();
			for (HashBlobArchive ar : st) {
				try {
					if (ar.syncd == false) {
						bk.add(ar);
					}
				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to sync", e);
				}
			}
			for (HashBlobArchive e : bk) {
				try {
					e.syncHashTables();
				} catch (IOException e1) {
					SDFSLogger.getLog().error("unable to sync " + e.f.getPath(), e1);
				}
			}

		}

	}

	public static void close() {
		closed = true;

		SDFSLogger.getLog().info("Closing HashBlobArchive in flush=" + rchunks.size());
		if (maxQueueSize == 0) {
			for (HashBlobArchive ar : rchunks.values()) {
				synchronized (ar.LOCK) {
					ar.LOCK.notify();
				}
			}
		}
		while (rchunks.size() > 0) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				SDFSLogger.getLog().error("error while closing ", e);
			}
		}
		SDFSLogger.getLog().info("Closed HashBlobArchive in flush=" + rchunks.size());
		cc.stop();
		maps.invalidateAll();
		openFiles.invalidateAll();
		wOpenFiles.invalidateAll();
		wMaps.invalidateAll();
		;

	}

	public static class BlockPolicy implements RejectedExecutionHandler {

		/**
		 * Creates a <tt>BlockPolicy</tt>.
		 */
		public BlockPolicy() {
		}

		/**
		 * Puts the Runnable to the blocking queue, effectively blocking the delegating
		 * thread until space is available.
		 * 
		 * @param r the runnable task requested to be executed
		 * @param e the executor attempting to execute this task
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
			try {
				e.getQueue().put(r);
			} catch (Exception e1) {
				SDFSLogger.getLog()
						.debug("Work discarded, thread was interrupted while waiting for space to schedule: {}", e1);
			}
		}
	}

	public static void main(String[] args) {
		byte[] b = StringUtils.getHexBytes(args[0]);
		String hb = BaseEncoding.base64().encode(b);
		System.out.println(hb);

	}

	public static class SyncQueue implements Runnable {
		private static LinkedBlockingQueue<HashBlobArchive> syncQueue = new LinkedBlockingQueue<HashBlobArchive>();
		private static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.dseIOThreads + 1,
				Main.dseIOThreads + 1, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new BlockPolicy());

		@Override
		public void run() {
			while (!HashBlobArchive.closed) {
				try {
					HashBlobArchive ar = syncQueue.take();
					ar._sync();
					ar.syncHashTables();
					ar.syncd = true;
					MoveThread mth = new MoveThread(ar);
					executor.execute(mth);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("issue while syncing archive", e);
				}
			}

		}

	}

	public static class MoveThread implements Runnable {
		HashBlobArchive ar = null;

		public MoveThread(HashBlobArchive ar) {
			this.ar = ar;
		}

		@Override
		public void run() {
			try {
				ar.moveDeleteArchive();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to move archive " + ar.f.getPath(), e);
			}

		}
	}

}