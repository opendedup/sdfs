package org.opendedup.sdfs.filestore.cloud;

import java.io.File;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import org.bouncycastle.util.Arrays;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.SyncFS;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.VolumeConfigWriterThread;
import org.opendedup.sdfs.io.events.CloudSyncDLRequest;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileSync;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileSync;
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.sdfs.io.events.VolumeWritten;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.OSValidator;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;

import fuse.SDFS.SDFSFileSystem;

public class FileReplicationService {
	public AbstractCloudFileSync sync = null;
	private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
	private static ReentrantLock iLock = new ReentrantLock(true);
	private static final int pl = Main.volume.getPath().length();
	private static final int sl = Main.dedupDBStore.length();
	private static final int maxTries = 3;
	private static final String DM = "/ThisIsADirectoryMarkerDoNotDelete";
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(2);
	private transient ThreadPoolExecutor executor = null;
	public static FileReplicationService service = null;

	private static EventBus eventUploadBus = new EventBus();

	public static void registerEvents(Object obj) {
		eventUploadBus.register(obj);
	}

	public FileReplicationService(AbstractCloudFileSync sync) {
		this.sync = sync;
		SparseDedupFile.registerListener(this);
		MetaDataDedupFile.registerListener(this);
		MetaFileStore.registerListener(this);
		VolumeConfigWriterThread.registerListener(this);
		if (OSValidator.isUnix())
			SDFSFileSystem.registerListener(this);
		SyncFS.registerListener(this);
		HCServiceProxy.registerListener(this);
		service = this;
	}

	private MetaDataDedupFile downloadMetaFile(String fname, File to) throws Exception {
		int tries = 0;
		for (;;) {
			to.delete();
			try {
				MetaFileStore.removedCachedMF(to.getPath());
				sync.downloadFile(fname, to, "files");
				MetaDataDedupFile mf = MetaFileStore.getMF(to);
				Main.volume.addDuplicateBytes(
						mf.getIOMonitor().getDuplicateBlocks() + mf.getIOMonitor().getActualBytesWritten(), true);
				Main.volume.addVirtualBytesWritten(mf.getIOMonitor().getVirtualBytesWritten(), true);
				SDFSLogger.getLog().debug("downloaded " + to.getPath() + " sz=" + to.length());
				return mf;
			} catch (Exception e) {
				if (tries > maxTries) {
					SDFSLogger.getLog().error("unable to sync file " + fname + " to " + to.getPath(), e);
					throw e;
				} else
					tries++;
			}
		}
	}

	private File downloadDDBFile(String guid, String lookupFilter) throws Exception {
		String sfp = Main.dedupDBStore + File.separator + guid.substring(0, 2) + File.separator + guid + File.separator
				+ guid + ".map";
		String dlf = guid.substring(0, 2) + "/" + guid + "/" + guid + ".map";
		SDFSLogger.getLog().info("downloading " + dlf + " to " + sfp);
		File f = new File(sfp);
		int tries = 0;
		for (;;) {
			f.delete();
			try {

				sync.downloadFile(dlf, f, "ddb");
				SDFSLogger.getLog().debug("downloaded " + f.getPath() + " sz=" + f.length());

				return f;
			} catch (Exception e) {
				if (tries > maxTries) {
					SDFSLogger.getLog().error("unable to sync ddb " + sfp + " to " + f.getPath(), e);
					throw e;
				} else
					tries++;
			}
		}

	}

	public static MetaDataDedupFile getMF(String fname) throws Exception {
		File f = new File(Main.volume.getPath() + File.separator + fname);
		fname = fname.replaceAll(Matcher.quoteReplacement("\\"), "/");
		MetaDataDedupFile mf = service.downloadMetaFile(fname, f);
		while (fname.startsWith(File.separator))
			fname = fname.substring(1);
		service.sync.checkoutFile("files/" + fname);
		return mf;
	}

	public static void refreshArchive(long id) {
		service.sync.addRefresh(id);
	}

	public static LongByteArrayMap getDDB(String fname, String lookupFilter) throws Exception {
		File f = service.downloadDDBFile(fname, lookupFilter);
		SDFSLogger.getLog().info("downloaded " + f.getPath() + " size= " + f.length());
		LongByteArrayMap m = LongByteArrayMap.getMap(fname, lookupFilter);
		service.sync.checkoutFile("ddb/" + fname);
		return m;
	}

	public static RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		return service.sync.getConnectedVolumes();
	}

	public static void removeVolume(long id) throws IOException {
		service.sync.removeVolume(id);
	}

	private ReentrantLock getLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			if (l == null) {
				l = new ReentrantLock(true);
				activeTasks.put(st, l);
			}
			return l;
		} finally {
			iLock.unlock();
		}
	}

	private void removeLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			try {

				if (l != null && !l.hasQueuedThreads()) {
					this.activeTasks.remove(st);
					SDFSLogger.getLog().debug("removed lock for " + st);
				}
				SDFSLogger.getLog().debug("lock count size is " + this.activeTasks.size());
			} finally {
				if (l != null && l.isLocked())
					l.unlock();
			}
		} finally {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("hmpa size=" + this.activeTasks.size());
			iLock.unlock();
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileDeleted(MFileDeleted evt) throws IOException {
		try {
			this.deleteFile(new File(evt.mf.getPath()));
			SDFSLogger.getLog().debug("deleted " + evt.mf.getPath());
			eventUploadBus.post(evt);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to delete " + evt.mf.getPath(), e);
			throw new IOException(e);
		}
	}
	/*
	 * @Subscribe
	 * 
	 * @AllowConcurrentEvents public void metaFileRenamed(MFileRenamed evt) {
	 * 
	 * try { ReentrantLock l = this.getLock(evt.mf.getPath()); l.lock(); int tries =
	 * 0; boolean done = false; while (!done) { try { if (SDFSLogger.isDebug())
	 * SDFSLogger.getLog().debug("renm " + evt.mf.getPath());
	 * 
	 * this.sync.renameFile("files/" + evt.from.substring(pl), evt.to.substring(pl),
	 * "files"); done = true; } catch (Exception e) { if (tries > maxTries) throw e;
	 * else tries++; } } } catch (Exception e) {
	 * SDFSLogger.getLog().error("unable to rename " + evt.mf.getPath(), e); }
	 * finally { removeLock(evt.mf.getPath()); }
	 * 
	 * }
	 */

	public void deleteFile(File f) throws IOException {
		boolean isDir = false;
		boolean isSymlink = false;
		ReentrantLock l = this.getLock(f.getPath());
		l.lock();
		try {
			if (f.exists()) {
				if (!OSValidator.isWindows()) {
					isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
							.isDirectory();
					isSymlink = Files.readAttributes(f.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
							.isSymbolicLink();
				} else {
					isDir = f.isDirectory();
				}
				if (!isSymlink && isDir) {
					File[] fs = f.listFiles();
					for (File _f : fs) {
						this.deleteFile(_f);
					}

				}
			}
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("delm " + f.getPath());
					String fn = f.getPath().substring(pl);
					if (!isSymlink && f.isDirectory())
						fn = fn + DM;
					this.sync.deleteFile(fn, "files");
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to delete " + f.getPath(), e);

		} finally {
			removeLock(f.getPath());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileWritten(MFileWritten evt) throws IOException {
		if (evt.mf.isFile() || evt.mf.isSymlink()) {
			try {
				
				ReentrantLock l = this.getLock(evt.mf.getPath());
				synchronized(l) {
					if(l.getQueueLength() > 1)
						return;
				}
				l.lock();
				int tries = 0;
				boolean done = false;
				while (!done) {
					try {
						if (evt.dirty || evt.mf.isSymlink()) {
							if (evt.mf.writeLock.tryLock(5, TimeUnit.SECONDS)) {
								try {
									SDFSLogger.getLog().debug("writem=" + evt.mf.getPath() + " len=" + evt.mf.length());
									this.sync.uploadFile(new File(evt.mf.getPath()), evt.mf.getPath().substring(pl),
											"files", new HashMap<String, String>(), false);
								} finally {
									evt.mf.writeLock.unlock();
								}
								eventUploadBus.post(evt);
							}
						} else {
							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug("nowritem " + evt.mf.getPath());
						}
						done = true;
					} catch (Exception e) {
						if (tries > maxTries)
							throw e;
						else
							tries++;
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to write " + evt.mf.getPath(), e);
			} finally {
				removeLock(evt.mf.getPath());
			}
		} else {
			writeFile(new File(evt.mf.getPath()));
		}
	}

	private void writeFile(File f) throws IOException {
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
		if (!isSymlink && isDir) {
			File[] fs = f.listFiles();
			for (File _f : fs) {
				this.writeFile(_f);
			}
		}
		try {
			ReentrantLock l = this.getLock(f.getPath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					String fn = f.getPath().substring(pl);
					if (!isSymlink && f.isDirectory())
						fn = fn + DM;
					this.sync.uploadFile(f, fn, "files", new HashMap<String, String>(), false);
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + f.getPath(), e);
		} finally {
			removeLock(f.getPath());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void sFileWritten(SFileWritten evt) {
		if (evt.getLocation() == -1) {
			try {
				ReentrantLock l = this.getLock(evt.sf.getDatabasePath());
				synchronized(l) {
					if(l.getQueueLength() > 1)
						return;
				}
				l.lock();
				int tries = 0;
				boolean done = false;
				while (!done) {
					try {
						if (evt.sf.isDirty()) {
							SDFSLogger.getLog().debug("writed " + evt.sf.getDatabasePath().substring(sl));
							this.sync.uploadFile(new File(evt.sf.getDatabasePath()),
									evt.sf.getDatabasePath().substring(sl), "ddb", new HashMap<String, String>(),
									false);
							if (Main.REFRESH_BLOBS) {
								evt.sf.bdb.iterInit();
								SparseDataChunk ck = evt.sf.bdb.nextValue(false);
								while (ck != null) {
									Collection<HashLocPair> pr = ck.getFingers().values();
									for (HashLocPair p : pr) {
										this.sync.addRefresh(Longs.fromByteArray(p.hashloc));
									}
									ck = evt.sf.bdb.nextValue(false);
								}
							}
							eventUploadBus.post(evt);
						} else {
							SDFSLogger.getLog().debug("nowrited " + evt.sf.getDatabasePath());
						}
						done = true;

					} catch (Exception e) {
						if (tries > maxTries)
							throw e;
						else
							tries++;
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to write " + evt.sf.getDatabasePath(), e);
			} finally {
				removeLock(evt.sf.getDatabasePath());
			}
		}

	}

	@Subscribe
	@AllowConcurrentEvents
	public void sFileSync(SFileSync evt) {
		try {
			ReentrantLock l = this.getLock(evt.sf.getPath());
			if (l.tryLock()) {
				try {
				int tries = 0;
				boolean done = false;
				while (!done) {
					try {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug("writed " + evt.sf.getPath());
						this.sync.uploadFile(evt.sf, evt.sf.getPath().substring(sl), "ddb",
								new HashMap<String, String>(), false);

						done = true;
					} catch (Exception e) {
						if (tries > maxTries)
							throw e;
						else
							tries++;
					}
				}
				}finally {
					l.unlock();
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.sf.getPath(), e);
		} finally {
			removeLock(evt.sf.getPath());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileSync(MFileSync evt) {

		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			try {
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("writem " + evt.mf.getPath());
					this.sync.uploadFile(new File(evt.mf.getPath()), evt.mf.getPath().substring(pl), "files",
							new HashMap<String, String>(), false);
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
			} finally {
				l.unlock();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void sFileDeleted(SFileDeleted evt) {
		try {
			ReentrantLock l = this.getLock(evt.sfp);
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("dels " + evt.sfp);
					SDFSLogger.getLog().debug("dels " + evt.sfp);
					this.sync.deleteFile(evt.sfp.substring(sl), "ddb");
					done = true;
					eventUploadBus.post(evt);
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to dels " + evt.sf, e);
		} finally {
			removeLock(evt.sfp);
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void volumeWritten(VolumeWritten evt) {

		try {
			ReentrantLock l = this.getLock(evt.vol.getConfigPath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("writev " + evt.vol.getConfigPath());
					this.sync.uploadFile(new File(evt.vol.getConfigPath()), new File(evt.vol.getConfigPath()).getName(),
							"volume", new HashMap<String, String>(), false);
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to write " + evt.vol.getConfigPath(), e);
		} finally {
			removeLock(evt.vol.getConfigPath());
		}
	}

	@Subscribe
	public void downloadAll(CloudSyncDLRequest req) {
		SDFSLogger.getLog().info("##################### Syncing Files from cloud now ########################");
		executor = new ThreadPoolExecutor(Main.dseIOThreads, Main.dseIOThreads, 10, TimeUnit.SECONDS, worksQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());

		try {
			this.sync.clearIter();
			MetaFileDownloader.reset();
			DDBDownloader.reset();
			String fname = this.sync.getNextName("files", req.getVolumeID());
			while (fname != null) {
				String efs = EncyptUtils.encString(fname, Main.chunkStoreEncryptionEnabled);
				if (this.sync.isCheckedOut("files/" + efs, req.getVolumeID())) {
					File f = new File(Main.volume.getPath() + File.separator + fname);
					if (fname.endsWith(DM)) {
						f = f.getParentFile();
						f.mkdirs();
					} else {
						executor.execute(new MetaFileDownloader(fname, f, sync));
					}
				} else {
					SDFSLogger.getLog().info("not checked out " + fname);
				}
				fname = this.sync.getNextName("files", req.getVolumeID());
			}
			executor.shutdown();
			// Wait for everything to finish.
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				SDFSLogger.getLog().info("Awaiting file download completion of threads.");
			}
			if (MetaFileDownloader.downloadSyncException != null)
				throw MetaFileDownloader.downloadSyncException;
			this.sync.clearIter();

			SDFSLogger.getLog().info("################# done syncing files from cloud #######################");
			SDFSLogger.getLog().info("Metadata Files downloaded : " + MetaFileDownloader.fdl.get());
			SDFSLogger.getLog().info("Metadata File download errors: " + MetaFileDownloader.fer.get());
			Main.syncDL = false;

		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync", e);
		}

	}

	private static class MetaFileDownloader implements Runnable {
		private static Exception downloadSyncException;
		AbstractCloudFileSync sync;
		String fname;
		File to;
		private static AtomicInteger fer = new AtomicInteger();
		private static AtomicInteger fdl = new AtomicInteger();

		private static void reset() {
			fer.set(0);
			fdl.set(0);
			downloadSyncException = null;
		}

		MetaFileDownloader(String fname, File to, AbstractCloudFileSync sync) {
			this.sync = sync;
			this.to = to;
			this.fname = fname;
		}

		@Override
		public void run() {
			int tries = 0;
			boolean done = false;
			while (!done) {
				to.delete();
				try {
					sync.downloadFile(fname, to, "files");
					sync.checkoutFile("files/" + fname);
					MetaDataDedupFile mf = MetaFileStore.getMF(to);
					DDBDownloader dl = new DDBDownloader(mf);
					dl.download();
					Main.volume.addDuplicateBytes(
							mf.getIOMonitor().getDuplicateBlocks() + mf.getIOMonitor().getActualBytesWritten(), true);
					Main.volume.addVirtualBytesWritten(mf.getIOMonitor().getVirtualBytesWritten(), true);
					Main.volume.addFile();
					SDFSLogger.getLog().debug("downloaded " + to.getPath() + " sz=" + to.length());
					done = true;
					fdl.incrementAndGet();
				} catch (Exception e) {
					if (tries > maxTries) {
						SDFSLogger.getLog().error("unable to sync file " + fname + " to " + to.getPath(), e);
						downloadSyncException = e;
						done = true;
						fer.incrementAndGet();
					} else
						tries++;
				}
			}
		}

	}

	private static class DDBDownloader {
		MetaDataDedupFile mf;
		private static AtomicInteger der = new AtomicInteger();
		private static AtomicInteger ddl = new AtomicInteger();

		private static void reset() {
			der.set(0);
			ddl.set(0);
		}

		DDBDownloader(MetaDataDedupFile mf) {
			this.mf = mf;
		}

		public void download() {
			try {
				int tries = 0;
				boolean done = false;
				while (!done) {
					try {
						FileReplicationService.getDDB(mf.getDfGuid(), mf.getLookupFilter());
						SDFSLogger.getLog().info("downloaded " + mf.getDfGuid());
						LongByteArrayMap ddb = (LongByteArrayMap) mf.getDedupFile(false).bdb;
						Set<Long> blks = new HashSet<Long>();
						if (ddb.getVersion() < 2)
							throw new IOException("only files version 2 or later can be imported");
						try {
							ddb.iterInit();
							for (;;) {
								LongKeyValue kv = ddb.nextKeyValue(false);
								if (kv == null)
									break;
								SparseDataChunk ck = kv.getValue();
								boolean dirty = false;
								TreeMap<Integer, HashLocPair> al = ck.getFingers();
								for (HashLocPair p : al.values()) {
									ChunkData cm = new ChunkData(Longs.fromByteArray(p.hashloc), p.hash);

									InsertRecord ir = null;
									if (mf.getLookupFilter() != null && Main.enableLookupFilter) {
										long pos = HCServiceProxy.getLookupFilter(mf.getLookupFilter()).put(p.hash,
												Longs.fromByteArray(p.hashloc));
										if (pos != -1) {
											ir = new InsertRecord(false, pos);
										} else {
											ir = HCServiceProxy.getHashesMap().put(cm, false);
											HCServiceProxy.getLookupFilter(mf.getLookupFilter()).put(p.hash, 1,
													Longs.fromByteArray(ir.getHashLocs()));
										}
									} else {
										ir = HCServiceProxy.getHashesMap().put(cm, false);
									}
									Main.volume.addVirtualBytesWritten(p.len, false);
									Main.volume.addDuplicateBytes(p.len, false);
									if (ir.getInserted())
										blks.add(Longs.fromByteArray(ir.getHashLocs()));
									else {
										if (!Arrays.areEqual(p.hashloc, ir.getHashLocs())) {
											p.hashloc = ir.getHashLocs();
											blks.add(Longs.fromByteArray(ir.getHashLocs()));
											dirty = true;
										}
									}
								}
								if (dirty)
									ddb.put(kv.getKey(), ck);
							}
							for (Long l : blks) {
								boolean inserted = false;
								int trs = 0;
								while (!inserted) {
									try {
										HashBlobArchive.claimBlock(l);
										inserted = true;

									} catch (Exception e) {
										trs++;

										if (trs > 100)
											throw e;
										else
											Thread.sleep(5000);
									}
								}
							}
						} catch (Throwable e) {
							SDFSLogger.getLog().warn("error while checking file [" + ddb + "]", e);
							throw new IOException(e);
						} finally {
							ddb.close();
							ddb = null;
						}
						done = true;
						ddl.incrementAndGet();
					} catch (Exception e) {
						if (tries > maxTries) {
							SDFSLogger.getLog().error("unable to sync ddb " + mf.getPath() + " to " + mf.getDfGuid(),
									e);
							der.incrementAndGet();
							done = true;
						} else
							tries++;
					}
				}

			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to recover " + mf.getPath(), e);
				der.incrementAndGet();
			}

		}

	}

}