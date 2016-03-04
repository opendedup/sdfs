package org.opendedup.sdfs.filestore.cloud;

import java.io.File;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;



import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.SyncFS;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.VolumeConfigWriterThread;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.io.events.CloudSyncDLRequest;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileRenamed;
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

import fuse.SDFS.SDFSFileSystem;

public class FileReplicationService {
	AbstractCloudFileSync sync = null;
	private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
	private static ReentrantLock iLock = new ReentrantLock(true);
	private static final int pl = Main.volume.getPath().length();
	private static final int sl = Main.dedupDBStore.length();
	private static final int maxTries = 3;
	private static final String DM = "/ThisIsADirectoryMarkerDoNotDelete";
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private transient ThreadPoolExecutor executor = null;
	
	private static EventBus eventMetaDataBus = new EventBus();
	private static EventBus eventDBBus = new EventBus();

	public static void registerMetaDataListener(Object obj) {
		eventMetaDataBus.register(obj);
	}
	
	public static void registerDBListener(Object obj) {
		eventDBBus.register(obj);
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
				}
			} finally {
				if (l != null)
					l.unlock();
			}
		} finally {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"hmpa size=" + this.activeTasks.size());
			iLock.unlock();
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileDeleted(MFileDeleted evt) throws IOException {
		try {
		//SDFSLogger.getLog().info("eeeks " + evt.mf.getPath());
		this.deleteFile(new File(evt.mf.getPath()));
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to delete " + evt.mf.getPath(),e);
			throw new IOException(e);
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileRenamed(MFileRenamed evt) {

		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("renm " + evt.mf.getPath());

					this.sync.renameFile("files/" + evt.from.substring(pl),
							evt.to.substring(pl), "files");
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to rename " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}

	}

	private void deleteFile(File f) throws IOException {
		boolean isDir = false;
		boolean isSymlink = false;
		if (!OSValidator.isWindows()) {
			isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			isSymlink = Files.readAttributes(f.toPath(),
					PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
		} else {
			isDir = f.isDirectory();
		}
		if (!isSymlink &&isDir) {
			File[] fs = f.listFiles();
			for (File _f : fs) {
				this.deleteFile(_f);
			}

		}
		try {
			ReentrantLock l = this.getLock(f.getPath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("delm " + f.getPath());
					String fn = f.getPath().substring(pl);
					if (!isSymlink &&f.isDirectory())
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
			SDFSLogger.getLog().error("unable to delete " + f.getPath(), e);

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
				l.lock();
				int tries = 0;
				boolean done = false;
				while (!done) {
					try {
						if (evt.mf.isDirty()|| evt.mf.isSymlink()) {
								SDFSLogger.getLog().debug(
										"writem " + evt.mf.getPath() + " info=" + evt.mf.toJSON(false));
							this.sync.uploadFile(new File(evt.mf.getPath()),
									evt.mf.getPath().substring(pl), "files");
							
							eventMetaDataBus.post(evt);
						} else {
							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug(
										"nowritem " + evt.mf.getPath());
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
				SDFSLogger.getLog().error(
						"unable to write " + evt.mf.getPath(), e);
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
			isDir = Files.readAttributes(f.toPath(), PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			isSymlink = Files.readAttributes(f.toPath(),
					PosixFileAttributes.class,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
		} else {
			isDir = f.isDirectory();
		}
		if (!isSymlink &&isDir) {
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
					this.sync.uploadFile(f, fn, "files");
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
		try {
			ReentrantLock l = this.getLock(evt.sf.getDatabasePath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (evt.sf.isDirty()) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"writed " + evt.sf.getDatabasePath());
						this.sync
								.uploadFile(new File(evt.sf.getDatabasePath()),
										evt.sf.getDatabasePath().substring(sl),
										"ddb");
						eventDBBus.post(evt);
						
					} else {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"nowrited " + evt.sf.getDatabasePath());
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
			SDFSLogger.getLog().error(
					"unable to write " + evt.sf.getDatabasePath(), e);
		} finally {
			removeLock(evt.sf.getDatabasePath());
		}

	}

	@Subscribe
	@AllowConcurrentEvents
	public void sFileSync(SFileSync evt) {
		try {
			ReentrantLock l = this.getLock(evt.sf.getPath());
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("writed " + evt.sf.getPath());
					
					this.sync.uploadFile(evt.sf,
							evt.sf.getPath().substring(sl), "ddb");

					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
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
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("writem " + evt.mf.getPath());
					SDFSLogger.getLog().debug(
							"synced " + evt.mf.getPath() + " info=" + evt.mf.toJSON(false));
					this.sync.uploadFile(new File(evt.mf.getPath()), evt.mf
							.getPath().substring(pl), "files");
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
	}

	@Subscribe
	@AllowConcurrentEvents
	public void sFileDeleted(SFileDeleted evt) {

		try {
			ReentrantLock l = this.getLock(evt.sf);
			l.lock();
			int tries = 0;
			boolean done = false;
			while (!done) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("dels " + evt.sf);
					this.sync.deleteFile(evt.sf.substring(sl), "ddb");
					done = true;
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
			removeLock(evt.sf);
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
						SDFSLogger.getLog().debug(
								"writev " + evt.vol.getConfigPath());
					this.sync.uploadFile(new File(evt.vol.getConfigPath()),
							new File(evt.vol.getConfigPath()).getName(),
							"volume");
					done = true;
				} catch (Exception e) {
					if (tries > maxTries)
						throw e;
					else
						tries++;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug(
					"unable to write " + evt.vol.getConfigPath(), e);
		} finally {
			removeLock(evt.vol.getConfigPath());
		}
	}

	@Subscribe
	public void downloadAll(CloudSyncDLRequest req) {
		SDFSLogger
				.getLog()
				.info("##################### Syncing Files from cloud now ########################");
		executor = new ThreadPoolExecutor(Main.dseIOThreads,
				Main.dseIOThreads, 10, TimeUnit.SECONDS, worksQueue,
				executionHandler);
		
		try {
			this.sync.clearIter();
			MetaFileDownloader.reset();
			DDBDownloader.reset();
			String fname = this.sync.getNextName("files");
			while (fname != null) {
					File f = new File(Main.volume.getPath() + File.separator
							+ fname);
					if (fname.endsWith(DM)) {
						f = f.getParentFile();
						f.mkdirs();
					} else {
						executor.execute(new MetaFileDownloader(fname,f,sync));
						
					}
				fname = this.sync.getNextName("files");
			}
			executor.shutdown();
			// Wait for everything to finish.
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
			  SDFSLogger.getLog().info("Awaiting file download completion of threads.");
			}
			if(MetaFileDownloader.downloadSyncException != null)
				throw MetaFileDownloader.downloadSyncException;
			this.sync.clearIter();
			executor = new ThreadPoolExecutor(Main.dseIOThreads ,
					Main.dseIOThreads, 10, TimeUnit.SECONDS, worksQueue,
					executionHandler);
			fname = this.sync.getNextName("ddb");
			while (fname != null) {
				executor.execute(new DDBDownloader(fname,sync));
				fname = this.sync.getNextName("ddb");
			}
			executor.shutdown();
			// Wait for everything to finish.
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
			  SDFSLogger.getLog().info("Awaiting ddb download completion of threads.");
			}
			if(DDBDownloader.downloadSyncException != null)
				throw DDBDownloader.downloadSyncException;
			SDFSLogger
					.getLog()
					.info("################# done syncing files from cloud #######################");
			SDFSLogger.getLog().info("Metadata Files downloaded : " + MetaFileDownloader.fdl.get());
			SDFSLogger.getLog().info("Metadata File download errors: " + MetaFileDownloader.fer.get());
			SDFSLogger.getLog().info("Map Files downloaded : " + DDBDownloader.ddl.get());
			SDFSLogger.getLog().info("Map File download errors :" + DDBDownloader.der.get());
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
					MetaDataDedupFile mf = MetaFileStore.getMF(to);
					Main.volume.addDuplicateBytes(mf.getIOMonitor()
							.getDuplicateBlocks()
							+ mf.getIOMonitor()
									.getActualBytesWritten(), true);
					Main.volume.addVirtualBytesWritten(mf
							.getIOMonitor()
							.getVirtualBytesWritten(), true);
					SDFSLogger.getLog().info(
							"downloaded " + to.getPath() + " sz="
									+ to.length() + " mf=" + mf.toJSON(false));
					done = true;
					fdl.incrementAndGet();
				} catch (Exception e) {
					if (tries > maxTries) {
						SDFSLogger.getLog().error("unable to sync file " + fname + " to " + to.getPath(),e);
						downloadSyncException = e;
						done = true;
						fer.incrementAndGet();
					}
					else
						tries++;
				}
			}
			
		}
		
	}
	
	private static class DDBDownloader implements Runnable {
		private static Exception downloadSyncException;
		AbstractCloudFileSync sync;
		String fname;
		private static AtomicInteger der = new AtomicInteger();
		private static AtomicInteger ddl = new AtomicInteger();
		
		private static void reset() {
			der.set(0);
			ddl.set(0);
			downloadSyncException = null;
		}
		
		DDBDownloader(String fname, AbstractCloudFileSync sync) {
			this.sync = sync;
			this.fname = fname;
		}
		
		@Override
		public void run() {
			try {
				File f = new File(Main.dedupDBStore + File.separator
						+ fname);
				int tries = 0;
				boolean done = false;
				while (!done) {
					f.delete();
					try {
						sync.downloadFile(fname, f, "ddb");
						SDFSLogger.getLog().info(
								"downloaded " + f.getPath() + " sz="
										+ f.length());
						done = true;
						ddl.incrementAndGet();
					} catch (Exception e) {
						if (tries > maxTries) {
							SDFSLogger.getLog().error("unable to sync ddb " + fname + " to " + f.getPath(),e);
							downloadSyncException = e;
							der.incrementAndGet();
							done = true;
						}else
							tries++;
					}
				}

			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to recover " + fname, e);
				downloadSyncException = e;
				der.incrementAndGet();
			}
			
		}
		
	}

}