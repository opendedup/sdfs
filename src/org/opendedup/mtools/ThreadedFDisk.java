package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class ThreadedFDisk {

	private ArrayBlockingQueue<File> queue = new ArrayBlockingQueue<File>(Main.writeThreads + 1);
	private ArrayList<CheckFileThread> ct = new ArrayList<CheckFileThread>();
	private long files = 0;
	private long corruptFiles = 0;
	private final ReentrantLock zLock = new ReentrantLock();

	public ThreadedFDisk() throws IOException {
		SDFSLogger.getLog().info("Starting Threaded FDISK");
		long start = System.currentTimeMillis();
		for(int i = 0;i<Main.writeThreads;i++) {
			CheckFileThread th = new CheckFileThread();
			ct.add(th);
		}
		File f = new File(Main.dedupDBStore);
		try {
			this.traverse(f);
			while(queue.size() >0) {
				Thread.sleep(1000);
			}
			
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			throw new IOException(e);
		} finally {
			for(int i = 0;i<ct.size();i++) {
				ct.get(i).close();
			}
			ct.clear();
		}
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - start) / 1000
						+ "] seconds to check [" + files + "]. Found ["
						+ this.corruptFiles + "] corrupt files");
		if (this.corruptFiles > 0) {
			SDFSEvent.gcWarnEvent(this.corruptFiles
					+ " Corrupt Files found during FDisk task.");
		}
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			try {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					traverse(new File(dir, children[i]));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error traversing " + dir.getPath(),
						e);
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				try {
					SDFSLogger.getLog().debug("adding " + dir.getPath() + " to queue");
					zLock.lock();
					queue.put(dir);
					zLock.unlock();
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		}
	}

	private class CheckFileThread implements Runnable {
		private final ReentrantLock cLock = new ReentrantLock();
		private boolean closed = false;
		Thread _th = null;
		private CheckFileThread() {
			_th = new Thread(this);
			_th.start();
		}
		public void run() {
			while (!closed) {
				try {
					Thread.sleep(1);
					zLock.lock();
					File f = queue.poll();
					zLock.unlock();
					if(f!= null){
						try {
							checkDedupFile(f);
						} catch (IOException e) {
							SDFSLogger.getLog().info("Unable to check file " + f.getPath());
						}
					}
						
				} catch (InterruptedException e) {
				}
			}
		}
		
		private void checkDedupFile(File mapFile) throws IOException {
			cLock.lock();
			LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath(), "r");
			try {
				byte[] val = new byte[0];
				mp.iterInit();
				boolean corruption = false;
				long corruptBlocks = 0;
				while (val != null) {
					val = mp.nextValue();
					if (val != null) {
						SparseDataChunk ck = new SparseDataChunk(val);
						if (!ck.isLocalData()) {
							boolean exists = HCServiceProxy
									.hashExists(ck.getHash());
							if (!exists) {
								SDFSLogger.getLog().debug("file ["+ mapFile +"] could not find " + StringUtils.getHexString(ck.getHash()));
								corruption = true;
								corruptBlocks ++;
							}
						}
					}
				}
				if (corruption) {
					corruptFiles++;
					SDFSLogger.getLog().info(
							"map file " + mapFile.getPath() + " is suspect, [" + corruptBlocks + "] missing blocks found.");
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"error while checking file [" + mapFile.getPath() + "]", e);
				throw new IOException(e);
			} finally {
				
				mp.close();
				mp = null;
				cLock.unlock();
			}
			files++;
		}
		
		private void close() {
			cLock.lock();
			SDFSLogger.getLog().debug("closing CheckFileThread");
			this.closed = true;
			_th.interrupt();
			cLock.unlock();
		}

	}
	
	

}
