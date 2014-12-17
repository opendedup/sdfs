package org.opendedup.hashing;

import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.QuickList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BufferClosedException;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class PoolThread implements AbstractPoolThread, Runnable {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private boolean isStopped = false;
	private static int maxTasks = ((Main.maxWriteBuffers * 1024 * 1024) / (Main.CHUNK_LENGTH)) + 1;
	// private static final int maxTasks = 40;
	private final QuickList<WritableCacheBuffer> tasks = new QuickList<WritableCacheBuffer>(
			maxTasks);
	Thread th = null;

	static {
		if (maxTasks > 120)
			maxTasks = 120;
		SDFSLogger.getLog().info("Pool List Size will be " + maxTasks);
	}

	public PoolThread(BlockingQueue<WritableCacheBuffer> queue) {
		taskQueue = queue;
	}

	@Override
	public void run() {
		while (!isStopped()) {
			try {
				tasks.clear();
				int ts = taskQueue.drainTo(tasks, maxTasks);
				if (ts > 0) {
					if (Main.chunkStoreLocal) {
						for (int i = 0; i < ts; i++) {
							WritableCacheBuffer runnable = tasks.get(i);
							try {
								runnable.close();
							} catch (Exception e) {
								SDFSLogger.getLog().fatal(
										"unable to execute thread", e);
							}
						}
					} else {

						if (HashFunctionPool.max_hash_cluster == 1) {
							for (int i = 0; i < ts; i++) {
								WritableCacheBuffer runnable = tasks.get(i);
								runnable.startClose();
								AbstractHashEngine hc = SparseDedupFile.hashPool
										.borrowObject();
								byte[] hash = null;

								try {

									byte[] b = runnable.getFlushedBuffer();
									hash = hc.getHash(b);
									ArrayList<HashLocPair> ar = new ArrayList<HashLocPair>();
									HashLocPair p = new HashLocPair();
									p.hash = hash;
									p.pos = 0;
									p.len = b.length;
									p.hashloc = new byte[8];
									p.hash = hash;
									p.data = b;
									ar.add(p);
									runnable.setAR(ar);
								} catch (BufferClosedException e) {
									
								} finally {
									SparseDedupFile.hashPool.returnObject(hc);
								}
							}
						} else {
							for (int i = 0; i < ts; i++) {
								WritableCacheBuffer writeBuffer = tasks.get(i);
								writeBuffer.startClose();
								VariableHashEngine hc = (VariableHashEngine) SparseDedupFile.hashPool
										.borrowObject();
								List<Finger> fs = hc.getChunks(writeBuffer
										.getFlushedBuffer());
								int _pos = 0;
								ArrayList<HashLocPair> ar = new ArrayList<HashLocPair>();
								for (Finger f : fs) {
									HashLocPair p = new HashLocPair();
									p.hash = f.hash;
									p.hashloc = f.hl;
									p.len = f.len;
									p.offset = 0;
									p.nlen = f.len;
									p.data = f.chunk;
									p.pos = _pos;
									_pos += f.chunk.length;
									ar.add(p);
								}
								writeBuffer.setAR(ar);
							}
						}
						ArrayList<HashLocPair> al = new ArrayList<HashLocPair>();
						
						for(int i = 0; i < tasks.size();i++) {
							WritableCacheBuffer ck = tasks.get(i);
							if(ck == null)
								break;
							else
								al.addAll(ck.getFingers());
						}
						
						HCServiceProxy.batchHashExists(al);
						for (int i = 0; i < ts; i++) {
							WritableCacheBuffer runnable = tasks.get(i);
							if (runnable != null) {

									try {
										runnable.endClose();
									} catch (Exception e) {
										SDFSLogger.getLog().warn(
												"unable to close block", e);
									}
							}
						}
					}
				} else {
					Thread.sleep(5);
				}

			} catch (Exception e) {
				SDFSLogger.getLog().fatal("unable to execute thread", e);
			}
		}
	}

	private ReentrantLock exitLock = new ReentrantLock();

	public void start() {
		th = new Thread(this);
		th.start();
	}

	public void exit() {
		exitLock.lock();
		try {
			isStopped = true;

			th.interrupt(); // break pool thread out of dequeue() call.
		} catch (Exception e) {
		} finally {
			exitLock.unlock();
		}
	}

	public boolean isStopped() {
		return isStopped;
	}

}
