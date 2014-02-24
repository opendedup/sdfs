package org.opendedup.hashing;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.QuickList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BufferClosedException;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class PoolThread implements AbstractPoolThread, Runnable {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private boolean isStopped = false;
	private int maxTasks = ((Main.maxWriteBuffers * 1024 * 1024) / Main.CHUNK_LENGTH) + 1;
	private final QuickList<WritableCacheBuffer> tasks = new QuickList<WritableCacheBuffer>(
			maxTasks + 20);
	Thread th = null;

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
						QuickList<SparseDataChunk> cks = new QuickList<SparseDataChunk>(ts);
						for (int i = 0; i < ts; i++) {
							WritableCacheBuffer runnable = tasks.get(i);
							runnable.startClose();
							AbstractHashEngine hc = SparseDedupFile.hashPool
									.borrowObject();
							byte[] hash = null;
							try {
								hash = hc.getHash(runnable.getFlushedBuffer());
								SparseDataChunk ck = new SparseDataChunk(0,
										hash, false, new byte[8],(byte)0);
								runnable.setHash(hash);
								cks.add(i,ck);
							} catch (BufferClosedException e) {
								cks.add(i,null);
							} finally {
								SparseDedupFile.hashPool.returnObject(hc);
							}

						}
						HCServiceProxy.batchHashExists(cks);
						for (int i = 0; i < ts; i++) {
							WritableCacheBuffer runnable = tasks.get(i);
							SparseDataChunk ck = cks.get(i);
							if (ck != null) {
								if (Arrays.equals(ck.getHash(),
										runnable.getHash())) {
									runnable.setHashLoc(ck.getHashLoc());
									try {
										runnable.endClose();
									} catch (Exception e) {
										SDFSLogger.getLog().warn(
												"unable to close block", e);
									}
								}
								else {
									SDFSLogger.getLog().fatal(
											"there is a hash mismatch!");
								}
							}
						}
						/*
						try {
						HCServiceProxy.batchWriteHash(tasks);
						}catch (Exception e) {
							SDFSLogger.getLog().warn(
									"unable to run batch", e);
						}
						for (int i = 0; i < ts; i++) {
							WritableCacheBuffer runnable = tasks.get(i);
							try {
								runnable.endClose();
							} catch (Exception e) {
								SDFSLogger.getLog().warn(
										"unable to close block", e);
							}
						}
						*/
						cks = null;
					}
				Thread.sleep(1);
				}
				else {
					Thread.sleep(10);
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
		} catch(Exception e) {}
		finally {
			exitLock.unlock();
		}
	}

	public boolean isStopped() {
		return isStopped;
	}

	
}
