package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.network.AsyncCmdListener;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;

public class DirectWriteHashCmd implements IOClientCmd {
	byte[] hash;
	byte[] aContents;
	int position;
	int len;
	boolean written = false;
	boolean compress = false;
	byte numberOfCopies = 1;
	byte[] resp = new byte[8];
	byte[] ignoredhosts = null;
	private final ReentrantLock lock = new ReentrantLock();
	Object wobj = new Object();
	byte dn = 0;
	byte exdn = 0;
	int pos = 1;
	byte sz = 0;
	private static BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private static RejectedExecutionHandler executionHandler = new BlockPolicy();
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 70,
			10, TimeUnit.SECONDS, worksQueue, executionHandler);
	static {
		executor.allowCoreThreadTimeOut(true);
	}

	public DirectWriteHashCmd(byte[] hash, byte[] aContents, int len,
			boolean compress, byte numberOfCopies) throws IOException {
		this.hash = hash;
		this.compress = compress;
		this.numberOfCopies = numberOfCopies;

		if (compress) {
			throw new IOException("not implemented");
		} else {
			this.aContents = aContents;
			this.len = len;
		}
	}

	public DirectWriteHashCmd(byte[] hash, byte[] aContents, int len,
			boolean compress, byte numberOfCopies, byte[] ignoredHosts)
			throws IOException {
		this.hash = hash;
		this.compress = compress;
		this.numberOfCopies = numberOfCopies;
		this.ignoredhosts = ignoredHosts;
		if (compress) {
			throw new IOException("not implemented");
			/*
			 * try { byte[] compB = CompressionUtils.compress(aContents); if
			 * (compB.length <= aContents.length) { this.aContents = compB;
			 * this.len = this.aContents.length; } else { this.compress = false;
			 * this.aContents = aContents; this.len = len; } } catch
			 * (IOException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); this.aContents = aContents; this.len = len;
			 * this.compress = false; }
			 */
		} else {
			this.aContents = aContents;
			this.len = len;
		}
	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		if (this.numberOfCopies > 7)
			this.numberOfCopies = 7;
		int stateSz = soc.serverState.size();
		if (soc.serverState.size() < this.numberOfCopies)
			this.numberOfCopies = (byte) stateSz;
		try {
			List<HashClientPool> pools = soc.getServerPools(
					this.numberOfCopies, ignoredhosts);

			sz = (byte) pools.size();

			AsyncCmdListener l = new AsyncCmdListener() {

				@Override
				public void commandException(Exception e) {
					lock.lock();
					try {
						dn++;
						exdn++;
						if (dn >= sz) {
							synchronized (this) {
								this.notify();
							}
						}
					} finally {
						lock.unlock();
					}

				}

				@Override
				public void commandResponse(Object result, HashClient client) {
					lock.lock();
					try {
						boolean done = (Boolean) result;
						if (!done)
							resp[0] = (byte) 1;
						resp[pos] = client.getId();
						pos++;
						dn++;
						if (dn >= sz) {

							synchronized (this) {
								this.notify();
							}
						}
					} finally {
						lock.unlock();
					}
				}

			};
			for (HashClientPool pool : pools) {
				HashClient hc = pool.borrowObject();
				hc.writeChunkAsync(this.hash, this.aContents, 0,
						this.aContents.length, l);
				executor.execute(hc);
			}
			if (dn < sz) {
				synchronized (l) {
					l.wait(10000);
				}
			}
			if (dn < sz)
				SDFSLogger.getLog().warn(
						"thread timed out before write was complete ");
			if (this.ignoredhosts != null) {
				for (byte bz : ignoredhosts) {
					if (bz != (byte) 0) {
						resp[pos] = bz;
						pos++;
					}
				}
			}
			if (pos == 1)
				throw new IOException("unable to write to any storage nodes");
		} catch (Exception e) {
			// SDFSLogger.getLog().error("error while writing",e);
			throw new IOException(e);
		}
	}

	public byte[] reponse() {
		return this.resp;
	}

	public byte getExDn() {
		return this.exdn;
	}

	public byte getDn() {
		return this.dn;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.WRITE_HASH_CMD;
	}

	public static class BlockPolicy implements RejectedExecutionHandler {

		/**
		 * Creates a <tt>BlockPolicy</tt>.
		 */
		public BlockPolicy() {
		}

		/**
		 * Puts the Runnable to the blocking queue, effectively blocking the
		 * delegating thread until space is available.
		 * 
		 * @param r
		 *            the runnable task requested to be executed
		 * @param e
		 *            the executor attempting to execute this task
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
			try {
				e.getQueue().put(r);
			} catch (InterruptedException e1) {
				SDFSLogger
						.getLog()
						.error("Work discarded, thread was interrupted while waiting for space to schedule: {}",
								e1);
			}
		}
	}

}
