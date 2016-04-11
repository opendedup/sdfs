package org.opendedup.sdfs.io;

import java.io.IOException;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.ReadAheadEvent;


public class ReadAhead implements Runnable {
	SparseDedupFile df;
	ReadAheadEvent evt = null;
	boolean closeWhenDone;
	private DedupFileChannel ch = null;
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads * 4,
			Main.writeThreads * 4, 10, TimeUnit.SECONDS, worksQueue, executionHandler);
	public HashMap<String, ReadAhead> active = new HashMap<String, ReadAhead>();

	public static ReadAhead getReadAhead(SparseDedupFile df) throws ExecutionException, IOException {
		if (Main.readAhead)
			return new  ReadAhead(df,false);
		else
			throw new IOException("ReadAhead disabled");
	}

	public static ReadAhead getReadAhead(MetaDataDedupFile mf) throws ExecutionException, IOException {
		SparseDedupFile df = (SparseDedupFile) DedupFileStore.getDedupFile(mf);
		return new  ReadAhead(df,true);
	}

	public ReadAhead(SparseDedupFile df, boolean closeWhenDone) throws IOException {
		synchronized (active) {
			if(active.containsKey(df.getMetaFile().getPath()))
				return;
			if (closeWhenDone) {
				this.ch = df.getChannel(-1);
			}
			this.df = df;
			active.put(df.mf.getPath(), this);
			this.evt = new ReadAheadEvent(Main.volume.getName(), df.getMetaFile());
			this.evt.maxCt = df.mf.length();
			Thread th = new Thread(this);
			th.start();
		}
	}

	private static class CacheChunk implements Runnable {
		WritableCacheBuffer buf = null;
		SparseDedupFile df = null;

		public void run() {
			try {
				if (!df.isClosed()) {
					buf.cacheChunk();
				}
			} catch (IOException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + buf.getFilePosition() + "] in " + df.getDatabasePath(), e);
			} catch (InterruptedException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + buf.getFilePosition() + "] in " + df.getDatabasePath(), e);
			} catch (DataArchivedException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + buf.getFilePosition() + "] in " + df.getDatabasePath(), e);
			}
		}
	}

	@Override
	public void run() {
		long i = 0;
		try {
			while (i < df.mf.length()) {
				try {
					WritableCacheBuffer buf = (WritableCacheBuffer) df.getWriteBuffer(i);
					CacheChunk ck = new CacheChunk();
					ck.buf = buf;
					ck.df = df;
					executor.execute(ck);
					i = buf.getEndPosition();
					evt.curCt = i;
				} catch (IOException e) {
					SDFSLogger.getLog().debug("error caching chunk [" + i + "] in " + df.getDatabasePath(), e);
					break;
				} catch (FileClosedException e) {
					SDFSLogger.getLog().debug("error caching chunk [" + i + "] in " + df.getDatabasePath(), e);
					break;
				}
			}
			evt.endEvent(df.getMetaFile().getPath() + " Cached");
		} finally {
			try {
			if (ch != null)
				df.unRegisterChannel(ch, -1);
			}catch(Exception e) {}
			
			try {
				synchronized (active) {
					active.remove(df.mf.getPath());
				}
			}catch(Exception e) {}
		}

	}

}
