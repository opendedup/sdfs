package org.opendedup.sdfs.io;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public  class ReadAhead  implements Runnable {
	SparseDedupFile df;
	
	private static transient BlockingQueue<Runnable> worksQueue = new LinkedBlockingQueue<Runnable>(
			2);
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(
			Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS,
			worksQueue, executionHandler);
	private static LoadingCache<SparseDedupFile, ReadAhead> readAheads = CacheBuilder
			.newBuilder().maximumSize(Main.writeThreads + 1)
			.concurrencyLevel(Main.writeThreads).expireAfterAccess(30, TimeUnit.MINUTES)
			.build(new CacheLoader<SparseDedupFile, ReadAhead>() {
				public ReadAhead load(SparseDedupFile key) throws IOException,
						FileClosedException {
					return new ReadAhead(key);
				}

			});
	
	public static ReadAhead getReadAhead(SparseDedupFile df) throws ExecutionException {
		return readAheads.get(df);
	}
	
	private ReadAhead(SparseDedupFile df) {
		this.df = df;
		Thread th = new Thread(this);
		th.start();
	}
	
	private static class CacheChunk implements Runnable {
		WritableCacheBuffer buf = null;
		SparseDedupFile df = null;
		public void run() {
			try {
				if(!df.isClosed()) {
					buf.cacheChunk();
				}
			} catch (IOException e) {
				SDFSLogger.getLog().error("error caching chunk [" +buf.getFilePosition() +"] in " + df.getDatabasePath(),e);
			} catch (InterruptedException e) {
				SDFSLogger.getLog().error("error caching chunk [" +buf.getFilePosition() +"] in " + df.getDatabasePath(),e);
			} catch (DataArchivedException e) {
				SDFSLogger.getLog().error("error caching chunk [" +buf.getFilePosition() +"] in " + df.getDatabasePath(),e);
			}
		}
	}

	@Override
	public void run() {
		long i = 0;
		while(i< df.mf.length()) {
			try {
				WritableCacheBuffer buf = (WritableCacheBuffer)df.getWriteBuffer(i);
				CacheChunk ck = new CacheChunk();
				ck.buf = buf;
				ck.df = df;
				executor.execute(ck);
				i = buf.getEndPosition();
			} catch (IOException e) {
				SDFSLogger.getLog().error("error caching chunk [" +i +"] in " + df.getDatabasePath(),e);
				break;
			} catch (FileClosedException e) {
				SDFSLogger.getLog().error("error caching chunk [" +i +"] in " + df.getDatabasePath(),e);
				break;
			}
		}
		
	}
	
	

}
