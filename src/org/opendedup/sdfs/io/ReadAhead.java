/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.io;

import java.io.IOException;




import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.notification.ReadAheadEvent;



public class ReadAhead {
	SparseDedupFile df;
	ReadAheadEvent evt = null;
	boolean closeWhenDone;
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(1,
			Main.readAheadThreads, 10, TimeUnit.SECONDS, worksQueue);
	public HashMap<String, ReadAhead> active = new HashMap<String, ReadAhead>();
	private long ep;
	private long sp;
	private int readAheadBuffers = Main.readAheadThreads;
	//private static int MAX_READAHEAD_BUFFERS = Main.readAheadThreads;

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
			SDFSLogger.getLog().debug("initiating readahead for " + df.mf.getPath());
			if(active.containsKey(df.getMetaFile().getPath()))
				return;
			
			this.df = df;
			active.put(df.mf.getPath(), this);
			this.evt = new ReadAheadEvent(Main.volume.getName(), df.getMetaFile());
			this.evt.maxCt = df.mf.length();
			
		}
	}
	/* Function to identify start of range */
	public synchronized void cacheFromRange(long startPos) {
		this.sp = this.df.getChuckPosition(startPos);
		if(ep >= df.mf.length())
			return;
		long _ep = this.sp + (Main.CHUNK_LENGTH * readAheadBuffers);

		if(_ep > (ep + (2*Main.CHUNK_LENGTH))) {
			while(ep > _ep) {
				try {
				CacheWriteBuffer cb = new CacheWriteBuffer(df,ep);
				Thread th = new Thread(cb);
				executor.execute(th);
				ep +=Main.CHUNK_LENGTH; 
				if(ep >= df.mf.length())
					return;
				}catch(Exception e) {
					SDFSLogger.getLog().warn("while readahead",e);
				}
			}
		}
	}
	
	public static void readAhead() {}

	private static class CacheWriteBuffer implements Runnable {
		long pos;
		SparseDedupFile df = null;
		
		public CacheWriteBuffer(SparseDedupFile df, long pos) {
			this.df = df;
			this.pos = pos;
		}
		
		public void run() {
			try {
				SDFSLogger.getLog().debug("active gets is " + df.mf.getPath() + " pos = " + pos);
				WritableCacheBuffer bf = (WritableCacheBuffer) df.getWriteBuffer(pos);
				bf.getReadChunk(0, 1);
			} catch (IOException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + pos + "] ", e);
			}  catch (DataArchivedException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + pos + "] ", e);
			} catch (FileClosedException e) {
				SDFSLogger.getLog()
				.debug("error caching chunk [" + pos + "] ", e);
			} catch (BufferClosedException e) {
				SDFSLogger.getLog()
				.debug("error caching chunk [" + pos + "] ", e);
			}
			
		}
	}

}
