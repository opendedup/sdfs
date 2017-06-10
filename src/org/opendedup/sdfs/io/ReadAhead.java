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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.notification.ReadAheadEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.primitives.Longs;


public class ReadAhead implements Runnable {
	SparseDedupFile df;
	ReadAheadEvent evt = null;
	boolean closeWhenDone;
	private DedupFileChannel ch = null;
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(16,
			Main.readAheadThreads, 10, TimeUnit.SECONDS, worksQueue, new ThreadPoolExecutor.CallerRunsPolicy());
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
			SDFSLogger.getLog().debug("initiating readahead for " + df.mf.getPath());
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
	
	public static void readAhead() {}
	private static AtomicInteger ct = new AtomicInteger();

	private static class CacheChunk implements Runnable {
		long pos;

		public void run() {
			try {
				int vt = ct.incrementAndGet();
				SDFSLogger.getLog().debug("active gets is " +vt);
				HCServiceProxy.cacheData(pos);
			} catch (IOException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + pos + "] ", e);
			}  catch (DataArchivedException e) {
				SDFSLogger.getLog()
						.debug("error caching chunk [" + pos + "] ", e);
			}
			ct.decrementAndGet();
		}
	}

	@Override
	public void run() {
		try {
			LongByteArrayMap mp =(LongByteArrayMap) df.bdb;
			mp.iterInit();
			Set<Long> blks = new LinkedHashSet<Long>();
			for (;;) {
				LongKeyValue kv = mp.nextKeyValue(false);
				if (kv == null)
					break;
				SparseDataChunk ck = kv.getValue();
				TreeMap<Integer,HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
					long pos = Longs.fromByteArray(p.hashloc);
					if(pos >100 || pos <-100) {
						blks.add(pos);
					}
				}
			}
			for(Long l : blks) {
				CacheChunk ck = new CacheChunk();
				ck.pos = l;
				executor.execute(ck);
			}
			if(evt.maxCt==0)
				evt.maxCt=1;
			evt.endEvent(df.getMetaFile().getPath() + " Cached");
		} catch (IOException e) {
			SDFSLogger.getLog().warn("unable to cache " +df.mf.getPath(),e);
		} catch (FileClosedException e) {
			SDFSLogger.getLog().warn("unable to cache " +df.mf.getPath(),e);
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
