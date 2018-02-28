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
package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;

import com.google.common.primitives.Longs;

public class RestoreArchive implements Runnable {
	private AtomicLong totalArchives = new AtomicLong(0);
	private AtomicLong importedArchives = new AtomicLong(0);
	public SDFSEvent fEvt = null;
	private MetaDataDedupFile f = null;
	private HashMap<String, String> restoreRequests = new HashMap<String, String>();

	public RestoreArchive(MetaDataDedupFile f,long id) throws IOException {
		
		this.f = f;
		fEvt = SDFSEvent.archiveRestoreEvent(f);
		fEvt.maxCt = FileCounts.getSize(new File(f.getPath()), false);
		if(id != -1) {
			String req = HCServiceProxy.restoreBlock(new byte [16],id);
			if (req != null) {
				SDFSLogger.getLog().info("will restore " + req + " for " + f.getPath());
				this.fEvt.maxCt++;
				this.totalArchives.incrementAndGet();
			}
			this.restoreRequests.put(Long.toString(id), req);
		}
	}

	private void init() throws IOException {
		SDFSLogger.getLog().info("Starting Archive Restore for " + f.getPath());
		try {
			File directory = new File(Main.dedupDBStore + File.separator + this.f.getDfGuid().substring(0, 2)
					+ File.separator + this.f.getDfGuid());
			File dbf = new File(directory.getPath() + File.separator + this.f.getDfGuid() + ".map");
			if (!dbf.exists())
				dbf = new File(directory.getPath() + File.separator + this.f.getDfGuid() + ".map.lz4");
			this.initiateArchive();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while restoring file [" + f.getPath() + "]", e);
		}
	}

	public static void recoverArchives(MetaDataDedupFile f) throws IOException {
		RestoreArchive ar = new RestoreArchive(f,-1);
		Thread th = new Thread(ar);
		th.start();
		while (!ar.fEvt.isDone()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
		if (ar.fEvt.level != SDFSEvent.INFO) {
			throw new IOException("unable to restore all archived data for " + f.getPath());
		}
	}

	private void initiateArchive() throws IOException {
		LongByteArrayMap ddb = LongByteArrayMap.getMap(f.getDfGuid(), f.getLookupFilter());
		
		if (ddb.getVersion() < 2)
			throw new IOException("only files version 2 or later can be imported");
		try {
			ddb.iterInit();
			for (;;) {
				LongKeyValue kv = ddb.nextKeyValue(false);
				if (kv == null)
					break;
				SparseDataChunk ck = kv.getValue();
				TreeMap<Integer, HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
		
					Long bw = new Long(Longs.fromByteArray(p.hashloc));
					if (!this.restoreRequests.containsKey(Long.toString(bw))) {
						SDFSLogger.getLog().debug("check = " + bw + " for restore.");
						
						String req = HCServiceProxy.restoreBlock(p.hash,bw);
						if (req != null) {
							SDFSLogger.getLog().info("will restore " + req + " for " + f.getPath());
							this.fEvt.maxCt++;
							this.totalArchives.incrementAndGet();
						}
						this.restoreRequests.put(Long.toString(bw), req);
					} 
				}
				
			}
			SDFSLogger.getLog().info("Restore Initiated for " + this.restoreRequests.size() + " for " + f.getPath());

		} catch (Throwable e) {
			try {
				SDFSLogger.getLog().error("error while restoring file [" + f.getPath() + "]", e);
			} catch (Exception e1) {
				SDFSLogger.getLog().error("error while restoring file ", e);
			}
			// throw new IOException(e);
		} finally {
			ddb.close();
		}
	}

	@Override
	public void run() {

		try {
			long start = System.currentTimeMillis();
			this.init();
			while (this.restoreRequests.size() > 0) {
				ArrayList<String> al = new ArrayList<String>();
				
				for (Entry<String, String> key : this.restoreRequests.entrySet()) {
					try {
						if (key.getValue() == null)
							al.add(key.getKey());
						else {
							SDFSLogger.getLog().info("archive restore will check " + key.getKey());
							if (HCServiceProxy.blockRestored(key.getValue())) {

								al.add(key.getKey());
								this.fEvt.curCt++;
								this.importedArchives.incrementAndGet();
								SDFSLogger.getLog().info("restored " + key.getValue());
							} else {
								SDFSLogger.getLog().info("not restored " + key.getValue());
							}
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error("unable to check restore for " + key.getValue(), e);
					}

				}
				for (String id : al) {
					this.restoreRequests.remove(id);
				}
				al = null;
				if (this.restoreRequests.size() > 0)
					Thread.sleep(2 * 60 * 1000);
			}
			SDFSLogger.getLog().info("took [" + (System.currentTimeMillis() - start) / (1000 * 60)
					+ "] Minutes to import [" + totalArchives.get() + "]");
			fEvt.endEvent("Archive Restore Succeeded for " + f.getPath());
		} catch (Exception e) {
			SDFSLogger.getLog().error("archive restore failed", e);
			fEvt.endEvent("Archive Restore failed because [" + e.toString() + "]", SDFSEvent.ERROR);
		}
	}

	public SDFSEvent getEvent() {
		return this.fEvt;
	}

}
