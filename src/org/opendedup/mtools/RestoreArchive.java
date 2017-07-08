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
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;

public class RestoreArchive implements Runnable {
	private AtomicLong totalArchives = new AtomicLong(0);
	private AtomicLong importedArchives = new AtomicLong(0);
	public SDFSEvent fEvt = null;
	private MetaDataDedupFile f = null;
	HashSet<String> restoreRequests = new HashSet<String>();

	public RestoreArchive(MetaDataDedupFile f) throws IOException {
		this.f = f;
		fEvt = SDFSEvent.archiveRestoreEvent(f);
		fEvt.maxCt = FileCounts.getSize(new File(f.getPath()), false);
	}

	private void init() throws IOException {
		SDFSLogger.getLog().info("Starting Archive Restore for " + f.getPath());

		File directory = new File(Main.dedupDBStore + File.separator
				+ this.f.getDfGuid().substring(0, 2) + File.separator
				+ this.f.getDfGuid());
		File dbf = new File(directory.getPath() + File.separator
				+ this.f.getDfGuid() + ".map");
		if(!dbf.exists())
			 dbf = new File(directory.getPath() + File.separator
						+ this.f.getDfGuid() + ".map.lz4");
		this.initiateArchive(dbf);
	}

	public static void recoverArchives(MetaDataDedupFile f) throws IOException {
		RestoreArchive ar = new RestoreArchive(f);
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
			throw new IOException("unable to restore all archived data for "
					+ f.getPath());
		}
	}

	private void initiateArchive(File mapFile) throws IOException {
		DataMapInterface mp = null;
		try {
			if(mapFile.getName().endsWith(".lz4"))
				mp = LongByteArrayMap.getMap(mapFile.getName().substring(0, mapFile.getName().length()-8));
			else
				mp = LongByteArrayMap.getMap(mapFile.getName().substring(0, mapFile.getName().length()-4));
			mp.iterInit();
			SparseDataChunk ck= mp.nextValue(false);
			while (ck != null) {
				
				TreeMap<Integer,HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
						String req = HCServiceProxy.restoreBlock(p.hash);
						if (req != null && !this.restoreRequests.contains(req)) {
							this.restoreRequests.add(req);
							SDFSLogger.getLog().info("will restore " + req);
							this.fEvt.maxCt++;
							this.totalArchives.incrementAndGet();
						}
					}
					ck = mp.nextValue(false);
			}
			SDFSLogger.getLog().info("Restore Initiated for " + this.restoreRequests.size());
			
		} catch (Throwable e) {
			SDFSLogger.getLog().error(
					"error while restoring file [" + f.getPath() + "]", e);
			// throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
	}

	@Override
	public void run() {
		try {
			long start = System.currentTimeMillis();
			this.init();
			while (this.restoreRequests.size() > 0) {
				ArrayList<String> al = new ArrayList<String>();
				for (String id : this.restoreRequests) {
					try {
						SDFSLogger.getLog().debug("will check " + id);
						if (HCServiceProxy.blockRestored(id)) {

							al.add(id);
							this.fEvt.curCt++;
							this.importedArchives.incrementAndGet();
							SDFSLogger.getLog().info("restored " + id);
						} else {
							SDFSLogger.getLog().info("not restored " + id);
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"unable to check restore for " + id, e);
					}

				}
				for (String id : al) {
					this.restoreRequests.remove(id);
				}
				al = null;
				if (this.restoreRequests.size() > 0)
					Thread.sleep(2 * 60 * 1000);
			}
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start)
							/ (1000 * 60) + "] Minutes to import ["
							+ totalArchives.get() + "]");
			fEvt.endEvent("Archive Restore Succeeded for " + f.getPath());
		} catch (Exception e) {
			SDFSLogger.getLog().error("archive restore failed", e);
			fEvt.endEvent("Archive Restore failed because [" + e.toString()
					+ "]", SDFSEvent.ERROR);
		}
	}

	public SDFSEvent getEvent() {
		return this.fEvt;
	}

}
