package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.primitives.Longs;

public class GetCloudFile implements Runnable {

	MetaDataDedupFile mf = null;
	MetaDataDedupFile sdf = null;
	SparseDedupFile sdd = null;

	File df = null;
	SDFSEvent fevt = null;

	public Element getResult(String file, String dstfile, boolean overwrite) throws IOException {

		fevt = SDFSEvent.cfEvent(file);
		if (dstfile != null && file.contentEquals(dstfile) && !overwrite)
			throw new IOException("local filename in the same as source name");

		File df = null;
		if (dstfile != null)
			df = new File(Main.volume.getPath() + File.separator + dstfile);
		if (!overwrite && df != null && df.exists())
			throw new IOException(dstfile + " already exists");
		try {
			Document doc = XMLUtils.getXMLDoc("cloudfile");
			Element root = doc.getDocumentElement();
			File f = new File(Main.volume.getPath() + File.separator + file);
			if (!overwrite && f.exists() && MetaDataDedupFile.getFile(f.getPath()).isLocalOwner())
				throw new IOException("File [" + file + "] already exists and is owned locally.");
			else {
				fevt.maxCt = 4;
				fevt.curCt = 1;
				fevt.shortMsg = "Downloading [" + file + "]";
				mf = FileReplicationService.getMF(file);
				mf.setLocalOwner(false);
				fevt.shortMsg = "Downloading Map Metadata for [" + file + "]";
				LongByteArrayMap ddb = null;
				FileReplicationService.getDDB(mf.getDfGuid());
				if (df != null) {
					sdf = mf.snapshot(df.getPath(), overwrite, fevt);
					sdd = sdf.getDedupFile(false);
					DedupFileChannel ch = sdd.getChannel(-1);
					ddb = (LongByteArrayMap) sdd.bdb;
					sdf.toXML(doc);
					sdd.unRegisterChannel(ch, -1);

				} else {
					sdd = mf.getDedupFile(false);
					DedupFileChannel ch = sdd.getChannel(-1);
					ddb = (LongByteArrayMap) sdd.bdb;
					mf.toXML(doc);
					sdd.unRegisterChannel(ch, -1);
				}
				if (ddb.getVersion() < 3)
					throw new IOException("only files version 3 or later can be imported");

				Thread th = new Thread(this);
				th.start();
			}
			return (Element) root.cloneNode(true);
		} catch (IOException e) {

			if (sdf != null) {
				MetaFileStore.removeMetaFile(sdf.getPath(), true, true);
			}
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get file " + file, e);
			fevt.endEvent("unable to get file " + file, SDFSEvent.ERROR);
			throw new IOException("request to fetch attributes failed because " + e.toString());

		}
	}

	private void checkDedupFile(SparseDedupFile sdb, SDFSEvent fevt) throws IOException {
		fevt.shortMsg = "Importing hashes for file";
		//SDFSLogger.getLog().info("Importing " + sdb.getGUID());
		Set<Long> blks = new HashSet<Long>();
		DedupFileChannel ch = sdb.getChannel(-1);
		LongByteArrayMap ddb = (LongByteArrayMap) sdb.bdb;
		mf.getIOMonitor().clearFileCounters(false);
		if (ddb.getVersion() < 3)
			throw new IOException("only files version 3 or later can be imported");
		try {
			ddb.iterInit();
			for (;;) {
				LongKeyValue kv = ddb.nextKeyValue(Main.refCount);
				if (kv == null)
					break;
				SparseDataChunk ck = kv.getValue();
				boolean dirty = false;
				TreeMap<Integer, HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
					ChunkData cm = new ChunkData(Longs.fromByteArray(p.hashloc), p.hash);
					InsertRecord ir = HCServiceProxy.getHashesMap().put(cm, false);
					mf.getIOMonitor().addVirtualBytesWritten(p.nlen, false);
					if (ir.getInserted()) {
						mf.getIOMonitor().addActualBytesWritten(p.nlen, false);
						blks.add(Longs.fromByteArray(ir.getHashLocs()));
					} else {
						mf.getIOMonitor().addDulicateData(p.nlen, false);
						if (!Arrays.equals(p.hashloc, ir.getHashLocs())) {
							SDFSLogger.getLog().info("z " + Longs.fromByteArray( ir.getHashLocs()) + " " +Longs.fromByteArray( p.hashloc) );
							p.hashloc = ir.getHashLocs();
							blks.add(Longs.fromByteArray(ir.getHashLocs()));
							dirty = true;
						}
					}
					
					
				}
				if (dirty)
					ddb.put(kv.getKey(), ck);
			}
			//SDFSLogger.getLog().info("new objects of size " + blks.size());
			for (Long l : blks) {
				SDFSLogger.getLog().info("importing " + l);
				HashBlobArchive.claimBlock(l);
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().warn("error while checking file [" + ddb + "]", e);
			throw new IOException(e);
		} finally {
			sdd.unRegisterChannel(ch, -1);
		}
	}

	@Override
	public void run() {
		try {
			this.checkDedupFile(sdd, fevt);
			fevt.endEvent("imported [" + mf.getPath() + "]");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to process file " + mf.getPath(), e);
			fevt.endEvent("unable to process file " + mf.getPath(), SDFSEvent.ERROR);
		} finally {
			if (df != null && mf != null) {
				MetaFileStore.removeMetaFile(mf.getPath(), true, true);
			}
		}

	}

}
