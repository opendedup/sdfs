package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bouncycastle.util.Arrays;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
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
	LongByteArrayMap ddb = null;
	
	File df = null;
	SDFSEvent fevt = null;
	public Element getResult(String file, String dstfile) throws IOException {
		
		SDFSEvent fevt = SDFSEvent.cfEvent(file);
		if (dstfile != null && file.contentEquals(dstfile))
			throw new IOException("local filename in the same as source name");

		File df = null;
		if (dstfile != null)
			df = new File(Main.volume.getPath() + File.separator + dstfile);
		if (df != null && df.exists())
			throw new IOException(dstfile + " already exists");
		try {
			Document doc = XMLUtils.getXMLDoc("cloudfile");
			Element root = doc.getDocumentElement();
			File f = new File(Main.volume.getPath() + File.separator + file);
			if (f.exists()
					&& MetaDataDedupFile.getFile(f.getPath()).isLocalOwner())
				throw new IOException("File [" + file
						+ "] already exists and is owned locally.");
			fevt.maxCt = 4;
			fevt.curCt = 1;
			fevt.shortMsg = "Downloading [" + file + "]";
			mf = FileReplicationService.getMF(file);
			mf.setLocalOwner(false);
			fevt.shortMsg = "Downloading Map Metadata for [" + file + "]";
			FileReplicationService.getDDB(mf.getDfGuid());
			if (df != null) {
				sdf = mf.snapshot(df.getPath(), false, fevt);
				SparseDedupFile sdd = sdf.getDedupFile(false);
				DedupFileChannel ch = sdd.getChannel(-1);
				ddb = (LongByteArrayMap)sdd.bdb;
				sdf.toXML(doc);
				sdd.unRegisterChannel(ch, -1);
				
			} else {
				SparseDedupFile sdd = mf.getDedupFile(false);
				DedupFileChannel ch = sdd.getChannel(-1);
				ddb = (LongByteArrayMap)sdd.bdb;
				mf.toXML(doc);
				sdd.unRegisterChannel(ch, -1);
			}
			if (ddb.getVersion() < 3)
				throw new IOException(
						"only files version 3 or later can be imported");
			
			Thread th = new Thread(this);
			th.start();
			return (Element) root.cloneNode(true);
		} catch (IOException e) {

			if (sdf != null) {
				MetaFileStore.removeMetaFile(sdf.getPath(), true);
			}
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get file " + file, e);
			fevt.endEvent("unable to get file " + file, SDFSEvent.ERROR);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());

		} 
	}

	private void checkDedupFile(LongByteArrayMap ddb, SDFSEvent fevt)
			throws IOException {
		fevt.shortMsg = "Importing hashes for file";
		Set<Long> blks = new HashSet<Long>();
		if (ddb.getVersion() < 3)
			throw new IOException(
					"only files version 3 or later can be imported");
		try {
			ddb.iterInit();
			for (;;) {
				LongKeyValue kv = ddb.nextKeyValue();
				if (kv == null)
					break;
				SparseDataChunk ck = new SparseDataChunk(kv.getValue(),
						ddb.getVersion());
				boolean dirty = false;
				List<HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al) {

					ChunkData cm = new ChunkData(
							Longs.fromByteArray(p.hashloc), p.hash);
					InsertRecord ir = HCServiceProxy.getHashesMap().put(cm,
							false);
					if (ir.getInserted())
						blks.add(Longs.fromByteArray(ir.getHashLocs()));
					else {
						if(!Arrays.areEqual(p.hashloc, ir.getHashLocs())) {
							p.hashloc = ir.getHashLocs();
							dirty = true;
						}
					}
				}
				if (dirty)
					ddb.put(kv.getKey(), ck.getBytes());
			}
			for (Long l : blks) {
				HashBlobArchive.claimBlock(l);
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().warn("error while checking file [" + ddb + "]",
					e);
			throw new IOException(e);
		} finally {
			ddb.close();
			ddb = null;
		}
	}

	@Override
	public void run() {
		try {
			this.checkDedupFile(ddb, fevt);
			fevt.endEvent("imported [" + mf.getPath() + "]");
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to process file " + mf.getPath(), e);
			fevt.endEvent("unable to process file " + mf.getPath(), SDFSEvent.ERROR);
		} finally {
			if (df != null && mf != null) {
				MetaFileStore.removeMetaFile(mf.getPath(), true);
			}
		}
		
	}

}
