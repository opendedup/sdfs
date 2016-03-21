package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.primitives.Longs;

public class GetCloudFile {

	public Element getResult(String file, String dstfile) throws IOException {
		MetaDataDedupFile mf = null;
		MetaDataDedupFile sdf = null;
		LongByteArrayMap ddb = null;
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
			ddb = FileReplicationService.getDDB(mf.getDfGuid());
			if (ddb.getVersion() < 3)
				throw new IOException(
						"only files version 3 or later can be imported");
			checkDedupFile(ddb, fevt);
			if (df != null) {
				sdf = mf.snapshot(df.getPath(), false, fevt);
				sdf.toXML(doc);
			} else {
				mf.toXML(doc);
			}
			fevt.endEvent("imported [" + file + "]");
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

		} finally {
			if (mf != null) {
				MetaFileStore.removeMetaFile(mf.getPath(), true);
			}
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
						p.hashloc = ir.getHashLocs();
						dirty = true;
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

}
