package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;

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
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.LRUCache;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.primitives.Longs;

public class GetCloudFile implements Runnable {

	MetaDataDedupFile mf = null;
	MetaDataDedupFile sdf = null;
	String sfile, dstfile;
	boolean overwrite;
	private Object obj = null;
	SDFSEvent fevt = null;
	static LRUCache<String, String> ck = new LRUCache<String, String>(500);
	public static LRUCache<String, Object> fack = new LRUCache<String, Object>(50);

	public Element getResult(String file, String dstfile, boolean overwrite, String changeid) throws IOException {
		synchronized (ck) {
			if (changeid != null && ck.containsKey(changeid)) {
				try {
					SDFSLogger.getLog().debug("ignoring " + changeid + " " + file);
					Document doc = XMLUtils.getXMLDoc("cloudfile");
					Element root = doc.getDocumentElement();
					root.setAttribute("action", "ignored");
					return (Element) root.cloneNode(true);
				} catch (Exception e) {
					throw new IOException(e);
				}

			}
			ck.put(changeid, file);
			if (fack.containsKey(file)) {
				obj = fack.get(file);
			} else {
				obj = new Object();
				fack.put(file, obj);
			}
		}
		this.sfile = file;
		this.dstfile = dstfile;
		this.overwrite = overwrite;
		fevt = SDFSEvent.cfEvent(file);
		Thread th = new Thread(this);
		th.start();
		try {
			return fevt.toXML();
		} catch (ParserConfigurationException e) {
			throw new IOException(e);
		}

	}

	private void downloadFile() throws IOException {
		if (dstfile != null && sfile.contentEquals(dstfile) && !overwrite)
			throw new IOException("local filename in the same as source name");

		File df = null;
		if (dstfile != null)
			df = new File(Main.volume.getPath() + File.separator + dstfile);
		if (!overwrite && df != null && df.exists())
			throw new IOException(dstfile + " already exists");
		try {
			File f = new File(Main.volume.getPath() + File.separator + sfile);
			if (!overwrite && f.exists() && MetaDataDedupFile.getFile(f.getPath()).isLocalOwner())
				throw new IOException("File [" + sfile + "] already exists and is owned locally.");
			else {
				MetaFileStore.removedCachedMF(new File(Main.volume.getPath() + File.separator + sfile).getPath());
				if (df != null) {
					MetaFileStore.removedCachedMF(df.getPath());
				}
				if (f.exists()) {
					if (df == null) {
						MetaFileStore.removeMetaFile(new File(Main.volume.getPath() + File.separator + sfile).getPath(),
								true, true,false);
						SDFSLogger.getLog()
								.debug("Removed " + new File(Main.volume.getPath() + File.separator + sfile).getPath());
					}
					try {
						MetaFileStore.getMF(f);
						MetaFileStore.getMF(f).clearRetentionLock();
					} catch (Exception e) {
						SDFSLogger.getLog().warn("File [" + f.getPath() + "] retention lock could not be removed ", e);
					}
					boolean removed = MetaFileStore.removeMetaFile(f.getPath(), true, true,false);
					SDFSLogger.getLog().info("removed " + f.getPath() + " success=" + removed);

					if (removed) {
						SDFSEvent.deleteFileEvent(f);

					} else {
						SDFSEvent.deleteFileFailedEvent(f);
					}
				}
				fevt.maxCt = 3;
				fevt.curCt = 1;
				SDFSLogger.getLog().debug("downloading " + sfile);
				fevt.shortMsg = "Downloading [" + sfile + "]";
				MetaDataDedupFile _mf = FileReplicationService.getMF(sfile);
				SDFSLogger.getLog().debug("downloaded " + sfile);
				fevt.shortMsg = "Downloading Map Metadata for [" + sfile + "]";
				SDFSLogger.getLog().debug("downloading ddb " + _mf.getDfGuid() + " lf=" + _mf.getLookupFilter());
				if(_mf.getDfGuid() == null) {
					throw new IOException("File " + sfile + " has no data");
				}
				FileReplicationService.getDDB(_mf.getDfGuid(), _mf.getLookupFilter());
				mf = MetaFileStore.getMF(_mf.getPath());
				mf.setLocalOwner(false);
				SDFSLogger.getLog().info("downloaded ddb " + mf.getDfGuid());
				if (df != null) {
					sdf = mf.snapshot(df.getPath(), overwrite, fevt);
				} else {
					SDFSLogger.getLog().info("checking dedupe file " + sfile + " sdd=" + mf.getDfGuid());

				}
				fevt.curCt++;

			}
		} catch (IOException e) {

			if (sdf != null) {
				sdf.deleteStub(true);
			}
			/*
			File f = new File(Main.volume.getPath() + File.separator + sfile);
			if (f.exists()) {
				try {
					MetaFileStore.removeMetaFile(f.getPath(), true, true,false);
				} catch (Exception e1) {

				} finally {
					f.delete();
				}
			}*/
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get file " + sfile, e);
			fevt.endEvent("unable to get file " + sfile, SDFSEvent.ERROR);
			throw new IOException("request to fetch attributes failed because " + e.toString());

		}
	}

	private void checkDedupFile(SDFSEvent fevt) throws IOException {

		fevt.shortMsg = "Importing hashes for file";
		SDFSLogger.getLog().info("Importing " + mf.getDfGuid());
		Set<Long> blks = new HashSet<Long>();
		LongByteArrayMap ddb = LongByteArrayMap.getMap(mf.getDfGuid(), mf.getLookupFilter());
		ddb.forceClose();
		ddb = LongByteArrayMap.getMap(mf.getDfGuid(), mf.getLookupFilter());
		mf.getIOMonitor().clearFileCounters(false);
		if (ddb.getVersion() < 2)
			throw new IOException("only files version 2 or later can be imported");
		try {
			long ct = 0;
			ddb.iterInit();
			for (;;) {
				LongKeyValue kv = ddb.nextKeyValue(false);
				ct++;
				if (kv == null)
					break;
				SparseDataChunk ck = kv.getValue();
				boolean dirty = false;
				TreeMap<Integer, HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
					ChunkData cm = new ChunkData(Longs.fromByteArray(p.hashloc), p.hash);
					cm.references = 1;
					InsertRecord ir = HCServiceProxy.getHashesMap().put(cm, false);
					mf.getIOMonitor().addVirtualBytesWritten(p.nlen, false);
					if (ir.getInserted()) {
						mf.getIOMonitor().addActualBytesWritten(p.nlen, false);
						blks.add(Longs.fromByteArray(ir.getHashLocs()));
					} else {
						mf.getIOMonitor().addDulicateData(p.nlen, false);
						if (!Arrays.equals(p.hashloc, ir.getHashLocs())) {
							SDFSLogger.getLog().debug("importing " + Longs.fromByteArray( ir.getHashLocs()) + " "
							 +Longs.fromByteArray( p.hashloc) );
							p.hashloc = ir.getHashLocs();
							blks.add(Longs.fromByteArray(ir.getHashLocs()));
							dirty = true;
						}
					}

				}
				if (dirty)
					ddb.put(kv.getKey(), ck);
			}

			SDFSLogger.getLog().info("new objects of size " + blks.size() + " iter count is " + ct);
			for (Long l : blks) {
				SDFSLogger.getLog().debug("importing " + l);
				HashBlobArchive.claimBlock(l);
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().warn("error while checking file [" + ddb + "]", e);
			throw new IOException(e);
		} finally {
			try {
				ddb.forceClose();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing file [" + mf.getPath() + "]", e);
			}

		}
		SDFSLogger.getLog().info("Done Importing " + mf.getDfGuid());
		fevt.curCt++;
	}

	@Override
	public void run() {
		try {
			synchronized (obj) {
				this.downloadFile();
				this.checkDedupFile(fevt);
				fevt.endEvent("imported [" + mf.getPath() + "]");
			}
		} catch (Exception e) {
			String pth = "";
			if (mf != null)
				pth = mf.getPath();
			try {
				File f = new File(Main.volume.getPath() + File.separator + sfile);
				if (f.exists()) {
					
						f.delete();
				}
			} catch (Exception e1) {

			}
			SDFSLogger.getLog().error("unable to process file " + pth, e);
			fevt.endEvent("unable to process file " + pth, SDFSEvent.ERROR);
		}

	}

}