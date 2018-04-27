package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.LRUCache;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DeleteFileCmd {
	static LRUCache<String, String> ck = new LRUCache<String, String>(50);
	private Object obj = null;

	public String getResult(String cmd, String file, String changeid, boolean rmlock, boolean localonly)
			throws IOException {
		synchronized (ck) {
			if (changeid != null && ck.containsKey(changeid)) {
				try {
					SDFSLogger.getLog().info("ignoring " + changeid + " " + file);
					Document doc = XMLUtils.getXMLDoc("cloudmfile");
					Element root = doc.getDocumentElement();
					root.setAttribute("action", "ignored");
					return "already executed " + changeid;
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			ck.put(changeid, file);
			synchronized (GetCloudFile.ck) {
				if (GetCloudFile.fack.containsKey(file)) {
					obj = GetCloudFile.fack.get(file);
				} else {
					obj = new Object();
					GetCloudFile.fack.put(file, obj);
				}
			}
		}
		synchronized (obj) {
			if (file.contains(".."))
				throw new IOException("requeste file " + file + " does not exist");
			String internalPath = Main.volume.getPath() + File.separator + file;
			File f = new File(internalPath);
			SDFSLogger.getLog().info("removing " + internalPath);
			if (!f.exists())
				throw new IOException("requeste file " + file + " does not exist at " + f.getPath());
			else {
				try {
					MetaFileStore.getMF(f);
					if (rmlock) {

						MetaFileStore.getMF(f).clearRetentionLock();

					}
				} catch (Exception e) {
					SDFSLogger.getLog().info("forcing delete of file " + internalPath);
					f.delete();
					MetaFileStore.removedCachedMF(internalPath);
				}
				if (FileReplicationService.service != null) {
					try {
						MetaDataDedupFile mf = MetaFileStore.getMF(f);
						if (mf != null && mf.getDfGuid() != null)
							FileReplicationService.service.sync.deleteFile(mf.getDfGuid().substring(0, 2)
									+ File.separator + mf.getDfGuid() + File.separator + mf.getDfGuid() + ".map",
									"ddb");
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to delete map file from cloud", e);
					}
				}
				boolean removed = MetaFileStore.removeMetaFile(internalPath, localonly, true, true);
				if (FileReplicationService.service != null) {
					try {
						FileReplicationService.service.deleteFile(f);
					} catch (Exception e) {

					}
				}
				SDFSLogger.getLog().info("removed " + internalPath + " success=" + removed);

				if (removed) {
					SDFSEvent.deleteFileEvent(f);
					return "removed [" + file + "]";

				} else {
					SDFSEvent.deleteFileFailedEvent(f);
					return "failed to remove [" + file + "]";
				}
			}
		}
	}

}