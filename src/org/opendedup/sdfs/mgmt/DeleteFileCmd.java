package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.LRUCache;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DeleteFileCmd {
	static LRUCache<String, String> ck = new LRUCache<String, String>(50);

	public String getResult(String cmd, String file, String changeid, boolean rmlock) throws IOException {
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
		}
		if (file.contains(".."))
			throw new IOException("requeste file " + file + " does not exist");
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist at " + f.getPath());
		else {
			if (rmlock)
				MetaFileStore.getMF(f).clearRetentionLock();
			boolean removed = MetaFileStore.removeMetaFile(internalPath, true, true);
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
