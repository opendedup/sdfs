package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.SDFSEvent;

public class FlushBuffersCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + file);
		if (f.isDirectory() && cmd.equalsIgnoreCase("file"))
			throw new IOException(
					"ERROR Flush File Failed : ["
							+ file
							+ "] is a directory. This command cannot be executed on directories");
		else if (cmd.equalsIgnoreCase("file")) {
			try {
				
				MetaFileStore.getMF(f.getPath()).getDedupFile().writeCache();
				return "SUCCESS Flush File : Write Cache Flushed for " + file;
			} catch (Exception e) {
				String errorMsg = "ERROR Flush File Failed :for " + file;
				SDFSLogger.getLog().error(errorMsg, e);
				throw new IOException(errorMsg + " because: " + e.toString());
			}
		} else if (cmd.equalsIgnoreCase("all")) {
			SDFSEvent evt = SDFSEvent.flushAllBuffers();
			try {
				DedupFileStore.flushAllFiles();
				evt.endEvent("SUCCESS Flush All Files : Write Cache Flushed");
				return "SUCCESS Flush All Files : Write Cache Flushed";
			} catch (Exception e) {
				String errorMsg = "ERROR Flush All Files Failed : ";
				evt.endEvent("ERROR Flush All Files Failed",SDFSEvent.WARN,e);
				SDFSLogger.getLog().error(errorMsg, e);
				throw new IOException("ERROR Flush All Files Failed : "
						+ errorMsg + " because: " + e.toString());
			}
		} else {
			throw new IOException(
					"ERROR Option not specified - file or all must be specified as part of this command. ["
							+ cmd + "] sent");
		}
	}

}
