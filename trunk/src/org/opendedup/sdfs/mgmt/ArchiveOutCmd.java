package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.RandomGUID;
import org.w3c.dom.Element;

import de.schlichtherle.truezip.file.TFile;

public class ArchiveOutCmd implements Runnable {
	SDFSEvent evt = null;
	private File nf = null;
	private File nft = null;
	private File f = null;
	private File vp = null;
	private File af = null;
	private String srcPath = null;

	public Element getResult(String cmd, String file) throws IOException {
		return archiveOut(file);
	}

	private synchronized Element archiveOut(String srcPath) throws IOException {
		this.srcPath = srcPath;
		f = new File(Main.volume.getPath() + File.separator + srcPath);
		SDFSLogger.getLog().debug("Relication base path = " + f.getPath());
		vp = new File(Main.volume.getPath()).getParentFile();
		SDFSLogger.getLog().debug("Volume parent folder = " + vp.getPath());
		af = new File(vp.getPath() + File.separator + "archives"
				+ File.separator + RandomGUID.getGuid());
		SDFSLogger.getLog().debug("Replication snapshot = " + af.getPath());
		nf = new File(vp.getPath() + File.separator + "archives"
				+ File.separator + RandomGUID.getGuid());
		SDFSLogger.getLog().debug("Replication staging = " + nf.getPath());
		nft = new File(nf.getPath() + ".tar.gz");
		SDFSLogger.getLog().debug("Created replication snapshot");
		evt = SDFSEvent.archiveOutEvent("Archiving out " + srcPath);
		evt.extendedInfo = nft.getPath();
		try {
			Thread th = new Thread(this);
			th.start();
			return evt.toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public void run() {
		String sc = " not successful";
		try {
			SDFSEvent sevt = SDFSEvent.snapEvent("Creating Snapshot of "
					+ srcPath, f);

			evt.maxCt = 4;
			evt.addChild(sevt);
			MetaDataDedupFile mf = null;
			try {
				mf = MetaFileStore.snapshot(f.getPath(), af.getPath(), false,
						sevt);

			} catch (Exception e) {
				sevt.endEvent("unable to create snapshot", SDFSEvent.ERROR, e);
				throw e;
			}
			evt.curCt = 2;

			SDFSEvent eevt = SDFSEvent.archiveOutEvent("Archiving out "
					+ srcPath);
			sevt.endEvent("Created Snapshot of " + srcPath);
			evt.addChild(eevt);
			try {
				SDFSLogger.getLog().debug("Created replication snapshot");
				eevt.maxCt = 3;
				eevt.curCt = 0;
				mf.copyTo(nf.getPath(), true);
				eevt.curCt = 1;
				MetaFileStore.removeMetaFile(af.getPath());
				eevt.curCt = 2;
				SDFSLogger.getLog().debug("Copied out replication snapshot");
				TFile dest = new TFile(nf.getPath() + ".tar.gz");
				TFile src = new TFile(nf);
				src.cp_rp(dest);
				SDFSLogger.getLog().debug(
						"created archive " + nf.getPath() + ".tar.gz");
				TFile.umount(dest, true);
				eevt.curCt = 3;
				evt.curCt = 4;
				eevt.endEvent("Archiving out " + srcPath + " successful");
				if (nft.exists())
					evt.endEvent("Archive Out complete from " + srcPath
							+ " to " + nft.getPath());
				else
					throw new IOException(nft.getPath() + " does not exist");
				sc = "successful";

			} catch (Exception e) {
				eevt.endEvent("Archiving out " + srcPath + " failed",
						SDFSEvent.ERROR, e);
			}
		} catch (Exception e) {
			evt.endEvent("Archive Out failed", SDFSEvent.ERROR, e);
			SDFSLogger.getLog().error(
					"Unable to archive out [" + srcPath + "] because :"
							+ e.toString(), e);

		} finally {
			DeleteDir.deleteDirectory(nf);
			SDFSLogger.getLog().info("Exited Replication task [" + sc + "]");
		}

	}

}
