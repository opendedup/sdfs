package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.OSValidator;
import org.opendedup.util.RandomGUID;
import org.w3c.dom.Element;

import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TVFS;

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
		} finally {

		}
	}

	public void run() {
		String sc = "not successful";
		try {
			SDFSEvent sevt = SDFSEvent.snapEvent("Creating Snapshot of "
					+ srcPath, f);

			evt.maxCt = 4;
			evt.addChild(sevt);
			MetaDataDedupFile mf = null;
			mf = MetaFileStore.snapshot(f.getPath(), af.getPath(), false, sevt);
			evt.curCt = 2;

			SDFSEvent eevt = SDFSEvent.archiveOutEvent("Archiving out "
					+ srcPath);
			sevt.endEvent("Created Snapshot of " + srcPath);
			evt.addChild(eevt);
			SDFSLogger.getLog().debug("Created replication snapshot");
			eevt.maxCt = 3;
			eevt.curCt = 0;
			mf.copyTo(nf.getPath(), true, true);
			eevt.curCt = 1;
			MetaFileStore.removeMetaFile(af.getPath(), true);
			eevt.curCt = 2;
			SDFSLogger.getLog().debug("Copied out replication snapshot");
			if (OSValidator.isWindows()) {
				TFile dest = new TFile(nf.getPath() + ".tar.gz");
				TFile src = new TFile(nf);
				SDFSLogger.getLog().debug(
						"created archive " + nf.getPath() + ".tar.gz");
				TVFS.umount(dest);
				TVFS.umount(src);
				TVFS.umount(dest.getInnerArchive());
			} else {
				Process p = Runtime.getRuntime().exec(
						"tar -cpzf " + nf.getPath() + ".tar.gz -C "
								+ nf.getPath() + " .", null, nf);
				p.waitFor();
			}
			eevt.curCt = 3;
			evt.curCt = 4;

			if (nft.exists())
				evt.endEvent("Archive Out complete from " + srcPath + " to "
						+ nft.getPath());
			else
				throw new IOException(nft.getPath() + " does not exist");
			eevt.endEvent("Archiving out " + srcPath + " successful");
			sc = "successful";
		} catch (Throwable e) {
			SDFSLogger.getLog().error(
					"Unable to archive out [" + srcPath + "] because :"
							+ e.toString(), e);
			evt.endEvent("Archive Out failed", SDFSEvent.ERROR, e);

		} finally {
			try {
				DeleteDir.deleteDirectory(nf);
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"error while deleting " + nf.getPath(), e);
			}
			SDFSLogger.getLog().info("Exited Replication task [" + sc + "]");
		}

	}

}
