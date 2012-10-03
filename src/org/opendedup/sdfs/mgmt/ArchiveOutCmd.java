package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.SDFSLogger;

import de.schlichtherle.truezip.file.TFile;

public class ArchiveOutCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		return archiveOut(file);
	}

	private synchronized String archiveOut(String srcPath) throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		SDFSLogger.getLog().debug("Relication base path = " +f.getPath());
		File vp = new File(Main.volume.getPath()).getParentFile();
		SDFSLogger.getLog().debug("Volume parent folder = " +vp.getPath());
        File af = new File(vp.getPath() + File.separator + "archives" +File.separator + RandomGUID.getGuid());
        SDFSLogger.getLog().debug("Replication snapshot = " +af.getPath());
        File nf = new File(vp.getPath() + File.separator + "archives" +File.separator + RandomGUID.getGuid());
        SDFSLogger.getLog().debug("Replication staging = " +nf.getPath());
		try {
			SDFSLogger.getLog().debug("Created replication snapshot");
			MetaDataDedupFile mf = MetaFileStore.snapshot(f.getPath(), af.getPath(), false);
			SDFSLogger.getLog().debug("Created replication snapshot");
			
			mf.copyTo(nf.getPath(), true);
			MetaFileStore.removeMetaFile(af.getPath());
			SDFSLogger.getLog().debug("Copied out replication snapshot");
			TFile dest = new TFile(nf.getPath() + ".tar.gz");
			TFile src = new TFile(nf);
			src.cp_rp(dest);
			SDFSLogger.getLog().debug("created archive " + nf.getPath() + ".tar.gz");
			TFile.umount(dest, true);
			File nft = new File(nf.getPath() + ".tar.gz");
			if(nft.exists())
				return nft.getName();
			else
				throw new IOException(nft.getPath() + " does not exist");
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to take archive of Source ["
							+ srcPath + "] " + "Destination [" + af.getPath()
							+ "] because :" + e.toString(), e);
			throw new IOException(
					"Unable to take archive of Source ["
							+ srcPath + "] " + "Destination [" + af.getPath()
							+ "] because :" + e.toString());
		} finally {
			FileUtils.deleteDirectory(new File(nf.getPath()));
            nf.delete();
		}
	}

}
