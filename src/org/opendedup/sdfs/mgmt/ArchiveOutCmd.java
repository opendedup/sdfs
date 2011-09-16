package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.SDFSLogger;

import de.schlichtherle.truezip.file.TFile;

public class ArchiveOutCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		return takeSnapshot(file);
	}

	private String takeSnapshot(String srcPath)
			throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		File vp = new File(Main.volume.getPath()).getParentFile();
		File nf = new File(vp.getPath() + File.separator + "archives" +File.separator + RandomGUID.getGuid());
		if(!nf.exists())
			nf.mkdirs();
			
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			mf.copyTo(nf.getPath(), true);
			TFile dest = new TFile(new File(nf.getPath() + ".tar.gz"));
			TFile src = new TFile(nf);
			src.cp_rp(dest);
			TFile.umount(dest);
			Process p = Runtime.getRuntime().exec("rm -rf " + nf.getPath());
			p.waitFor();
			return nf.getName() + ".tar.gz";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to take archive of Source ["
							+ srcPath + "] " + "Destination [" + nf.getPath()
							+ "] because :" + e.toString(), e);
			throw new IOException(
					"Unable to take archive of Source ["
							+ srcPath + "] " + "Destination [" + nf.getPath()
							+ "] because :" + e.toString());
		}
	}

}
