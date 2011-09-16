package org.opendedup.sdfs.replication;

import de.schlichtherle.truezip.file.TFile;
import java.io.*;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.SDFSLogger;

public class ArchiveImporter {
	
	
	public static void importArchive(String srcArchive, String dest) throws IOException {
		File f = new File(srcArchive);
		if(!f.exists())
			throw new IOException("File does not exist " + srcArchive);
		TFile srcFilesRoot = new TFile(new File(srcArchive + "/files/"));
		TFile srcFiles = srcFilesRoot.listFiles()[0];
		TFile dstFiles = new TFile(Main.volume.getPath() + File.separator + dest);
		srcFiles.cp_rp(dstFiles);
		srcFiles = new TFile(new File(srcArchive + "/ddb/"));
		dstFiles = new TFile(Main.dedupDBStore + File.separator);	
		TFile.umount(srcFiles.getInnerArchive());
		MetaFileImport imp = new MetaFileImport(Main.volume.getPath() + File.separator + dest);
		if(imp.getCorruptFiles().size() > 0) {
			SDFSLogger.getLog().warn("Import failed for "+srcArchive);
			SDFSLogger.getLog().warn("rolling back import");
			rollBackImport(Main.volume.getPath() + File.separator + dest);
			SDFSLogger.getLog().warn("Import rolled back");
			throw new IOException("uable to import files: There are files that are missing blocks");
		}
	}
	
	public static void rollBackImport(String path) {
		MetaDataDedupFile mf = MetaFileStore.getMF(path);
		if(mf.isDirectory()) {
			String [] files = mf.list();
			for(int i = 0;i<files.length;i++) {
				MetaDataDedupFile _mf = MetaFileStore.getMF(files[i]);
				if(_mf.isDirectory())
					rollBackImport(_mf.getPath());
				else {
					MetaFileStore.removeMetaFile(_mf.getPath());
				}
			}
		}
		MetaFileStore.removeMetaFile(mf.getPath());
	}
	
	
	
}
