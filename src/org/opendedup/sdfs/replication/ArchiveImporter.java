package org.opendedup.sdfs.replication;

import de.schlichtherle.truezip.file.TFile;

import java.io.*;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.GCMain;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.SDFSLogger;

public class ArchiveImporter {

	public static String importArchive(String srcArchive, String dest, String server, String password, int port,int maxSz)
			throws IOException {
		try {
			GCMain.gclock.lock();

			File f = new File(srcArchive);
			String sdest = dest + "." + RandomGUID.getGuid();
			SDFSLogger.getLog().info("Importing " + srcArchive + " from " +server+":" + port+ " to " + dest);
			if (!f.exists())
				throw new IOException("File does not exist " + srcArchive);
			TFile srcFilesRoot = new TFile(new File(srcArchive + "/files/"));
			TFile srcFiles = srcFilesRoot.listFiles()[0];
			TFile dstFiles = new TFile(Main.volume.getPath() + File.separator
					+ sdest);
			srcFiles.cp_rp(dstFiles);
			srcFiles = new TFile(new File(srcArchive + "/ddb/"));
			File ddb = new File(Main.dedupDBStore + File.separator);
			if (!ddb.exists())
				ddb.mkdirs();
			dstFiles = new TFile(Main.dedupDBStore + File.separator);
			srcFiles.cp_rp(dstFiles);
			TFile.umount(srcFiles.getInnerArchive());
			try {
				MetaFileImport imp = new MetaFileImport(Main.volume.getPath()
						+ File.separator + sdest,server,password,port,maxSz);
				if (imp.isCorrupt()) {
					SDFSLogger.getLog().warn("Import failed for " + srcArchive);
					SDFSLogger.getLog().warn("rolling back import");
					rollBackImport(Main.volume.getPath() + File.separator
							+ sdest);
					SDFSLogger.getLog().warn("Import rolled back");
					throw new IOException(
							"uable to import files: There are files that are missing blocks");
				} else {
						commitImport(Main.volume.getPath() + File.separator
								+ dest, Main.volume.getPath() + File.separator
								+ sdest);
						StringBuffer sb = new StringBuffer();
						sb.append("<replication-import ");
						sb.append("src=\""+srcArchive+ "\" ");
						sb.append("dest=\""+dest+ "\" ");
						sb.append("srcserver=\""+server+ "\" ");
						sb.append("srcserverport=\""+port+ "\" ");
						sb.append("batchsize=\""+maxSz+ "\" ");
						sb.append("filesimported=\""+imp.getFilesProcessed()+ "\" ");
						sb.append("bytesimported=\""+imp.getBytesTransmitted()+ "\" ");
						sb.append("entriesimported=\""+imp.getEntries()+ "\" ");
						sb.append("virtualbytesimported=\""+imp.getVirtualBytesTransmitted()+ "\" ");
						sb.append("starttime=\""+imp.getStartTime()+ "\" ");
						sb.append("endtime=\""+imp.getEndTime()+ "\" ");
						sb.append("volume=\""+Main.volume.getName()+ "\" ");
						sb.append("/>");
						return sb.toString();
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("rolling back import");
				rollBackImport(Main.volume.getPath() + File.separator + sdest);
				SDFSLogger.getLog().warn("Import rolled back");
				throw new IOException(e);
			}
		} finally {
			GCMain.gclock.unlock();
		}

	}

	public static void rollBackImport(String path) {
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(path);
			if (mf.isDirectory()) {
				String[] files = mf.list();
				for (int i = 0; i < files.length; i++) {
					MetaDataDedupFile _mf = MetaFileStore.getMF(files[i]);
					if (_mf.isDirectory())
						rollBackImport(_mf.getPath());
					else {
						MetaFileStore.removeMetaFile(_mf.getPath());
					}
				}
			}
			MetaFileStore.removeMetaFile(mf.getPath());
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"unable to remove " + path + " during rollback ");
		}
	}

	public static void commitImport(String dest, String sdest)
			throws IOException {
		File f = new File(dest);
		if (f.exists()) {
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(dest);
				MetaFileStore.removeMetaFile(mf.getPath());
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to commit replication while removing old data in ["
								+ dest + "]", e);
				throw new IOException(
						"unable to commit replication while removing old data in ["
								+ dest + "]");
			}
		}
		try {
			MetaDataDedupFile nmf = MetaFileStore.getMF(sdest);
			nmf.renameTo(dest);
			SDFSLogger.getLog().info("moved " + sdest + " to " + dest);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to commit replication while moving from staing ["
							+ sdest + "] to [" + dest + "]", e);
			throw new IOException(
					"unable to commit replication while moving from staing ["
							+ sdest + "] to [" + dest + "]");

		}

	}

}
