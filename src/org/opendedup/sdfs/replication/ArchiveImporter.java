package org.opendedup.sdfs.replication;

import de.schlichtherle.truezip.file.TFile;


import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.GCMain;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.SDFSLogger;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ArchiveImporter {

	public static Element importArchive(String srcArchive, String dest, String server, String password, int port,int maxSz,SDFSEvent evt,boolean useSSL)
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
						+ File.separator + sdest,server,password,port,maxSz,evt,useSSL);
				if (imp.isCorrupt()) {
					evt.endEvent("Import failed for " + srcArchive + " because not all the data could be imported from " + server,SDFSEvent.WARN);
					SDFSLogger.getLog().warn("Import failed for " + srcArchive + " because not all the data could be imported from " + server);
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
						DocumentBuilderFactory factory = DocumentBuilderFactory
								.newInstance();
						DocumentBuilder builder;
						builder = factory.newDocumentBuilder();

						DOMImplementation impl = builder.getDOMImplementation();
						// Document.
						Document doc = impl.createDocument(null, "replication-import", null);
						// Root element.
						Element root = doc.getDocumentElement();
						root.setAttribute("src", srcArchive);
						root.setAttribute("dest", dest);
						root.setAttribute("srcserver", server);
						root.setAttribute("srcserverport", Integer.toString(port));
						root.setAttribute("batchsize", Integer.toString(maxSz));
						root.setAttribute("filesimported", Long.toString(imp.getFilesProcessed()));
						root.setAttribute("bytesimported", Long.toString(imp.getBytesTransmitted()));
						root.setAttribute("entriesimported", Long.toString(imp.getEntries()));
						root.setAttribute("virtualbytesimported", Long.toString(imp.getVirtualBytesTransmitted()));
						root.setAttribute("starttime", Long.toString(imp.getStartTime()));
						root.setAttribute("endtime", Long.toString(imp.getEndTime()));
						root.setAttribute("volume", Main.volume.getName());
						root.setAttribute("volumeconfig", Main.wth.getConfigFilePath());
						evt.endEvent(srcArchive + " from " +server+":" + port+ " to " + dest + " imported successfully");
						return (Element)root.cloneNode(true);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("rolling back import");
				rollBackImport(Main.volume.getPath() + File.separator + sdest);
				SDFSLogger.getLog().warn("Import rolled back");
				if(!evt.isDone())
					evt.endEvent("Import failed and was rolled back",SDFSEvent.ERROR,e);
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
