/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.replication;

import java.io.File;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.ClusterRedundancyCheck;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.GCMain;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.cli.MgmtServerConnection;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.FileCounts;
import org.opendedup.util.OSValidator;
import org.opendedup.util.ProcessWorker;
import org.opendedup.util.RandomGUID;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TVFS;

public class ArchiveImporter {

	private boolean closed = false;
	private static ConcurrentHashMap<String, ArchiveImporter> runningJobs = new ConcurrentHashMap<String, ArchiveImporter>();
	SDFSEvent ievt = null;
	MetaFileImport imp = null;

	public static void stopJob(String id) {
		runningJobs.get(id).close();
	}

	public void close() {
		this.closed = true;
		if (this.imp != null)
			imp.close();
	}

	public Element importArchive(String srcArchive, String dest, String server,
			String password, int port, int maxSz, SDFSEvent evt, boolean useSSL,boolean useLz4)
			throws Exception {
		ievt = SDFSEvent.archiveImportEvent("Importing " + srcArchive
				+ " from " + server + ":" + port + " to " + dest, evt);
		ReadLock l = GCMain.gclock.readLock();
		l.lock();
		runningJobs.put(evt.uid, this);
		String sdest = dest + "." + RandomGUID.getGuid();
		File f = new File(srcArchive);
		File fDstFiles = new File(Main.volume.getPath() + File.separator
				+ sdest);
		try {

			SDFSLogger.getLog().info("setting up staging at " + sdest);
			try {
				SDFSLogger.getLog().info(
						"Importing " + srcArchive + " from " + server + ":"
								+ port + " to " + dest);
				if (!f.exists())
					throw new IOException("File does not exist " + srcArchive);
				if (OSValidator.isWindows()) {

					TFile srcRoot = new TFile(new File(srcArchive + "/"));
					ievt.maxCt = FileCounts.getSize(srcRoot);

					SDFSLogger.getLog().info("Tar file size is " + ievt.maxCt);
					TFile srcFilesRoot = new TFile(new File(srcArchive
							+ "/files/"));
					TFile srcFiles = null;
					try {
						srcFiles = srcFilesRoot.listFiles()[0];
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"Replication archive is corrupt " + srcArchive
										+ " size of "
										+ new File(srcArchive).length(), e);
						throw e;
					}
					TFile tfDstFiles = new TFile(Main.volume.getPath()
							+ File.separator + sdest);
					this.export(srcFiles, tfDstFiles);
					srcFiles = new TFile(new File(srcArchive + "/ddb/"));
					File ddb = new File(Main.dedupDBStore + File.separator);
					if (!ddb.exists())
						ddb.mkdirs();
					TFile mDstFiles = new TFile(Main.dedupDBStore
							+ File.separator);
					this.export(srcFiles, mDstFiles);
					TVFS.umount(srcFiles);
					TVFS.umount(mDstFiles);
					TVFS.umount(srcRoot.getInnerArchive());
				} else {
					ievt.maxCt = 3;
					File stg = null;
					try {
						stg = new File(new File(srcArchive).getParentFile()
								.getPath()
								+ File.separator
								+ RandomGUID.getGuid());
						stg.mkdirs();
						
						String expFile = "tar -xzpf " + srcArchive + " -C "
								+ stg.getPath();
						if(useLz4)
							expFile = "lz4 -dc " + srcArchive + " | tar -xpf -";
						int xt = ProcessWorker.runProcess(expFile);
						if (xt != 0)
							throw new IOException("expand failed in " + expFile
									+ " exit value was " + xt);
						ievt.curCt++;
						SDFSLogger.getLog().info(
								"executed " + expFile + " exit code was " + xt);
						File srcFilesRoot = new File(stg.getPath()
								+ File.separator + "files");
						File srcFiles = null;
						try {
							srcFiles = srcFilesRoot.listFiles()[0];
						} catch (Exception e) {
							SDFSLogger.getLog().error(
									"Replication archive is corrupt "
											+ srcArchive + " size of "
											+ new File(srcArchive).length(), e);
							throw e;
						}
						SDFSLogger.getLog().info(
								"setting up staging at " + fDstFiles.getPath());
						fDstFiles.getParentFile().mkdirs();
						String cpCmd = "cp -rfap " + srcFiles + " " + fDstFiles;

						xt = ProcessWorker.runProcess(cpCmd);
						if (xt != 0)
							throw new IOException("copy failed in " + cpCmd
									+ " exit value was " + xt);
						SDFSLogger.getLog().info(
								"executed " + cpCmd + " exit code was " + xt);
						ievt.curCt++;
						srcFiles = new File(stg.getPath() + File.separator
								+ "ddb");
						File ddb = new File(Main.dedupDBStore + File.separator);
						if (!ddb.exists())
							ddb.mkdirs();
						if (srcFiles.exists()) {
							cpCmd = "cp -rfap " + srcFiles + File.separator
									+ " " + ddb.getParentFile().getPath();
							xt = ProcessWorker.runProcess(cpCmd);
							if (xt != 0)
								throw new IOException("copy failed in " + cpCmd
										+ " exit value was " + xt);
						}
						SDFSLogger.getLog().info(
								"executed " + cpCmd + " exit code was " + xt);
						ievt.endEvent("Staging completed successfully");
					} catch (Exception e) {
						ievt.endEvent(e.getMessage(), SDFSEvent.ERROR);
						throw e;
					} finally {
						// FileUtils.deleteDirectory(stg);
						Process p = Runtime.getRuntime().exec("rm -rf " + stg);
						p.waitFor();
						f.delete();
					}

				}
				String hmac = MgmtServerConnection.getAuth(password);
				imp = new MetaFileImport(Main.volume.getPath() + File.separator
						+ sdest, server, hmac, port, maxSz, evt, useSSL);
				imp.runImport();
				if (imp.isCorrupt()) {

					// evt.endEvent("Import failed for " + srcArchive +
					// " because not all the data could be imported from " +
					// server,SDFSEvent.WARN);
					SDFSLogger
							.getLog()
							.warn("Import failed for "
									+ srcArchive
									+ " because not all the data could be imported from "
									+ server);
					SDFSLogger.getLog().warn("rolling back import");
					rollBackImport(Main.volume.getPath() + File.separator
							+ sdest);
					SDFSLogger.getLog().warn("Import rolled back");
					throw new IOException(
							"uable to import files: There are files that are missing blocks");
				} else {
					if (!Main.chunkStoreLocal)
						new ClusterRedundancyCheck(ievt,
								new File(Main.volume.getPath() + File.separator
										+ sdest), true);
					commitImport(Main.volume.getPath() + File.separator + dest,
							Main.volume.getPath() + File.separator + sdest);
					DocumentBuilderFactory factory = DocumentBuilderFactory
							.newInstance();
					DocumentBuilder builder;
					builder = factory.newDocumentBuilder();

					DOMImplementation impl = builder.getDOMImplementation();
					// Document.
					Document doc = impl.createDocument(null,
							"replication-import", null);
					// Root element.
					Element root = doc.getDocumentElement();
					root.setAttribute("src", srcArchive);
					root.setAttribute("dest", dest);
					root.setAttribute("srcserver", server);
					root.setAttribute("srcserverport", Integer.toString(port));
					root.setAttribute("batchsize", Integer.toString(maxSz));
					root.setAttribute("filesimported",
							Long.toString(imp.getFilesProcessed()));
					root.setAttribute("bytesimported",
							Long.toString(imp.getBytesTransmitted()));
					root.setAttribute("entriesimported",
							Long.toString(imp.getEntries()));
					root.setAttribute("virtualbytesimported",
							Long.toString(imp.getVirtualBytesTransmitted()));
					root.setAttribute("starttime",
							Long.toString(imp.getStartTime()));
					root.setAttribute("endtime",
							Long.toString(imp.getEndTime()));
					root.setAttribute("volume", Main.volume.getName());
					root.setAttribute("volumeconfig",
							Main.volume.getConfigPath());
					evt.endEvent(srcArchive + " from " + server + ":" + port
							+ " to " + dest + " imported successfully");
					return (Element) root.cloneNode(true);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("rolling back import ", e);
				rollBackImport(Main.volume.getPath() + File.separator + sdest);
				SDFSLogger.getLog().warn("Import rolled back");

				if (!evt.isDone())
					evt.endEvent("Import failed and was rolled back ",
							SDFSEvent.ERROR, e);
				throw e;
			}
		} finally {
			try {

			} catch (Exception e) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("error", e);
			}
			runningJobs.remove(evt.uid);
			l.unlock();
		}

	}

	public void rollBackImport(String path) {
		try {
			path = new File(path).getPath();
			MetaDataDedupFile mf = MetaFileStore.getMF(path);

			if (mf.isDirectory()) {
				String[] files = mf.list();
				for (int i = 0; i < files.length; i++) {
					MetaDataDedupFile _mf = MetaFileStore.getMF(files[i]);
					if (_mf.isDirectory())
						rollBackImport(_mf.getPath());
					else {
						MetaFileStore.removeMetaFile(_mf.getPath(), true,true);
					}
				}
			}
			MetaFileStore.removeMetaFile(mf.getPath(), true,true);
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"unable to remove " + path + " during rollback ", e);
		}
	}

	public void commitImport(String dest, String sdest) throws IOException {
		try {
			boolean rn = MetaFileStore.rename(new File(sdest).getPath(), new File(dest).getPath());
			SDFSLogger.getLog().info("moved "+ sdest + " to " + dest + " "+ rn);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to commit replication while moving from staing ["
							+ sdest + "] to [" + dest + "]", e);
			throw new IOException(
					"unable to commit replication while moving from staing ["
							+ sdest + "] to [" + dest + "]");

		} finally {
			File f = new File(sdest);
			if(f.exists() && f.isDirectory()) {
				FileUtils.deleteDirectory(f);
				SDFSLogger.getLog().info("deleted "+ f.getPath());
			}
			else if(f.exists()) {
				f.delete();
				SDFSLogger.getLog().info("deleted "+ f.getPath());
			}
		}
		
	}

	private void export(File file, File dst)
			throws ReplicationCanceledException, IOException {
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"extracting " + file.getPath() + " to " + dst.getPath());
		if (!closed) {
			if (OSValidator.isWindows())
				TFile.cp_rp(file, dst, TArchiveDetector.NULL);
			/*
			 * if (file.isDirectory()) { dst.mkdirs(); // All files and
			 * subdirectories TFile[] files = file.listFiles(); for (int i = 0;
			 * i < files.length; i++) { File dstF = new File(dst,
			 * files[i].getName()); if (files[i].isFile()) {
			 * files[i].cp_p(dstF); ievt.curCt += dstF.length(); } else {
			 * export(files[i], dstF); } } } else {
			 * dst.getParentFile().mkdirs();
			 * 
			 * }
			 */

		} else {
			throw new ReplicationCanceledException(
					"replication job was canceled");
		}
	}

	public static void main(String[] args) throws IOException {
	}

}
