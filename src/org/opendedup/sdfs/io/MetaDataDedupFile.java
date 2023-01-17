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
package org.opendedup.sdfs.io;

import java.io.File;

import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.CC;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.grpc.FileInfo.FileAttributes;
import org.opendedup.grpc.FileInfo.FileInfoResponse;
import org.opendedup.grpc.FileInfo.FileInfoResponse.fileType;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileRenamed;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.io.events.MMetaUpdated;
import org.opendedup.sdfs.mgmt.GetCloudFile;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.monitor.IOMonitor;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.ByteUtils;
import org.opendedup.util.OSValidator;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import net.jpountz.xxhash.XXHash64;

import com.google.common.eventbus.EventBus;
import com.google.gson.JsonObject;

/**
 *
 * @author annesam Stores Meta-Data about a dedupFile. This class is modeled
 *         from the java.io.File class. Meta-Data files are stored within the
 *         MetaDataFileStore @see com.annesam.filestore.MetaDataFileStore
 */
public class MetaDataDedupFile implements java.io.Externalizable {

	private static final long serialVersionUID = -4598940197202968523L;
	private static EventBus eventBus = new EventBus();
	transient public static final String pathSeparator = File.pathSeparator;
	transient public static final String separator = File.separator;
	transient public static final char pathSeparatorChar = File.pathSeparatorChar;
	transient public static final char separatorChar = File.separatorChar;
	private long length = 0;
	private String path = "";
	private String backingFile = null;
	private AtomicLong lastModified = new AtomicLong(0);
	private AtomicLong lastAccessed = new AtomicLong(0);
	private boolean execute = true;
	private boolean read = true;
	private boolean write = true;
	private boolean directory = false;
	private boolean hidden = false;
	private boolean ownerWriteOnly = false;
	private boolean ownerExecOnly = false;
	private boolean ownerReadOnly = false;
	private String dfGuid = null;

	public boolean deleteOnClose = false;
	private String guid = "";
	private IOMonitor monitor;
	private boolean vmdk;
	private boolean importing;
	private int permissions;
	private int owner_id = -1;
	private int group_id = -1;
	private int mode = -1;
	private HashMap<String, String> extendedAttrs = new HashMap<String, String>();
	private boolean symlink = false;
	private String symlinkPath = null;
	private String version = Main.version;
	private boolean dirty = false;
	private long attributes = 0;
	private long retentionLock = -1;
	private static final int pl = Main.volume.getPath().length();


	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public static void unregisterListener(Object obj) {
		eventBus.unregister(obj);
	}

	public int getMode() throws IOException {
		if (mode == -1) {
			Path p = Paths.get(this.path);
			if (!OSValidator.isWindows()) {
				this.mode = (Integer) Files.getAttribute(p, "unix:mode", LinkOption.NOFOLLOW_LINKS);
			} else {
				this.mode = 0;
			}
		}
		return this.mode;
	}

	public void setMode(int mode) throws IOException {
		setMode(mode, true);
	}

	


	public void setMode(int mode, boolean propigateEvent) throws IOException {
		this.mode = mode;
		this.dirty = true;
		if (!OSValidator.isWindows()) {
			Path p = Paths.get(this.path);
			Files.setAttribute(p, "unix:mode", Integer.valueOf(mode), LinkOption.NOFOLLOW_LINKS);
		}
	}

	/**
	 * adds a posix extended attribute
	 *
	 * @param name  the name of the attribute
	 * @param value the value of the attribute
	 * @throws IOException
	 */
	public void addXAttribute(String name, String value) throws IOException {
		addXAttribute(name, value, true, true);

	}

	/**
	 * adds a posix extended attribute
	 *
	 * @param name           the name of the attribute
	 * @param value          the value of the attribute
	 * @param propigateEvent TODO
	 * @throws IOException
	 */
	public void addXAttribute(String name, String value, boolean propigateEvent, boolean write) throws IOException {
		this.writeLock.lock();
		try {
			extendedAttrs.put(name, value);
			if (propigateEvent) {
				this.dirty = true;
				eventBus.post(new MMetaUpdated(this, name, value));
				this.unmarshal();
			} else if (write) {
				this.writeFile(false);
			} else {
				this.dirty = true;
			}

		} finally {
			this.writeLock.unlock();
		}
	}

	public void removeXAttribute(String name) throws IOException {
		this.writeLock.lock();
		try {
			if (name.equals("lookup.filter")) {
				SDFSLogger.getLog().warn("cannot reset lookup filter");
			} else {
				extendedAttrs.remove(name);
				this.dirty = true;
				eventBus.post(new MMetaUpdated(this, name, null));
				this.unmarshal();
			}
		} finally {
			this.writeLock.unlock();
		}
	}

	public void setBackingFile(String file) {
		this.backingFile = file;
	}

	public String getBackingFile() {
		return this.backingFile;
	}

	/**
	 * returns an extended attribute for a give name
	 *
	 * @param name
	 * @return the extended attribute
	 */
	public String getXAttribute(String name) {
		if (this.extendedAttrs.containsKey(name))
			return extendedAttrs.get(name);
		else
			return null;
	}

	/**
	 *
	 * @return list of all extended attribute names
	 */
	public String[] getXAttersNames() {
		String[] keys = new String[this.extendedAttrs.size()];
		Iterator<String> iter = this.extendedAttrs.keySet().iterator();
		int i = 0;
		while (iter.hasNext()) {
			keys[i] = iter.next();
			i++;
		}
		return keys;
	}

	/**
	 *
	 * @return posix permissions e.g. 0777
	 */
	public int getPermissions() {
		return permissions;
	}

	/**
	 *
	 * @param permissions sets permissions
	 */
	public void setPermissions(int permissions) {
		setPermissions(permissions, true);
	}

	/**
	 *
	 * @param permissions    sets permissions
	 * @param propigateEvent TODO
	 */
	public void setPermissions(int permissions, boolean propigateEvent) {
		this.writeLock.lock();
		try {
			this.dirty = true;
			this.permissions = permissions;
		} finally {
			this.writeLock.unlock();
		}
	}

	/**
	 *
	 * @return the file owner id
	 * @throws IOException
	 */
	public int getOwner_id() throws IOException {
		if (owner_id == -1) {
			if (!OSValidator.isWindows()) {
				Path p = Paths.get(this.path);
				this.owner_id = (Integer) Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS);
			} else {
				this.owner_id = 0;
			}
		}
		return owner_id;
	}

	/**
	 *
	 * @param owner_id sets the file owner id
	 * @throws IOException
	 */
	public void setOwner_id(int owner_id) throws IOException {
		setOwner_id(owner_id, true);
	}

	/**
	 *
	 * @param owner_id       sets the file owner id
	 * @param propigateEvent TODO
	 * @throws IOException
	 */
	public void setOwner_id(int owner_id, boolean propigateEvent) throws IOException {
		this.owner_id = owner_id;
		if (!OSValidator.isWindows()) {
			Path p = Paths.get(this.path);
			Files.setAttribute(p, "unix:uid", Integer.valueOf(owner_id), LinkOption.NOFOLLOW_LINKS);
		}
	}

	/**
	 *
	 * @return returns the group owner id
	 * @throws IOException
	 */
	public int getGroup_id() throws IOException {
		if (group_id == -1) {
			if (!OSValidator.isWindows()) {
				Path p = Paths.get(this.path);
				this.group_id = (Integer) Files.getAttribute(p, "unix:gid", LinkOption.NOFOLLOW_LINKS);
			} else {
				this.group_id = 0;
			}
		}
		return group_id;
	}

	/**
	 *
	 * @param l sets the group owner id
	 * @throws IOException
	 */
	public void setGroup_id(int l) throws IOException {
		setGroup_id(l, true);
	}

	/**
	 *
	 * @param group_id       sets the group owner id
	 * @param propigateEvent TODO
	 * @throws IOException
	 */
	public void setGroup_id(int group_id, boolean propigateEvent) throws IOException {
		this.group_id = group_id;
		if (!OSValidator.isWindows()) {
			Path p = Paths.get(this.path);
			Files.setAttribute(p, "unix:gid", Integer.valueOf(group_id), LinkOption.NOFOLLOW_LINKS);
		}
	}

	/**
	 *
	 * @return true if this file is a vmdk
	 */
	public boolean isVmdk() {
		return vmdk;
	}

	/**
	 *
	 * @param vmdk flags this file as a vmdk if true
	 */
	public void setVmdk(boolean vmdk) {
		setVmdk(vmdk, true);
	}

	/**
	 *
	 * @param vmdk           flags this file as a vmdk if true
	 * @param propigateEvent TODO
	 */
	public void setVmdk(boolean vmdk, boolean propigateEvent) {
		this.vmdk = vmdk;
	}

	public static MetaDataDedupFile getFile(String path) throws IOException {
		File f = new File(path);
		if (!f.getCanonicalPath().startsWith(Main.volume.connicalPath)) {
			throw new IOException(" get file connical path [" + f.getCanonicalPath() + "] is not in folder structure "
					+ Main.volume.connicalPath);
		}
		MetaDataDedupFile mf = null;
		Path p = Paths.get(path);
		if (Files.isSymbolicLink(p)) {
			mf = new MetaDataDedupFile();
			mf.path = path;
			mf.symlink = true;
			try {
				mf.symlinkPath = Files.readSymbolicLink(p).toFile().getPath();
				if (new File(mf.symlinkPath).isDirectory())
					mf.directory = true;
			} catch (IOException e) {
				SDFSLogger.getLog().warn(e);
			}
		} else if (!f.exists() && FileReplicationService.MetaFileExists(f.getPath().substring(pl))) {
			GetCloudFile cf = new GetCloudFile();
			cf.getResult(f.getPath().substring(pl), f.getPath().substring(pl));
			cf.downloadAll();
		} else if (!f.exists() || f.isDirectory()) {
			mf = new MetaDataDedupFile(path);
			MetaFileStore.addToCache(mf);
		} else {
			ObjectInputStream in = null;
			try {
				in = new ObjectInputStream(new FileInputStream(path));
				mf = (MetaDataDedupFile) in.readObject();
				mf.path = path;
				SDFSLogger.getLog().debug("reading in file " + mf.path + " df=" + mf.dfGuid);
				if (mf.getDfGuid() != null) {
					SparseDedupFile df = DedupFileStore.get(mf.getDfGuid());
					if (df != null) {
						mf = df.getMetaFile();
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to de-serialize " + path, e);
				throw new IOException(e);
			} finally {
				if (in != null) {
					try {
						in.close();
					} catch (IOException e) {
					}
				}
			}

		}
		return mf;
	}

	/**
	 *
	 * @return returns the IOMonitor for this file. IOMonitors monitor reads,writes,
	 *         and dedup rate.
	 */
	public IOMonitor getIOMonitor() {
		if (monitor == null)
			monitor = new IOMonitor(this);
		return monitor;
	}

	/**
	 *
	 * @param path the path to the dedup file.
	 */
	private MetaDataDedupFile(String path) {

		init(path);
	}

	/**
	 *
	 * @param path the path to the dedup file.
	 */
	private MetaDataDedupFile(String path, MetaDataDedupFile mf) {
		this.path = path;
		this.directory = mf.directory;
		mf.execute = mf.execute;
		this.hidden = mf.hidden;
		this.lastModified = mf.lastModified;
		this.setLength(mf.length, false, true);
		this.ownerExecOnly = mf.ownerExecOnly;
		this.ownerReadOnly = mf.ownerReadOnly;
		this.ownerWriteOnly = mf.ownerWriteOnly;
		this.read = mf.read;
		this.write = mf.write;
		this.owner_id = mf.owner_id;
		this.group_id = mf.group_id;
		this.permissions = mf.permissions;
		this.attributes = mf.attributes;
		monitor = new IOMonitor(this);
		this.dirty = true;
	}

	/**
	 *
	 * @param path the path to the dedup file.
	 */
	public MetaDataDedupFile() {
	}

	public SparseDedupFile getDedupFile(boolean addtoopen) throws IOException {
		this.writeLock.lock();
		try {
			if (this.dfGuid == null) {
				SparseDedupFile df = new SparseDedupFile(this);
				this.dfGuid = df.getGUID();
				SDFSLogger.getLog()
						.debug("No DF EXISTS .... Set dedup file for " + this.getPath() + " to " + this.dfGuid);
				if (addtoopen)
					DedupFileStore.addOpenDedupFiles(df);
				// this.sync();
				return df;
			} else {
				if (addtoopen)
					return (SparseDedupFile) DedupFileStore.openDedupFile(this);
				else
					return (SparseDedupFile) DedupFileStore.getDedupFile(this);
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void setDfGuid(String guid) {
		setDfGuid(guid, true);
	}

	public void setDfGuid(String guid, boolean propigateEvent) {
		this.dfGuid = guid;
		this.dirty = true;
	}

	/**
	 *
	 * @return the guid associated with this file
	 */
	public String getGUID() {
		return guid;
	}

	/**
	 * Clones a file and the underlying DedupFile
	 *
	 * @param snaptoPath the path to clone to
	 * @param overwrite  if true, it will overwrite the destination file if it
	 *                   alreay exists
	 * @return the new clone
	 * @throws IOException
	 */
	public MetaDataDedupFile snapshot(String snaptoPath, boolean overwrite, SDFSEvent evt) throws IOException {
		return snapshot(snaptoPath, overwrite, evt, true);
	}

	/**
	 * Clones a file and the underlying DedupFile
	 *
	 * @param snaptoPath     the path to clone to
	 * @param overwrite      if true, it will overwrite the destination file if it
	 *                       alreay exists
	 * @param propigateEvent TODO
	 * @return the new clone
	 * @throws IOException
	 */
	public MetaDataDedupFile snapshot(String snaptoPath, boolean overwrite, SDFSEvent evt, boolean propigateEvent)
			throws IOException {

		SDFSLogger.getLog().debug("taking snapshot of " + this.getPath() + " to " + snaptoPath);
		if (this.isSymlink()) {

			File dst = new File(snaptoPath);
			File src = new File(this.getPath());
			if (dst.exists() && !overwrite) {
				throw new IOException(snaptoPath + " already exists");
			}
			Path srcP = Paths.get(src.getPath());
			Path dstP = Paths.get(dst.getPath());
			try {
				Files.createSymbolicLink(dstP, Files.readSymbolicLink(srcP).toFile().toPath());
			} catch (IOException e) {
				SDFSLogger.getLog().error("error symlinking " + this.getPath() + " to " + snaptoPath, e);
			}
			return getFile(snaptoPath);
		} else if (!this.isDirectory()) {
			SDFSLogger.getLog().debug("is snapshot file");
			File f = new File(snaptoPath);
			if (f.exists() && !overwrite)
				throw new IOException("path exists [" + snaptoPath + "]Cannot overwrite existing data ");
			else if (f.exists()) {
				MetaFileStore.removeMetaFile(snaptoPath, true, true, true);
			}

			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			MetaDataDedupFile _mf = new MetaDataDedupFile(snaptoPath, this);
			this.getGUID();

			try {
				DedupFileStore.cloneDedupFile(this, _mf);
			} catch (java.lang.NullPointerException e) {
				SDFSLogger.getLog().debug("no dedupfile for " + this.path, e);
			}
			_mf.getIOMonitor().setVirtualBytesWritten(this.getIOMonitor().getVirtualBytesWritten(), true);
			_mf.getIOMonitor().setDuplicateBlocks(
					this.getIOMonitor().getDuplicateBlocks() + this.getIOMonitor().getActualBytesWritten(), true);
			Main.volume.addDuplicateBytes(
					this.getIOMonitor().getDuplicateBlocks() + this.getIOMonitor().getActualBytesWritten(), true);
			Main.volume.addVirtualBytesWritten(this.getIOMonitor().getVirtualBytesWritten(), true);
			Main.volume.addFile();
			_mf.setVmdk(this.isVmdk(), true);
			_mf.dirty = true;

			_mf.unmarshal();
			_mf.sync();
			evt.addCount(1);
			return _mf;
		} else {
			SDFSLogger.getLog().debug("is snapshot dir");

			File f = new File(snaptoPath);

			f.mkdirs();
			int trimlen = this.getPath().length();
			MetaDataDedupFile[] files = this.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (!files[i].getPath().equals(f.getPath())) {

					File rf = new File(Main.volume.getPath() + File.separator + "sdfsactiverepl");
					if (!files[i].getPath().equals(f.getPath()) && !files[i].getPath().startsWith(rf.getPath())) {
						MetaDataDedupFile file = files[i];
						String newPath = snaptoPath + File.separator + file.getPath().substring(trimlen);
						file.snapshot(newPath, overwrite, evt, propigateEvent);
					}
				}
			}
			try {
				return MetaFileStore.getMF(snaptoPath);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to traverse " + this.getPath());
				throw new IOException(e);
			}
		}
	}

	/**
	 * Clones a file and the underlying DedupFile
	 *
	 * @param snaptoPath the path to clone to
	 * @param overwrite  if true, it will overwrite the destination file if it
	 *                   alreay exists
	 * @return the new clone
	 * @throws IOException
	 */
	public void copyTo(String npath, boolean overwrite) throws IOException {
		copyTo(npath, overwrite, true);
	}

	/**
	 * Clones a file and the underlying DedupFile
	 *
	 * @param overwrite      if true, it will overwrite the destination file if it
	 *                       alreay exists
	 * @param propigateEvent TODO
	 * @param snaptoPath     the path to clone to
	 *
	 * @return the new clone
	 * @throws IOException
	 */
	public void copyTo(String npath, boolean overwrite, boolean propigateEvent) throws IOException {
		String snaptoPath = new File(npath + File.separator + "files").getPath() + File.separator;
		File f = new File(snaptoPath);
		if (f.exists() && !overwrite)
			throw new IOException("while copying path exists [" + snaptoPath + "]Cannot overwrite existing data ");
		if (!f.exists())
			f.mkdirs();
		FileUtils.copyDirectory(new File(this.path), new File(snaptoPath), true);
		// SDFSLogger.getLog().info("copied files to " + npath);
		MetaDataDedupFile dmf = MetaDataDedupFile.getFile(snaptoPath);
		dmf.copyDir(npath);
	}

	private void copyDir(String npath) throws IOException {
		String[] files = this.list();

		for (int i = 0; i < files.length; i++) {
			Path p = Paths.get(this.getPath() + File.separator + files[i]);
			if (Files.isSymbolicLink(p)) {

			} else {
				MetaDataDedupFile file = MetaDataDedupFile.getFile(this.getPath() + File.separator + files[i]);
				if (file.isDirectory())
					file.copyDir(npath);
				else {
					SDFSLogger.getLog()
							.debug("copy dedup file for : " + file.getPath() + " guid :" + file.getDfGuid());
					if (file.dfGuid != null) {
						if (DedupFileStore.fileOpen(file))
							file.getDedupFile(false).copyTo(npath, true);
						else {
							File sdbf = LongByteArrayMap.getFile(file.getDfGuid());
							if (sdbf.exists()) {
								boolean lz4 = false;
								if (sdbf.getPath().toLowerCase().endsWith(".lz4")) {
									lz4 = true;
								}
								File ddbdir = new File(npath + File.separator + "ddb" + File.separator
										+ file.dfGuid.substring(0, 2) + File.separator + file.dfGuid);
								ddbdir.mkdirs();
								Path ddbf = new File(ddbdir.getPath() + File.separator + file.dfGuid + ".map").toPath();
								if (lz4) {
									ddbf = new File(ddbdir.getPath() + File.separator + file.dfGuid + ".map.lz4")
											.toPath();
								}
								Files.copy(sdbf.toPath(), ddbf, StandardCopyOption.REPLACE_EXISTING,
										StandardCopyOption.COPY_ATTRIBUTES);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * initiates the MetaDataDedupFile
	 *
	 * @param path the path to the file
	 */
	private void init(String path) {
		this.lastAccessed.set(System.currentTimeMillis());
		this.path = path;
		File f = new File(path);
		if (!f.exists()) {
			SDFSLogger.getLog().debug("Creating new MetaFile for " + this.path);

			this.guid = UUID.randomUUID().toString();
			monitor = new IOMonitor(this);
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.permissions = Main.defaultFilePermissions;
			this.lastModified.set(System.currentTimeMillis());
			this.setLength(0, false, true);
			this.dirty = true;
			Main.volume.addFile();
			this.sync();
		} else if (f.isDirectory()) {
			this.permissions = Main.defaultDirPermissions;
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.directory = true;
			this.length = 4096;
		}

	}

	public Map<String, String> getExtendedAttributes() {
		return this.extendedAttrs;
	}

	public ReentrantLock writeLock = new ReentrantLock();

	/**
	 * Writes the stub for this file to disk. Stubs are pointers written to a file
	 * system that map to virtual filesystem directory and file structure. The stub
	 * only contains the guid associated with the file in question.
	 *
	 * @return true if written
	 */
	private boolean writeFile(boolean notify) {
		writeLock.lock();
		ObjectOutputStream out = null;
		try {
			File f = new File(this.path);

			try {
				// SDFSLogger.getLog().info("cannonical path is " + f.getCanonicalPath() + "
				// path is " + f.getPath());
				if (!f.getCanonicalPath().startsWith(Main.volume.connicalPath)) {
					SDFSLogger.getLog().warn("in writefile connical path [" + f.getCanonicalPath()
							+ "] is not in folder structure " + Main.volume.connicalPath);
					f = new File(Main.volume.connicalPath + File.separator + f.getCanonicalPath());
				}
			} catch (IOException e) {
				SDFSLogger.getLog().error(
						"connical path [" + f.getPath() + "] is not in folder structure " + Main.volume.connicalPath,
						e);
				return false;
			}
			if (!f.isDirectory() && !this.isSymlink()) {
				/*
				 * try { throw new IOException("Writing mf ["+ f.getPath() +"] len=" +
				 * this.length()); }catch(Exception e) {
				 * SDFSLogger.getLog().warn("Writing mf ["+ f.getPath() +"] len=" +
				 * this.length(),e); }
				 */

				try {

					if (f.getParentFile() == null || !f.getParentFile().exists())
						f.getParentFile().mkdirs();
					FileIOServiceImpl.ImmuteLinuxFDFileFile(f.getPath(), false);
					FileOutputStream fout = null;
						fout = new FileOutputStream(this.path);
					out = new ObjectOutputStream(fout);
					out.writeObject(this);
					out.flush();
					out.close();
					try {
						fout.getFD().sync();
					} catch (Exception e) {

					}
					FileIOServiceImpl.ImmuteLinuxFDFileFile(f.getPath(), true);
					if (notify) {
						try {
							eventBus.post(new MFileWritten(this, this.dirty));
						} catch (Exception e) {
							SDFSLogger.getLog().error("unable to post mfilewritten " + path, e);
						}
					}
					this.dirty = false;

				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to write file metadata for [" + this.path + "]", e);
					return false;
				}
			}
		} finally {
			writeLock.unlock();
		}
		return true;
	}

	/**
	 * Serializes the file to the MetaFileStore
	 *
	 * @return true if serialized
	 */
	public boolean unmarshal() {
		return this.writeFile(true);
	}

	/**
	 *
	 * @return returns the GUID for the underlying DedupFile
	 */
	public String getDfGuid() {
		return dfGuid;
	}

	/**
	 *
	 * @return time when file was last modified
	 */
	/**
	 *
	 * @return time when file was last modified
	 */
	public long lastModified() {
		if (this.dfGuid != null)
			return lastModified.get();
		if (this.isDirectory())
			return new File(this.path).lastModified();
		return lastModified.get();
	}

	/**
	 *
	 * @return creates a blank new file
	 */
	public boolean createNewFile() {
		this.dirty = true;
		return this.unmarshal();
	}

	/**
	 *
	 * @return true if hidden
	 */
	public boolean isHidden() {
		return hidden;
	}

	/**
	 *
	 * @param hidden true if hidden
	 */
	public void setHidden(boolean hidden) {
		setHidden(hidden, true);
	}

	/**
	 *
	 * @param hidden         true if hidden
	 * @param propigateEvent TODO
	 */
	public void setHidden(boolean hidden, boolean propigateEvent) {
		this.writeLock.lock();
		try {
			this.hidden = hidden;
			this.dirty = true;
			this.unmarshal();
		} finally {
			this.writeLock.unlock();
		}
	}

	/**
	 *
	 * @return true if deleted
	 */
	public boolean deleteStub(boolean localonly) {
		this.writeLock.lock();
		try {
			if (this.retentionLock > 0)
				return false;
			File f = new File(this.path);
			if (f.exists()) {
				boolean del = f.delete();
				Main.volume.removeFile();
				if (!localonly)
					eventBus.post(new MFileDeleted(this));
				return del;
			} else
				return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	/**
	 *
	 * @param df the DedupFile that will be referenced within this file
	 */
	protected void setDedupFile(DedupFile df) {
		setDedupFile(df, true);
	}

	/**
	 *
	 * @param df             the DedupFile that will be referenced within this file
	 * @param propigateEvent TODO
	 */
	protected void setDedupFile(DedupFile df, boolean propigateEvent) {
		if (!df.getGUID().equalsIgnoreCase(this.dfGuid)) {

			this.dirty = true;
			this.dfGuid = df.getGUID();
		}
	}

	/**
	 *
	 * @return the children of this directory and null if it is not a directory.
	 */
	public String[] list() {
		File f = new File(this.path);
		if (f.isDirectory()) {
			return f.list();
		} else {
			return null;
		}
	}

	/**
	 *
	 * @return the children as MetaDataDedupFiles or null if it is not a directory
	 */
	public MetaDataDedupFile[] listFiles() {
		File f = new File(this.path);
		if (f.isDirectory()) {
			String[] files = f.list();
			MetaDataDedupFile[] df = new MetaDataDedupFile[files.length];
			for (int i = 0; i < df.length; i++) {

				try {
					df[i] = MetaFileStore.getMF(this.getPath() + File.separator + files[i]);
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to traverse " + this.getPath());
				}
			}
			return df;
		} else {
			return null;
		}
	}

	public boolean renameTo(String dest) throws IOException {
		return renameTo(dest, true);
	}

	public boolean renameTo(String dest, boolean propigateEvent) throws IOException {

		writeLock.lock();
		try {
			File f = new File(this.path);
			if (this.symlink) {
				Files.deleteIfExists(f.toPath());
				Path dstP = Paths.get(dest);
				Path srcP = Paths.get(this.path);
				try {
					Files.createSymbolicLink(dstP, srcP);
					return true;
				} catch (IOException e) {
					SDFSLogger.getLog().warn("unable to rename file to " + dest, e);
					return false;
				}
			} else if (f.isDirectory()) {
				eventBus.post(new MFileDeleted(this, true));
				boolean rn = f.renameTo(new File(dest));
				if (rn) {
					this.path = dest;
				}
				try {
					eventBus.post(new MFileWritten(this, this.dirty));
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to post mfilewritten " + path, e);
				}
				return rn;
			} else {
				String oldPath = f.getPath();
				String newPath = dest;
				// MetaFileStore.removeMetaFile(dest, true);
				if (this.dfGuid != null)
					DedupFileStore.updateDedupFile(this);
				boolean rename = f.renameTo(new File(dest));

				if (rename) {
					eventBus.post(new MFileRenamed(this, oldPath, newPath));
					this.dirty = true;
					eventBus.post(new MFileDeleted(this));
					SDFSLogger.getLog().debug("FileSystem rename succesful");

					this.path = dest;
					this.unmarshal();

				} else {
					SDFSLogger.getLog().warn("unable to move file " + f.getPath() + " to " + new File(dest).getPath());

				}
				return rename;
			}
		} finally {
			writeLock.unlock();
		}
	}

	public boolean exists() {
		writeLock.lock();

		try {
			return new File(this.path).exists();
		} finally {
			writeLock.unlock();
		}
	}

	public String getAbsolutePath() {
		return this.path;
	}

	public String getCanonicalPath() {
		return this.path;
	}

	public String getParent() {
		return new File(this.path).getParent();
	}

	public boolean canExecute() {
		return execute;
	}

	public boolean canRead() {
		return this.read;
	}

	public boolean canWrite() {

		return this.write;
	}

	public boolean setExecutable(boolean executable, boolean propigateEvent) {

		this.writeLock.lock();
		try {
			this.execute = executable;
			this.dirty = true;
			this.unmarshal();
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public boolean setWritable(boolean writable, boolean ownerOnly, boolean propigateEvent) {

		this.writeLock.lock();
		try {
			this.write = writable;
			this.ownerWriteOnly = ownerOnly;
			this.dirty = true;
			this.unmarshal();
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public boolean setWritable(boolean writable, boolean propigateEvent) {

		this.writeLock.lock();
		try {
			this.write = writable;
			this.dirty = true;
			this.unmarshal();
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public boolean setReadable(boolean readable, boolean ownerOnly, boolean propigateEvent) {

		this.writeLock.lock();
		try {

			this.read = readable;
			this.dirty = true;
			this.ownerReadOnly = ownerOnly;
			this.unmarshal();
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public boolean setReadable(boolean readable, boolean propigateEvent) {

		this.writeLock.lock();
		try {
			this.read = readable;
			this.dirty = true;
			this.unmarshal();
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public void setReadOnly(boolean propigateEvent) {

		this.read = true;
		this.dirty = true;
		this.unmarshal();
	}

	public boolean isFile() {
		return new File(this.path).isFile();
	}

	public boolean isDirectory() {
		return new File(this.path).isDirectory();
	}

	public String getName() {
		return new File(this.path).getName();

	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	/**
	 * @param lastModified the lastModified to set
	 */

	public boolean setLastModified(long lastModified) {
		return setLastModified(lastModified, true);
	}

	/**
	 * @param lastModified   the lastModified to set
	 * @param propigateEvent TODO
	 */

	public boolean setLastModified(long lastModified, boolean propigateEvent) {

		this.writeLock.lock();
		try {
			this.lastModified.set(lastModified);
			this.dirty = true;
			this.lastAccessed.set(lastModified);
			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	/**
	 * @return the length
	 */
	public long length() {
		return length;
	}

	/**
	 * @param length the length to set
	 */
	public void setLength(long l, boolean serialize) {
		setLength(l, serialize, true);
	}

	/**
	 * @param propigateEvent TODO
	 * @param length         the length to set
	 */
	public void setLength(long l, boolean serialize, boolean propigateEvent) {

		this.writeLock.lock();
		try {

			long len = l - this.length;
			Main.volume.updateCurrentSize(len, true);
			this.length = l;
			this.dirty = true;
			if (serialize)
				this.unmarshal();
		} finally {
			this.writeLock.unlock();
		}
	}

	/**
	 * @return the path to the file stub on disk
	 */
	public String getPath() {
		return path;
	}

	/**
	 *
	 * @param filePath the path to the file
	 * @return true if the file exists
	 */
	public static boolean exists(String filePath) {
		File f = new File(filePath);
		return f.exists();
	}

	public boolean isAbsolute() {
		return true;
	}

	@Override
	public int hashCode() {
		return new File(this.path).hashCode();
	}

	/**
	 * writes all the metadata and the Dedup blocks to the dedup chunk service
	 */
	public void sync() {
		sync(true);
	}

	/**
	 * writes all the metadata and the Dedup blocks to the dedup chunk service
	 *
	 * @param propigateEvent TODO
	 */
	public void sync(boolean propigateEvent) {
		if (this.dirty) {
			this.writeFile(propigateEvent);
		}
	}

	/**
	 *
	 * @param lastAccessed
	 */
	public void setLastAccessed(long lastAccessed) {
		setLastAccessed(lastAccessed, true);
	}

	public synchronized void setRetentionLock() {

		if (this.retentionLock <= 0) {
			this.dirty = true;
			this.retentionLock = System.currentTimeMillis();
		}
		// SDFSLogger.getLog().info("retention lock set to " + this.retentionLock);
	}

	public synchronized void clearRetentionLock() {
		this.retentionLock = -1;
	}

	public long getRetentionLock() {
		return this.retentionLock;
	}

	public boolean isRetentionLock() {
		if (this.retentionLock > 0)
			return true;
		else
			return false;
	}

	/**
	 *
	 * @param lastAccessed
	 * @param propigateEvent TODO
	 **/
	public void setLastAccessed(long lastAccessed, boolean propigateEvent) {
		this.writeLock.lock();
		try {
			this.lastModified.set(lastAccessed);
			this.dirty = true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public long getLastAccessed() {
		return lastAccessed.get();
	}

	public boolean isDirty() {
		return this.dirty;
	}

	public void readBytes(byte[] b) throws IOException, ClassNotFoundException {
		this.writeLock.lock();
		try {
			FileInfoResponse rsp = FileInfoResponse.parseFrom(b);
			this.length = rsp.getSize();
			this.lastModified.set(rsp.getMtime());
			this.lastAccessed.set(rsp.getAtime());
			this.execute = rsp.getExecute();
			this.read = rsp.getRead();
			this.write = rsp.getWrite();
			this.hidden = rsp.getHidden();
			try {
				this.dfGuid = rsp.getMapGuid();
			} catch (Exception e) {
				this.dfGuid = null;
			}
			this.guid = rsp.getFileGuild();
			this.monitor = new IOMonitor(this);
			if (rsp.getIoMonitor() != null) {
				this.monitor.fromGrpc(rsp.getIoMonitor());
			}

			this.owner_id = (int) rsp.getUserId();
			this.group_id = (int) rsp.getGroupId();
			this.extendedAttrs = new HashMap<String, String>();
			for (FileAttributes attr : rsp.getFileAttributesList()) {
				this.extendedAttrs.put(attr.getKey(), attr.getValue());
			}
			this.version = rsp.getVersion();
			this.attributes = rsp.getAttributes();
			this.mode = rsp.getMode();

			this.deleteOnClose = rsp.getDeleteOnClose();
			this.retentionLock = rsp.getRetentionLock();
			this.importing = rsp.getImporting();

		} finally {
			this.writeLock.unlock();
		}

	}

	public long getHashCode() {
		String fl = this.getPath().substring(Main.volume.getPath().length());
		byte[] stb = fl.getBytes();
		return HASHER.hash(stb, 0, stb.length, 6442);
	}

	private static final XXHash64 HASHER = CC.HASH_FACTORY.hash64();

	public static MetaDataDedupFile fromProtoBuf(FileInfoResponse resp, String path) throws IOException {
		MetaDataDedupFile mf = new MetaDataDedupFile(path);
		mf.setGroup_id((int) resp.getGroupId(), false);
		mf.setOwner_id((int) resp.getUserId(), false);
		mf.setPermissions(resp.getPermissions(), false);
		mf.setLastAccessed(resp.getAtime(), false);
		mf.setDfGuid(resp.getMapGuid(), false);
		mf.setExecutable(resp.getExecute(), false);
		mf.setHidden(resp.getHidden(), false);
		mf.setImporting(resp.getImporting());
		mf.setLastModified(resp.getMtime(), false);
		mf.setLength(resp.getSize(), false);
		mf.setMode(resp.getMode(), false);
		mf.setReadable(resp.getRead(), false);
		mf.setSymlink(false);
		mf.setWritable(resp.getWrite(), false);
		mf.unmarshal();
		return mf;
	}

	public FileInfoResponse toGRPC(boolean compact) throws IOException {
		FileInfoResponse.Builder b = FileInfoResponse.newBuilder();
		b.setFileName(this.getName());
		String fl = this.getPath().substring(Main.volume.getPath().length());
		String plp = "";
		if (fl.length() > 0) {
			plp = this.getParent().substring(Main.volume.getPath().length());
		}
		while (fl.startsWith("/") || fl.startsWith("\\"))
			fl = fl.substring(1, fl.length());
		if (plp.trim().length() == 0)
			plp = "##rootDir##";
		b.setId(Main.volume.getSerialNumber() + "/" + fl).setFilePath(fl).setParentPath(plp)
				.setMtime(this.lastModified());
		if (this.isFile()) {
			b.setType(fileType.FILE);
		} else if (this.isDirectory()) {
			b.setType(fileType.DIR);
		}
		byte[] stb = fl.getBytes();
		b.setHashcode(HASHER.hash(stb, 0, stb.length, 6442));
		b.setGroupId(this.getGroup_id()).setUserId(this.getOwner_id()).setPermissions(this.getPermissions());
		if (!compact && this.isFile()) {
			b.setAtime(this.getLastAccessed()).setCtime(this.getLastAccessed()).setHidden(this.hidden)
					.setSize(this.length()).setRead(this.read).setWrite(this.write).setLocalOwner(this.isFile())
					.setExecute(this.execute);
			b.setIoMonitor(this.getIOMonitor().toGRPC()).setLocalOwner(true).setAttributes(this.attributes)
					.setVersion(this.version).setDeleteOnClose(this.deleteOnClose);
			if (OSValidator.isWindows()) {
				b.setMode(511);
			} else {
				b.setMode(this.getMode());
			}
			try {
				b.setOpen(DedupFileStore.fileOpen(this));
			} catch (NullPointerException e) {
				b.setOpen(false);
			}
			if (this.extendedAttrs.size() > 0) {
				for (String key : this.extendedAttrs.keySet()) {

					if (key.trim().length() > 0) {
						FileAttributes.Builder fb = FileAttributes.newBuilder();
						fb.setKey(key.trim());
						fb.setValue(this.extendedAttrs.get(key));
						b.addFileAttributes(fb.build());
					}
				}
			}
			b.setFileGuild(this.getGUID()).setImporting(this.importing).setSymlink(this.symlink);
			if (this.symlink) {
				b.setSymlinkPath(this.getSymlinkPath());
			}
			if (this.getDfGuid() != null) {
				b.setMapGuid(this.getDfGuid());
			}
		} else if (!compact && this.isDirectory()) {
			Path p = Paths.get(this.getPath());
			File f = new File(this.getPath());
			b.setType(fileType.DIR);
			BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			b.setAtime(attrs.lastAccessTime().toMillis()).setMtime(attrs.lastModifiedTime().toMillis())
					.setCtime(attrs.creationTime().toMillis()).setHidden(f.isHidden()).setSize(f.length())
					.setSymlink(this.symlink);
			if (OSValidator.isWindows()) {
				b.setMode(511);
			} else {
				b.setMode(this.getMode());
			}
			if (this.symlink) {
				b.setSymlinkPath(this.getSymlinkPath());
			}
		} else if (!compact && this.isSymlink()) {
			Path p = Paths.get(this.getPath());
			b.setSymlinkPath(this.getSymlinkPath());
			b.setSymlink(this.symlink);
			File f = new File(this.getPath());
			BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			b.setAtime(attrs.lastAccessTime().toMillis()).setMtime(attrs.lastModifiedTime().toMillis())
					.setCtime(attrs.creationTime().toMillis()).setHidden(f.isHidden()).setSize(f.length())
					.setSymlink(this.symlink);
			if (OSValidator.isWindows()) {
				b.setMode(511);
			} else {
				b.setMode(this.getMode());
			}
		}
		return b.build();
	}

	public JsonObject toJSON(boolean compact) throws IOException {
		JsonObject dataset = new JsonObject();
		dataset.addProperty("file.name", this.getName());
		String fl = this.getPath().substring(Main.volume.getPath().length());
		String pl = "";
		if (fl.length() > 0) {
			pl = this.getParent().substring(Main.volume.getPath().length());
		}
		while (fl.startsWith("/") || fl.startsWith("\\"))
			fl = fl.substring(1, fl.length());
		if (pl.trim().length() == 0)
			pl = "##rootDir##";
		dataset.addProperty("id", Long.toString(Main.volume.getSerialNumber()) + "/" + fl);
		dataset.addProperty("file.path", fl);
		dataset.addProperty("file.path.parent", pl);
		dataset.addProperty("mtime", this.lastModified());
		dataset.addProperty("volumeid", Long.toString(Main.volume.getSerialNumber()));
		if (this.isFile())
			dataset.addProperty("type", "file");
		else if (this.isDirectory())
			dataset.addProperty("type", "dir");
		if (!compact && this.isFile()) {
			dataset.addProperty("atimeL", this.getLastAccessed());
			DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
			Instant instant = Instant.ofEpochMilli(this.getLastAccessed());
			dataset.addProperty("atime", formatter.format(instant));
			instant = Instant.ofEpochMilli(this.lastModified());
			dataset.addProperty("mtimeL", this.lastModified());
			dataset.addProperty("mtime", formatter.format(instant));
			instant = Instant.ofEpochMilli(0);
			dataset.addProperty("ctimeL", -1L);
			dataset.addProperty("ctime", formatter.format(instant));
			dataset.addProperty("hidden", this.isHidden());
			dataset.addProperty("size", this.length());
			dataset.addProperty("read", this.read);
			dataset.addProperty("write", this.write);
			dataset.addProperty("localowner", true);
			dataset.addProperty("execute", this.execute);
			this.getIOMonitor().toJson(dataset);
			try {
				dataset.addProperty("open", DedupFileStore.fileOpen(this));
			} catch (NullPointerException e) {
				dataset.addProperty("open", Boolean.toString(false));
			}
			if (this.extendedAttrs.size() > 0) {
				for (String key : this.extendedAttrs.keySet()) {
					if (key.trim().length() > 0) {
						if (StringUtils.isNumeric(this.extendedAttrs.get(key))) {
							long l = Long.parseLong(this.extendedAttrs.get(key));
							dataset.addProperty("extendedattrs." + key, l);
						} else if (this.extendedAttrs.get(key).length() < 50) {
							dataset.addProperty("extendedattrs." + key, this.extendedAttrs.get(key));
						}
					}
				}
			}
			dataset.addProperty("file.guid", this.getGUID());
			dataset.addProperty("dedupe.map.guid", this.getDfGuid());
			dataset.addProperty("vmdk", this.isVmdk());
			dataset.addProperty("importing", this.importing);
			if (symlink) {
				dataset.addProperty("symlink", this.isSymlink());
				dataset.addProperty("symlink-path", this.getSymlinkPath());
			}

		}
		if (!compact && this.isDirectory()) {
			Path p = Paths.get(this.getPath());
			File f = new File(this.getPath());
			dataset.addProperty("type", "directory");
			BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			dataset.addProperty("atime", attrs.lastAccessTime().toMillis());
			dataset.addProperty("mtime", attrs.lastModifiedTime().toMillis());
			dataset.addProperty("ctime", attrs.creationTime().toMillis());
			dataset.addProperty("hidden", f.isHidden());
			dataset.addProperty("size", attrs.size());
			if (symlink) {
				dataset.addProperty("symlink", this.isSymlink());
				dataset.addProperty("symlink.path", this.getSymlinkPath());
			}
		}
		return dataset;
	}

	public Element toXML(Document doc) throws ParserConfigurationException, DOMException, IOException {
		Element root = doc.createElement("file-info");

		root.setAttribute("file-name", URLEncoder.encode(this.getName(), "UTF-8"));
		String fl = this.getPath().substring(Main.volume.getPath().length());
		while (fl.startsWith("/") || fl.startsWith("\\"))
			fl = fl.substring(1, fl.length());
		root.setAttribute("file-path", fl);
		root.setAttribute("sdfs-path", URLEncoder.encode(this.getPath(), "UTF-8"));
		if (this.isFile()) {
			root.setAttribute("type", "file");
			root.setAttribute("atime", Long.toString(this.getLastAccessed()));
			root.setAttribute("mtime", Long.toString(this.lastModified()));
			root.setAttribute("ctime", Long.toString(-1L));
			root.setAttribute("hidden", Boolean.toString(this.isHidden()));
			root.setAttribute("size", Long.toString(this.length()));
			try {
				root.setAttribute("open", Boolean.toString(DedupFileStore.fileOpen(this)));
			} catch (NullPointerException e) {
				root.setAttribute("open", Boolean.toString(false));
			}
			root.setAttribute("file-guid", this.getGUID());
			root.setAttribute("dedup-map-guid", this.getDfGuid());
			root.setAttribute("vmdk", Boolean.toString(this.isVmdk()));
			root.setAttribute("localowner", Boolean.toString(true));
			root.setAttribute("execute", Boolean.toString(this.execute));
			root.setAttribute("read", Boolean.toString(this.read));
			root.setAttribute("write", Boolean.toString(this.write));
			root.setAttribute("importing", Boolean.toString(this.importing));
			if (!this.extendedAttrs.isEmpty()) {
				Element ear = doc.createElement("extended-attributes");
				for (Entry<String, String> en : this.extendedAttrs.entrySet()) {
					try {
						if (en.getKey().length() > 0) {
							Element ar = doc.createElement(StringEscapeUtils.escapeXml11(en.getKey()));
							ar.setAttribute("value", StringEscapeUtils.escapeXml11(en.getValue()));
							ear.appendChild(ar);
						}
					} catch (Exception e) {
						SDFSLogger.getLog()
								.warn("unable to xml encode key " + StringEscapeUtils.escapeXml11(en.getKey()), e);
					}

				}
				root.appendChild(ear);
			}
			if (symlink) {
				root.setAttribute("symlink", Boolean.toString(this.isSymlink()));
				root.setAttribute("symlink-path", this.getSymlinkPath());
			}

			Element monEl = this.getIOMonitor().toXML(doc);
			root.appendChild(monEl);
		}
		if (this.isDirectory()) {
			Path p = Paths.get(this.getPath());
			File f = new File(this.getPath());
			root.setAttribute("type", "directory");
			BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			root.setAttribute("atime", Long.toString(attrs.lastAccessTime().toMillis()));
			root.setAttribute("mtime", Long.toString(attrs.lastModifiedTime().toMillis()));
			root.setAttribute("ctime", Long.toString(attrs.creationTime().toMillis()));
			root.setAttribute("hidden", Boolean.toString(f.isHidden()));
			root.setAttribute("size", Long.toString(attrs.size()));
			if (symlink) {
				root.setAttribute("symlink", Boolean.toString(this.isSymlink()));
				root.setAttribute("symlink-path", this.getSymlinkPath());
			}
		}

		return root;
	}

	public boolean isSymlink() {
		return symlink;
	}

	public void setSymlink(boolean symlink) {
		setSymlink(symlink, true);
	}

	public void setSymlink(boolean symlink, boolean propigateEvent) {
		this.symlink = symlink;
	}

	public String getSymlinkPath() {
		return symlinkPath;
	}

	public void setSymlinkPath(String symlinkPath) {
		setSymlinkPath(symlinkPath, true);
	}

	public void setSymlinkPath(String symlinkPath, boolean propigateEvent) {
		this.symlinkPath = symlinkPath;
	}

	public long getAttributes() {
		return attributes;
	}

	public void setAttributes(long attributes) {
		this.attributes = attributes;
	}

	public boolean isImporting() {
		return importing;
	}

	public void setImporting(boolean importing) {
		this.importing = importing;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.writeLock.lock();
		try {

			in.readLong();
			this.length = in.readLong();
			this.lastModified.set(in.readLong());
			this.lastAccessed.set(in.readLong());
			this.execute = in.readBoolean();
			this.read = in.readBoolean();
			this.write = in.readBoolean();
			this.hidden = in.readBoolean();
			this.ownerWriteOnly = in.readBoolean();
			this.ownerExecOnly = in.readBoolean();
			this.ownerReadOnly = in.readBoolean();
			int dfgl = in.readInt();
			if (dfgl == -1) {
				this.dfGuid = null;
			} else {
				byte[] dfb = new byte[dfgl];
				in.readFully(dfb);
				this.dfGuid = new String(dfb);
			}
			int gl = in.readInt();
			byte[] gfb = new byte[gl];
			in.readFully(gfb);
			this.guid = new String(gfb);

			int ml = in.readInt();
			if (ml == -1) {
				this.monitor = null;
			} else {
				byte[] mlb = new byte[ml];
				in.readFully(mlb);
				this.monitor = new IOMonitor(this);
				monitor.fromByteArray(mlb);
			}
			this.vmdk = in.readBoolean();
			// owner id is ignored
			this.owner_id = in.readInt();
			// group id is ignored
			this.group_id = in.readInt();
			byte[] hmb = new byte[in.readInt()];
			in.readFully(hmb);
			this.extendedAttrs = ByteUtils.deSerializeHashMap(hmb);
			in.readBoolean();
			try {
				if (in.available() > 0) {
					int vlen = in.readInt();
					byte[] vb = new byte[vlen];
					in.readFully(vb);
					this.version = new String(vb);
				}
				if (in.available() > 0) {
					this.attributes = in.readLong();
				}
				if (in.available() > 0) {
					this.mode = in.readInt();
				}
				if (in.available() > 0) {
					this.deleteOnClose = in.readBoolean();
				}
				if (in.available() > 0) {
					in.readBoolean();
				}
				if (in.available() > 0) {
					this.retentionLock = in.readLong();
				}
				if (in.available() > 0) {
					this.importing = in.readBoolean();
				}
				if (in.available() > 0) {
					this.permissions = in.readInt();
				}
				

				/*
				 * if(in.available() > 0) { int vlen = in.readInt(); byte[] vb = new byte[vlen];
				 * in.readFully(vb); this.backingFile = new String(vb); this.iterWeaveCP =
				 * in.readBoolean(); }
				 */
			} catch (Exception e) {

			}
		} finally {
			this.writeLock.unlock();
		}

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		this.writeLock.lock();
		try {
			if (this.isSymlink())
				return;
			// SDFSLogger.getLog().info("writing out file=" + this.path + " df=" +
			// this.dfGuid);
			// SDFSLogger.getLog().info("writing out file=" + this.path + " df=" +
			// this.dfGuid + " length=" + this.length);
			out.writeLong(-1);
			out.writeLong(length);
			out.writeLong(lastModified.get());
			out.writeLong(lastAccessed.get());
			out.writeBoolean(execute);
			out.writeBoolean(read);
			out.writeBoolean(write);
			out.writeBoolean(hidden);
			out.writeBoolean(ownerWriteOnly);
			out.writeBoolean(ownerExecOnly);
			out.writeBoolean(ownerReadOnly);
			if (this.dfGuid != null) {
				byte[] dfb = this.dfGuid.getBytes();
				out.writeInt(dfb.length);
				out.write(dfb);
			} else {
				out.writeInt(-1);
			}
			byte[] dfb = this.guid.getBytes();
			out.writeInt(dfb.length);
			out.write(dfb);
			if (this.monitor != null) {
				byte[] mfb = this.monitor.toByteArray();
				out.writeInt(mfb.length);
				out.write(mfb);
			} else {
				out.writeInt(-1);
			}
			out.writeBoolean(vmdk);
			out.writeInt(this.getOwner_id());
			out.writeInt(this.getGroup_id());
			byte[] hmb = ByteUtils.serializeHashMap(extendedAttrs);
			out.writeInt(hmb.length);
			out.write(hmb);
			out.writeBoolean(true);
			byte[] vb = this.version.getBytes();
			out.writeInt(vb.length);
			out.write(vb);
			out.writeLong(attributes);
			out.writeInt(this.getMode());
			out.writeBoolean(this.deleteOnClose);
			out.writeBoolean(true);
			out.writeLong(this.retentionLock);
			out.writeBoolean(this.importing);
			out.writeInt(this.permissions);
			/*
			 * if(this.backingFile == null) out.writeInt(0); else { byte [] bb =
			 * this.backingFile.getBytes(); out.writeInt(bb.length); out.write(bb); }
			 * out.writeBoolean(this.iterWeaveCP);
			 */
		} finally {
			this.writeLock.unlock();
		}
	}
}