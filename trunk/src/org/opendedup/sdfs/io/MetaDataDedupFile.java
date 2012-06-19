package org.opendedup.sdfs.io;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Iterator;

import org.opendedup.util.SDFSLogger;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.monitor.IOMonitor;
import org.opendedup.util.ByteUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

/**
 * 
 * @author annesam Stores Meta-Data about a dedupFile. This class is modeled
 *         from the java.io.File class. Meta-Data files are stored within the
 *         MetaDataFileStore @see com.annesam.filestore.MetaDataFileStore
 */
public class MetaDataDedupFile implements java.io.Externalizable {

	private static final long serialVersionUID = -4598940197202968523L;
	transient public static final String pathSeparator = File.pathSeparator;
	transient public static final String separator = File.separator;
	transient public static final char pathSeparatorChar = File.pathSeparatorChar;
	transient public static final char separatorChar = File.separatorChar;
	protected long timeStamp = 0;
	private long length = 0;
	private String path = "";
	private long lastModified = 0;
	private long lastAccessed = 0;
	private boolean execute = true;
	private boolean read = true;
	private boolean write = true;
	private boolean directory = false;
	private boolean hidden = false;
	private boolean ownerWriteOnly = false;
	private boolean ownerExecOnly = false;
	private boolean ownerReadOnly = false;
	private String dfGuid = null;
	private String guid = "";
	private IOMonitor monitor;
	private boolean vmdk;
	private int permissions;
	private int owner_id = -1;
	private int group_id = -1;
	private int mode = -1;
	private HashMap<String, String> extendedAttrs = new HashMap<String, String>();
	private boolean dedup = Main.dedupFiles;
	private boolean symlink = false;
	private String symlinkPath = null;
	private String version = Main.version;

	/**
	 * 
	 * @return true if all chunks within the file will be deduped.
	 */
	public boolean isDedup() {
		return dedup;
	}

	public int getMode() throws IOException {
		if (mode == -1) {
			Path p = Paths.get(this.path);
			this.mode = (Integer) Files.getAttribute(p, "unix:mode");
		}
		return this.mode;
	}

	public void setMode(int mode) throws IOException {
		this.mode = mode;
		Path p = Paths.get(this.path);
		Files.setAttribute(p, "unix:mode", Integer.valueOf(mode));
	}

	/**
	 * 
	 * @param dedup
	 *            if true all chunks will be deduped, Otherwise chunks will be
	 *            deduped opportunistically.
	 * @throws IOException
	 */
	public void setDedup(boolean dedupNow) throws IOException,
			HashtableFullException {
		if (!this.dedup && dedupNow) {
			try {
				this.dedup = dedupNow;
				this.getDedupFile().optimize();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				SDFSLogger.getLog().error(
						"unable to set dedup on " + this.getPath(), e);
			}
		}
		this.dedup = dedupNow;
	}

	/**
	 * adds a posix extended attribute
	 * 
	 * @param name
	 *            the name of the attribute
	 * @param value
	 *            the value of the attribute
	 */
	public void addXAttribute(String name, String value) {
		extendedAttrs.put(name, value);
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
	 * @param permissions
	 *            sets permissions
	 */
	public void setPermissions(int permissions) {
		this.permissions = permissions;
	}

	/**
	 * 
	 * @return the file owner id
	 * @throws IOException
	 */
	public int getOwner_id() throws IOException {
		if (owner_id == -1) {
			Path p = Paths.get(this.path);
			this.owner_id = (Integer) Files.getAttribute(p, "unix:uid");
		}
		return owner_id;
	}

	/**
	 * 
	 * @param owner_id
	 *            sets the file owner id
	 * @throws IOException
	 */
	public void setOwner_id(int owner_id) throws IOException {
		this.owner_id = owner_id;
		Path p = Paths.get(this.path);
		Files.setAttribute(p, "unix:uid", Integer.valueOf(owner_id));
	}

	/**
	 * 
	 * @return returns the group owner id
	 * @throws IOException
	 */
	public int getGroup_id() throws IOException {
		if (group_id == -1) {
			Path p = Paths.get(this.path);
			this.group_id = (Integer) Files.getAttribute(p, "unix:gid");
		}
		return group_id;
	}

	/**
	 * 
	 * @param group_id
	 *            sets the group owner id
	 * @throws IOException
	 */
	public void setGroup_id(int group_id) throws IOException {
		this.group_id = group_id;
		Path p = Paths.get(this.path);
		Files.setAttribute(p, "unix:gid", Integer.valueOf(group_id));
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
	 * @param vmdk
	 *            flags this file as a vmdk if true
	 */
	public void setVmdk(boolean vmdk) {
		this.vmdk = vmdk;
	}

	public static MetaDataDedupFile getFile(String path) {
		File f = new File(path);
		MetaDataDedupFile mf = null;
		if (!f.exists() || f.isDirectory()) {
			mf = new MetaDataDedupFile(path);
		} else {
			ObjectInputStream in = null;
			try {
				in = new ObjectInputStream(new FileInputStream(path));

				mf = (MetaDataDedupFile) in.readObject();
				mf.path = path;
			} catch (Exception e) {
				SDFSLogger.getLog().fatal("unable to de-serialize " + path, e);
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
	 * @return returns the IOMonitor for this file. IOMonitors monitor
	 *         reads,writes, and dedup rate.
	 */
	public IOMonitor getIOMonitor() {
		if (monitor == null)
			monitor = new IOMonitor();
		return monitor;
	}

	/**
	 * 
	 * @param path
	 *            the path to the dedup file.
	 */
	private MetaDataDedupFile(String path) {
		init(path);
	}

	/**
	 * 
	 * @param path
	 *            the path to the dedup file.
	 */
	public MetaDataDedupFile() {
	}

	/**
	 * 
	 * @param parent
	 *            the parent folder
	 * @param child
	 *            the file name
	 */
	public MetaDataDedupFile(File parent, String child) {
		String pth = parent.getAbsolutePath() + File.separator + child;
		init(pth);
	}

	/**
	 * 
	 * @return the DedupFile associated with this file. It will create one if it
	 *         does not already exist.
	 * @throws IOException
	 */
	private ReentrantLock getDFLock = new ReentrantLock();

	public DedupFile getDedupFile() throws IOException {
		try {
			getDFLock.lock();
			if (this.dfGuid == null) {
				DedupFile df = DedupFileStore.getDedupFile(this);
				this.dfGuid = df.getGUID();
				SDFSLogger.getLog().debug(
						"No DF EXISTS .... Set dedup file for "
								+ this.getPath() + " to " + this.dfGuid);
				this.sync();
				return df;
			} else {
				return DedupFileStore.getDedupFile(this);
			}
		} finally {
			getDFLock.unlock();
		}
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
	 * @param snaptoPath
	 *            the path to clone to
	 * @param overwrite
	 *            if true, it will overwrite the destination file if it alreay
	 *            exists
	 * @return the new clone
	 * @throws IOException
	 */
	public MetaDataDedupFile snapshot(String snaptoPath, boolean overwrite)
			throws IOException {
		if (!this.isDirectory()) {
			SDFSLogger.getLog().debug("is snapshot file");
			File f = new File(snaptoPath);
			if (f.exists() && !overwrite)
				throw new IOException("path exists [" + snaptoPath
						+ "]Cannot overwrite existing data ");
			else if (f.exists()) {
				MetaFileStore.removeMetaFile(snaptoPath);
			}

			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			MetaDataDedupFile _mf = new MetaDataDedupFile(snaptoPath);
			_mf.directory = this.directory;
			_mf.execute = this.execute;
			_mf.hidden = this.hidden;
			_mf.lastModified = this.lastModified;
			_mf.setLength(this.length, false);
			_mf.ownerExecOnly = this.ownerExecOnly;
			_mf.ownerReadOnly = this.ownerReadOnly;
			_mf.ownerWriteOnly = this.ownerWriteOnly;
			_mf.timeStamp = this.timeStamp;
			_mf.read = this.read;
			_mf.write = this.write;
			_mf.owner_id = this.owner_id;
			_mf.group_id = this.group_id;
			_mf.permissions = this.permissions;
			_mf.dedup = this.dedup;
			this.getGUID();
			if (!this.dedup) {
				try {
					this.setDedup(true);
					try {
						_mf.dfGuid = DedupFileStore.cloneDedupFile(this, _mf)
								.getGUID();
					} catch (java.lang.NullPointerException e) {
						SDFSLogger.getLog().debug(
								"no dedupfile for " + this.path, e);
					}
				} catch (HashtableFullException e) {
					throw new IOException(e);
				} finally {
					try {
						this.setDedup(false);
					} catch (HashtableFullException e) {
						SDFSLogger
								.getLog()
								.error("error while setting dedup option back to false",
										e);
					}
				}
			} else {
				try {
					DedupFileStore.cloneDedupFile(this, _mf);
				} catch (java.lang.NullPointerException e) {
					SDFSLogger.getLog().debug("no dedupfile for " + this.path,
							e);
				}
			}
			_mf.getIOMonitor().setVirtualBytesWritten(
					this.getIOMonitor().getVirtualBytesWritten());
			_mf.getIOMonitor().setDuplicateBlocks(
					this.getIOMonitor().getDuplicateBlocks()
							+ this.getIOMonitor().getActualBytesWritten());
			Main.volume.addDuplicateBytes(this.getIOMonitor()
					.getDuplicateBlocks()
					+ this.getIOMonitor().getActualBytesWritten());
			Main.volume.addVirtualBytesWritten(this.getIOMonitor()
					.getVirtualBytesWritten());
			_mf.setVmdk(this.isVmdk());
			_mf.unmarshal();
			return _mf;
		} else {
			SDFSLogger.getLog().debug("is snapshot dir");
			File f = new File(snaptoPath);
			f.mkdirs();
			int trimlen = this.getPath().length();
			MetaDataDedupFile[] files = this.listFiles();
			for (int i = 0; i < files.length; i++) {
				MetaDataDedupFile file = files[i];
				String newPath = snaptoPath + File.separator
						+ file.getPath().substring(trimlen);
				file.snapshot(newPath, overwrite);
			}
			return MetaFileStore.getMF(snaptoPath);
		}
	}

	/**
	 * Clones a file and the underlying DedupFile
	 * 
	 * @param snaptoPath
	 *            the path to clone to
	 * @param overwrite
	 *            if true, it will overwrite the destination file if it alreay
	 *            exists
	 * @return the new clone
	 * @throws IOException
	 */
	public void copyTo(String npath, boolean overwrite) throws IOException {
		String snaptoPath = new File(npath + File.separator + "files")
				.getPath();
		SDFSLogger.getLog().info("Copying to " + snaptoPath);
		File f = new File(snaptoPath);
		if (f.exists() && !overwrite)
			throw new IOException("path exists [" + snaptoPath
					+ "]Cannot overwrite existing data ");

		if (!this.isDirectory()) {

			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();

			Path p = f.toPath();
			Files.copy(new File(this.path).toPath(), p,
					StandardCopyOption.REPLACE_EXISTING,
					StandardCopyOption.COPY_ATTRIBUTES);

			this.getDedupFile().copyTo(npath);
		} else {
			SDFSLogger.getLog().debug("is snapshot dir");
			if (!f.exists())
				f.mkdirs();
			String cpCmd = "cp -rf --preserve=mode,ownership,timestamps "
					+ this.path + " " + snaptoPath;
			Process p = Runtime.getRuntime().exec(cpCmd);
			try {
				int ecode = p.waitFor();
				if (ecode != 0)
					throw new IOException("unable to copy " + this.path
							+ " cp exit code is " + ecode);

			} catch (Exception e) {
				throw new IOException(e);
			}
			MetaDataDedupFile dmf = MetaDataDedupFile.getFile(snaptoPath);
			dmf.copyDir(npath);
		}
	}

	private void copyDir(String npath) throws IOException {
		String[] files = this.list();
		for (int i = 0; i < files.length; i++) {
			MetaDataDedupFile file = MetaDataDedupFile.getFile(this.getPath()
					+ File.separator + files[i]);
			if (file.isDirectory())
				file.copyDir(npath);
			else {
				SDFSLogger.getLog().debug(
						"copy dedup file for : " + file.getPath() + " guid :"
								+ file.getDfGuid());
				if (file.dfGuid != null) {
					if (DedupFileStore.fileOpen(file))
						file.getDedupFile().copyTo(npath);
					else {
						File sdbdirectory = new File(Main.dedupDBStore
								+ File.separator + file.dfGuid.substring(0, 2)
								+ File.separator + file.dfGuid);
						Path sdbf = new File(sdbdirectory.getPath()
								+ File.separator + file.dfGuid + ".map")
								.toPath();
						Path sdbc = new File(sdbdirectory.getPath()
								+ File.separator + file.dfGuid + ".chk")
								.toPath();
						File ddbdir = new File(npath + File.separator + "ddb"
								+ File.separator + file.dfGuid.substring(0, 2)
								+ File.separator + file.dfGuid);
						ddbdir.mkdirs();
						Path ddbf = new File(ddbdir.getPath() + File.separator
								+ file.dfGuid + ".map").toPath();
						Path ddbc = new File(ddbdir.getPath() + File.separator
								+ file.dfGuid + ".chk").toPath();
						Files.copy(sdbf, ddbf,
								StandardCopyOption.REPLACE_EXISTING,
								StandardCopyOption.COPY_ATTRIBUTES);
						Files.copy(sdbc, ddbc,
								StandardCopyOption.REPLACE_EXISTING,
								StandardCopyOption.COPY_ATTRIBUTES);
					}
				}
			}
		}
	}

	/**
	 * initiates the MetaDataDedupFile
	 * 
	 * @param path
	 *            the path to the file
	 */
	private void init(String path) {
		this.lastAccessed = System.currentTimeMillis();
		this.path = path;
		File f = new File(path);
		if (!f.exists()) {
			SDFSLogger.getLog().debug("Creating new MetaFile for " + this.path);
			this.guid = UUID.randomUUID().toString();
			monitor = new IOMonitor();
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.permissions = Main.defaultFilePermissions;
			this.lastModified = System.currentTimeMillis();
			this.dedup = Main.dedupFiles;
			this.timeStamp = System.currentTimeMillis();
			this.setLength(0, false);
			this.sync();
		} else if (f.isDirectory()) {
			this.permissions = Main.defaultDirPermissions;
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.directory = true;
			this.length = 4096;
		}

	}

	private ReentrantLock writeLock = new ReentrantLock();

	/**
	 * Writes the stub for this file to disk. Stubs are pointers written to a
	 * file system that map to virtual filesystem directory and file structure.
	 * The stub only contains the guid associated with the file in question.
	 * 
	 * @return true if written
	 */
	private boolean writeFile() {
		writeLock.lock();
		ObjectOutputStream out = null;
		try {
			File f = new File(this.path);
			if (!f.isDirectory()) {
				if (!f.getParentFile().exists())
					f.getParentFile().mkdirs();

				try {
					out = new ObjectOutputStream(
							new FileOutputStream(this.path));
					out.writeObject(this);
				} catch (Exception e) {
					SDFSLogger.getLog().warn(
							"unable to write file metadata for [" + this.path
									+ "]", e);
					return false;
				}
			}
		} finally {
			if (out != null)
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
				}
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
		return this.writeFile();
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
	public long lastModified() {
		if (this.isDirectory())
			return new File(this.path).lastModified();
		return lastModified;
	}

	/**
	 * 
	 * @return creates a blank new file
	 */
	public boolean createNewFile() {
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
	 * @param hidden
	 *            true if hidden
	 */
	public void setHidden(boolean hidden) {
		this.hidden = hidden;
		this.unmarshal();
	}

	/**
	 * 
	 * @return true if deleted
	 */
	public boolean deleteStub() {
		File f = new File(this.path);
		return f.delete();
	}

	/**
	 * 
	 * @param df
	 *            the DedupFile that will be referenced within this file
	 */
	protected void setDedupFile(DedupFile df) {
		this.dfGuid = df.getGUID();
	}

	/**
	 * Delete this file only. This is used to delete the file in question but
	 * not the DedupFile Reference
	 * 
	 */
	protected boolean deleteSelf() {
		File f = new File(this.path);
		MetaFileStore.removedCachedMF(this.path);
		return f.delete();
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
	 * @return the children as MetaDataDedupFiles or null if it is not a
	 *         directory
	 */
	public MetaDataDedupFile[] listFiles() {
		File f = new File(this.path);
		if (f.isDirectory()) {
			String[] files = f.list();
			MetaDataDedupFile[] df = new MetaDataDedupFile[files.length];
			for (int i = 0; i < df.length; i++) {

				df[i] = MetaFileStore.getMF(this.getPath() + File.separator
						+ files[i]);
			}
			return df;
		} else {
			return null;
		}
	}

	public boolean mkdir() {
		this.directory = true;
		File f = new File(this.path);
		return f.mkdir();
	}

	public boolean mkdirs() {
		this.directory = true;
		File f = new File(this.path);
		return f.mkdirs();
	}

	public boolean renameTo(String dest) {
		File f = new File(this.path);
		if (this.symlink) {
			f = new File(this.symlinkPath);
			f.delete();
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
			return f.renameTo(new File(dest));
		} else {
			boolean rename = f.renameTo(new File(dest));
			if (rename) {
				MetaFileStore.rename(this.path, dest, this);
				this.path = dest;
				this.unmarshal();
			} else {
				SDFSLogger.getLog().warn("unable to move file");
			}
			return rename;
		}
	}

	public boolean exists() {
		return new File(this.path).exists();
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

	public boolean setExecutable(boolean executable, boolean ownerOnly) {
		this.execute = executable;
		this.ownerExecOnly = ownerOnly;
		this.unmarshal();
		return true;
	}

	public boolean setExecutable(boolean executable) {
		this.execute = executable;
		this.unmarshal();
		return true;
	}

	public boolean setWritable(boolean writable, boolean ownerOnly) {
		this.write = writable;
		this.ownerWriteOnly = ownerOnly;
		this.unmarshal();
		return true;
	}

	public boolean setWritable(boolean writable) {
		this.write = writable;
		this.unmarshal();
		return true;
	}

	public boolean setReadable(boolean readable, boolean ownerOnly) {
		this.read = readable;
		this.ownerReadOnly = ownerOnly;
		this.unmarshal();
		return true;
	}

	public boolean setReadable(boolean readable) {
		this.read = readable;
		this.unmarshal();
		return true;
	}

	public void setReadOnly() {
		this.read = true;
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

	/**
	 * @param lastModified
	 *            the lastModified to set
	 */

	public boolean setLastModified(long lastModified) {
		this.lastModified = lastModified;
		this.lastAccessed = lastModified;
		return true;
	}

	/**
	 * @return the timeStamp
	 */
	public long getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(long timeStamp, boolean serialize) {
		this.timeStamp = timeStamp;
		if (serialize)
			this.unmarshal();
	}

	/**
	 * @return the length
	 */
	public long length() {
		return length;
	}

	/**
	 * @param length
	 *            the length to set
	 */
	public void setLength(long l, boolean serialize) {

		long len = l - this.length;
		Main.volume.updateCurrentSize(len);
		this.length = l;
		if (serialize)
			this.unmarshal();
	}

	/**
	 * @return the path to the file stub on disk
	 */
	public String getPath() {
		return path;
	}

	/**
	 * 
	 * @param filePath
	 *            the path to the file
	 * @return true if the file exists
	 */
	public static boolean exists(String filePath) {
		File f = new File(filePath);
		return f.exists();
	}

	public boolean isAbsolute() {
		return true;
	}

	public int hashCode() {
		return new File(this.path).hashCode();
	}

	/**
	 * writes all the metadata and the Dedup blocks to the dedup chunk service
	 */
	public void sync() {
		this.unmarshal();
	}

	/**
	 * 
	 * @param lastAccessed
	 */
	public void setLastAccessed(long lastAccessed) {
		this.lastAccessed = lastAccessed;
	}

	public long getLastAccessed() {
		return lastAccessed;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.timeStamp = in.readLong();
		this.length = in.readLong();
		this.lastModified = in.readLong();
		this.lastAccessed = in.readLong();
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
			in.read(dfb);
			this.dfGuid = new String(dfb);
		}
		int gl = in.readInt();
		byte[] gfb = new byte[gl];
		in.read(gfb);
		this.guid = new String(gfb);

		int ml = in.readInt();
		if (ml == -1) {
			this.monitor = null;
		} else {
			byte[] mlb = new byte[ml];
			in.read(mlb);
			this.monitor = new IOMonitor();
			monitor.fromByteArray(mlb);
		}
		this.vmdk = in.readBoolean();
		// owner id is ignored
		in.readInt();
		// group id is ignored
		in.readInt();
		byte[] hmb = new byte[in.readInt()];
		in.read(hmb);
		this.extendedAttrs = ByteUtils.deSerializeHashMap(hmb);
		this.dedup = in.readBoolean();
		try {
			if (in.available() > 0) {
				int vlen = in.readInt();
				byte[] vb = new byte[vlen];
				in.read(vb);
				this.version = new String(vb);
			}
		} catch (Exception e) {

		}

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(timeStamp);
		out.writeLong(length);
		out.writeLong(lastModified);
		out.writeLong(lastAccessed);
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
		out.writeInt(owner_id);
		out.writeInt(group_id);
		byte[] hmb = ByteUtils.serializeHashMap(extendedAttrs);
		out.writeInt(hmb.length);
		out.write(hmb);
		out.writeBoolean(dedup);
		byte[] vb = this.version.getBytes();
		out.writeInt(vb.length);
		out.write(vb);

	}

	public Element toXML(Document doc) throws ParserConfigurationException,
			DOMException, IOException {
		Element root = doc.createElement("file-info");
		root.setAttribute("file-name", this.getName());
		root.setAttribute("sdfs-path", this.getPath());
		if (this.isFile()) {
			root.setAttribute("type", "file");
			root.setAttribute("atime", Long.toString(this.getLastAccessed()));
			root.setAttribute("mtime", Long.toString(this.lastModified()));
			root.setAttribute("ctime", Long.toString(this.getTimeStamp()));
			root.setAttribute("hidden", Boolean.toString(this.isHidden()));
			root.setAttribute("size", Long.toString(this.length()));
			try {
				root.setAttribute("open",
						Boolean.toString(DedupFileStore.fileOpen(this)));
			} catch (NullPointerException e) {
				root.setAttribute("open", Boolean.toString(false));
			}
			root.setAttribute("file-guid", this.getGUID());
			root.setAttribute("dedup-map-guid", this.getDfGuid());
			root.setAttribute("dedup", Boolean.toString(this.isDedup()));
			root.setAttribute("vmdk", Boolean.toString(this.isVmdk()));
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
			BasicFileAttributes attrs = Files.readAttributes(p,
					BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			root.setAttribute("atime",
					Long.toString(attrs.lastAccessTime().toMillis()));
			root.setAttribute("mtime",
					Long.toString(attrs.lastModifiedTime().toMillis()));
			root.setAttribute("ctime",
					Long.toString(attrs.creationTime().toMillis()));
			root.setAttribute("hidden", Boolean.toString(f.isHidden()));
			root.setAttribute("size", Long.toString(attrs.size()));
			if (symlink) {
				root.setAttribute("symlink", Boolean.toString(this.isSymlink()));
				root.setAttribute("symlink-source", this.getSymlinkPath());
			}
		}

		return root;
	}

	public boolean isSymlink() {
		return symlink;
	}

	public void setSymlink(boolean symlink) {
		this.symlink = symlink;
	}

	public String getSymlinkPath() {
		return symlinkPath;
	}

	public void setSymlinkPath(String symlinkPath) {
		this.symlinkPath = symlinkPath;
	}
}
