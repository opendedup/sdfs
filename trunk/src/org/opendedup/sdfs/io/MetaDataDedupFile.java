package org.opendedup.sdfs.io;

import java.io.File;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;

import java.io.*;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.monitor.IOMonitor;

/**
 * 
 * @author annesam Stores Meta-Data about a dedupFile. This class is modeled
 *         from the java.io.File class. Meta-Data files are stored within the
 *         MetaDataFileStore @see com.annesam.filestore.MetaDataFileStore
 */
public class MetaDataDedupFile implements java.io.Serializable {

	private static final long serialVersionUID = -4598940197202968523L;
	transient public static final String pathSeparator = File.pathSeparator;
	transient public static final String separator = File.separator;
	transient public static final char pathSeparatorChar = File.pathSeparatorChar;
	transient public static final char separatorChar = File.separatorChar;
	transient private static Logger log = Logger.getLogger("sdfs");
	protected long timeStamp = 0;
	private long length = 0;
	private String path = "";
	private long lastModified = 0;
	private long lastAccessed = 0;
	private boolean execute = true;
	private boolean read = true;
	private boolean write = true;
	private boolean directory = false;
	private boolean file = true;
	private boolean hidden = false;
	private boolean ownerWriteOnly = false;
	private boolean ownerExecOnly = false;
	private boolean ownerReadOnly = false;
	private String name;
	private String dfGuid = null;
	private String guid = "";
	private IOMonitor monitor;
	private boolean vmdk;
	private VMDKData vmdkData;
	private int permissions;
	private int owner_id;
	private int group_id;
	private HashMap<String, String> extendedAttrs = new HashMap<String, String>();
	private boolean dedup = Main.dedupFiles;

	/**
	 * 
	 * @return true if all chunks within the file will be deduped.
	 */
	public boolean isDedup() {
		return dedup;
	}

	/**
	 * 
	 * @param dedup
	 *            if true all chunks will be deduped, Otherwise chunks will be
	 *            deduped opportunistically.
	 */
	public void setDedup(boolean dedupNow) {
		if (!this.dedup && dedupNow) {
			try {
				this.dedup = dedupNow;
				this.getDedupFile().optimize(this.length);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
			return "-1";
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
	 */
	public int getOwner_id() {
		return owner_id;
	}

	/**
	 * 
	 * @param owner_id
	 *            sets the file owner id
	 */
	public void setOwner_id(int owner_id) {
		this.owner_id = owner_id;
	}

	/**
	 * 
	 * @return returns the group owner id
	 */
	public int getGroup_id() {
		return group_id;
	}

	/**
	 * 
	 * @param group_id
	 *            sets the group owner id
	 */
	public void setGroup_id(int group_id) {
		this.group_id = group_id;
	}

	/**
	 * 
	 * @param data
	 *            sets the VMDK specific data for this file
	 */
	public void setVmdkData(VMDKData data) {
		this.vmdkData = data;
	}

	/**
	 * 
	 * @return returns the VMDK specific data for this file
	 */
	public VMDKData getVmdkData() {
		return this.vmdkData;
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

	public MetaDataDedupFile() {
	}

	/**
	 * 
	 * @return returns the IOMonitor for this file. IOMonitors monitor
	 *         reads,writes, and dedup rate.
	 */
	public IOMonitor getIOMonitor() {
		if (monitor == null)
			monitor = new IOMonitor(this);
		return monitor;
	}

	/**
	 * 
	 * @param path
	 *            the path to the dedup file.
	 */
	public MetaDataDedupFile(String path) {
		init(path);
	}

	/**
	 * 
	 * @param parent
	 *            parent folder
	 * @param child
	 *            the file name
	 */
	public MetaDataDedupFile(String parent, String child) {
		String pth = parent + File.separator + child;
		init(pth);
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

	public synchronized DedupFile getDedupFile() throws IOException {
		if (this.dfGuid == null) {
			DedupFile df = DedupFileStore.getDedupFile(this);
			this.dfGuid = df.getGUID();
			log.finer("No DF EXISTS .... Set dedup file for " + this.getPath()
					+ " to " + this.dfGuid);
			this.sync();
			return df;
		} else {
			return DedupFileStore.getDedupFile(this);
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
			File f = new File(snaptoPath);
			if (f.exists() && !overwrite)
				throw new IOException("path exists [" + snaptoPath
						+ "]Cannot overwrite existing data ");
			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			MetaDataDedupFile _mf = new MetaDataDedupFile(snaptoPath);
			_mf.directory = this.directory;
			_mf.execute = this.execute;
			_mf.file = true;
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
			_mf.dfGuid = DedupFileStore.cloneDedupFile(this, _mf).getGUID();
			_mf.getIOMonitor().setVirtualBytesWritten(this.length());
			_mf.getIOMonitor().setDuplicateBlocks(this.getIOMonitor().getDuplicateBlocks());
			_mf.setVmdk(this.isVmdk());
			if (this.isVmdk())
				_mf.setVmdkData(this.getVmdkData().clone());
			_mf.unmarshal();

			return _mf;
		} else {
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
	 * initiates the MetaDataDedupFile
	 * 
	 * @param path
	 *            the path to the file
	 */
	private void init(String path) {
		this.lastAccessed = System.currentTimeMillis();
		this.path = path;
		this.name = path.substring(path.lastIndexOf(File.separator) + 1);
		File f = new File(path);
		if (!f.exists()) {
			log.finer("Creating new MetaFile for " + this.path);
			this.guid = UUID.randomUUID().toString();
			monitor = new IOMonitor(this);
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.permissions = Main.defaultFilePermissions;
			this.lastModified = System.currentTimeMillis();
			this.dedup = Main.dedupFiles;
			this.setLength(0, false);
		} else if (f.isDirectory()) {
			this.permissions = Main.defaultDirPermissions;
			this.owner_id = Main.defaultOwner;
			this.group_id = Main.defaultGroup;
			this.directory = true;
			this.length = 4096;
		} else if (f.isFile()) {
			this.mashal();
		}
		this.timeStamp = System.currentTimeMillis();
	}

	/**
	 * Writes the stub for this file to disk. Stubs are pointers written to a
	 * file system that map to virtual filesystem directory and file structure.
	 * The stub only contains the guid associated with the file in question.
	 * 
	 * @return true if written
	 */
	private boolean writeStub() {
		Stub stub = new Stub(this.guid);
		File f = new File(this.path);
		if (!f.isDirectory()) {
			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();

			try {
				FileWriter writer = new FileWriter(this.path);
				writer.write(stub.getGUID().trim() + "\n");
				try {
					writer.close();
					writer = null;
				} catch (Exception e) {

				}
				return true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	/**
	 * Reads the guid from the stub file
	 * 
	 * @return the guid associated with the file.
	 * @throws IOException
	 */
	private String getStubGuid() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.path));
		String _guid = reader.readLine();
		reader.close();
		reader = null;
		return _guid;
	}

	/**
	 * Serializes the file to the MetaFileStore
	 * 
	 * @return true if serialized
	 */
	private boolean unmarshal() {
		File f = new File(path);
		if (!f.exists()) {
			this.writeStub();
		}
		MetaFileStore.setMetaFile(this);
		return true;
	}

	/**
	 * Gets the meta-data from the MetaFileStore
	 */
	protected void mashal() {
		try {
			MetaDataDedupFile df = MetaFileStore
					.getMetaFile(this.getStubGuid());
			if (df != null) {
				this.guid = df.guid;
				this.directory = df.directory;
				this.execute = df.execute;
				this.file = true;
				this.hidden = df.hidden;
				this.lastModified = df.lastModified;
				this.length = df.length;
				this.ownerExecOnly = df.ownerExecOnly;
				this.ownerReadOnly = df.ownerReadOnly;
				this.ownerWriteOnly = df.ownerWriteOnly;
				this.timeStamp = df.timeStamp;
				this.read = df.read;
				this.write = df.write;
				this.dfGuid = df.dfGuid;
				this.vmdk = df.vmdk;
				this.vmdkData = df.vmdkData;
				this.dedup = df.dedup;
				this.permissions = df.getPermissions();
				this.owner_id = df.getOwner_id();
				this.group_id = df.getGroup_id();
				this.monitor = df.getIOMonitor();
				df = null;
			} else {
				log.severe("unable to find metafile for " + this.path);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	 * @param removeMeta
	 *            if true removes the file, otherwise it will remove just the
	 *            cached object from the MetaFileStore.
	 * @return true if done
	 */
	public synchronized boolean delete(boolean removeMeta) {
		try {
			log.finer("deleting " + this.path);
			MetaFileStore.removedCachedMF(this.path);
			if (removeMeta) {
				MetaFileStore.removeDedupFile(this.getPath());
				if (this.dfGuid != null) {
					this.getDedupFile().delete();
					DedupFileStore.removeOpenDedupFile(this);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		Main.volume.updateCurrentSize(-1 * this.length());
		File f = new File(this.path);
		return f.delete();
	}

	/**
	 * 
	 * @return true if deleted
	 */
	public boolean delete() {
		return this.delete(true);
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
	protected synchronized boolean deleteSelf() {
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
		File f = new File(this.path);
		return f.mkdirs();
	}

	public boolean renameTo(MetaDataDedupFile dest) {
		File f = new File(this.path);
		if (f.isDirectory()) {
			return f.renameTo(new File(dest.getPath()));
		} else {
			try {
				dest.delete();
			} catch (Exception e) {

			}
			boolean rename = f.renameTo(new File(dest.getPath()));
			if (rename) {
				MetaFileStore.removedCachedMF(this.path);
				this.path = dest.getPath();
				this.unmarshal();
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
		return name;

	}

	/**
	 * @param lastModified
	 *            the lastModified to set
	 */

	public boolean setLastModified(long lastModified, boolean serialize) {
		this.lastModified = lastModified;
		this.lastAccessed = lastModified;
		if (serialize)
			this.unmarshal();
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

	private class Stub {
		private String guid;

		Stub(String guid) {
			this.guid = guid;
		}

		private String getGUID() {
			return this.guid;
		}

	}
}
