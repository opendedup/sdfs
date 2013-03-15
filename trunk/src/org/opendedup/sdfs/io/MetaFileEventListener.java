package org.opendedup.sdfs.io;

import org.opendedup.sdfs.notification.SDFSEvent;

public interface MetaFileEventListener {
	void onCreate(boolean dedup,MetaDataDedupFile mf);
	void onDedupChange(boolean dedup,MetaDataDedupFile mf);
	void onDedupFileChange(String guid,MetaDataDedupFile mf);
	void onExecutableChange(boolean executable,MetaDataDedupFile mf);
	void onExecutableChange(boolean executable, boolean ownerOnly,MetaDataDedupFile mf);
	void onWritabeChange(boolean writable, boolean ownerOnly,MetaDataDedupFile mf);
	void onGroupChange(int grp,MetaDataDedupFile mf);
	void onHiddenChange(boolean hidden,MetaDataDedupFile mf);
	void onLastAccessedChange(long tm,MetaDataDedupFile mf);
	void onLastModifiedChange(long tm,MetaDataDedupFile mf);
	void onLengthChange(long length,boolean persist,MetaDataDedupFile mf);
	void onModeChange(int mode,MetaDataDedupFile mf);
	void onOwnerChange(int owner,MetaDataDedupFile mf);
	void onPermissionsChange(int permissions,MetaDataDedupFile mf);
	void onReadableChange(boolean readable,MetaDataDedupFile mf);
	void onReadableChange(boolean readable, boolean ownerOnly,MetaDataDedupFile mf);
	void onReadOnly(MetaDataDedupFile mf);
	void onSymlinkChange(boolean isSymlink,MetaDataDedupFile mf);
	void onSymlinkPathChange(String path,MetaDataDedupFile mf);
	void onTimeStampChange(long timestamp,boolean persist,MetaDataDedupFile mf);
	void onSetVMDK(boolean isVMDK,MetaDataDedupFile mf);
	void onWritableChange(String writable,MetaDataDedupFile mf);
	void onSnapshotExecuted(String snaptoPath, boolean overwrite, SDFSEvent evt,MetaDataDedupFile mf);
	void onSync(MetaDataDedupFile mf);
	void onDelete(MetaDataDedupFile mf);
	void onRenameTo(String dst);
	void onMkDir(MetaDataDedupFile mf);
	void onMkDirs(MetaDataDedupFile mf);
	void onDfGuidChange(String guid,MetaDataDedupFile mf);
	void onAddXAttribute(String name, String value,MetaDataDedupFile mf);
	void onCopyTo(String npath, boolean overwrite,MetaDataDedupFile mf);
}
