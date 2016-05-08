package org.opendedup.sdfs.filestore.cloud;

import java.io.File;

import java.io.IOException;

public interface AbstractCloudFileSync {
	public abstract void uploadFile(File f, String to, String parentPath)
			throws IOException;
	
	public abstract void checkoutFile(String name) throws IOException;
	
	public abstract boolean isCheckedOut(String name,long volumeID) throws IOException;

	public abstract void downloadFile(String name, File to, String parentPath)
			throws IOException;

	public abstract String getNextName(String prefix,long id) throws IOException;

	public abstract void clearIter();

	public abstract void deleteFile(String name, String parentPath)
			throws IOException;

	public abstract void renameFile(String from, String to, String parentPath)
			throws IOException;
	
	public abstract RemoteVolumeInfo[] getConnectedVolumes()throws IOException;

}
