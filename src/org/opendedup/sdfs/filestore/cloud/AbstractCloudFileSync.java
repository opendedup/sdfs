package org.opendedup.sdfs.filestore.cloud;

import java.io.File;

import java.io.IOException;
import java.util.HashMap;

public interface AbstractCloudFileSync {
	public abstract void uploadFile(File f, String to, String parentPath,HashMap<String,String> md,boolean disableComp)
			throws IOException;
	
	public void addRefresh(long id);
	
	public abstract void checkoutFile(String name) throws IOException;
	
	public abstract boolean isCheckedOut(String name,long volumeID) throws IOException;

	public abstract void downloadFile(String name, File to, String parentPath)
			throws IOException;
	
	public abstract void setCredentials(String accessKey,String secretKey);

	public abstract String getNextName(String prefix,long id) throws IOException;
	
	public abstract void removeVolume(long volumeID) throws IOException;

	public abstract void clearIter();

	public abstract void deleteFile(String name, String parentPath)
			throws IOException;

	public abstract void renameFile(String from, String to, String parentPath)
			throws IOException;
	
	public abstract RemoteVolumeInfo[] getConnectedVolumes()throws IOException;

}
