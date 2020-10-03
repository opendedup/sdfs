package org.opendedup.sdfs.filestore.cloud;

import java.util.Map;

public class RemoteVolumeInfo {
	public long id;
	public String hostname;
	public int port;
	public long data;
	public long compressed;
	public int version;
	public String sdfsversion;
	public long lastupdated;
	public Map<String,String> metaData;

}
