package org.opendedup.sdfs.mgmt;

import java.util.HashMap;

public class SDFSTask {
	public String statusType = null;
	public String statusMsg = null;
	public String startTime = null;
	public String endTime = null;
	public static final String GC= "gc";
	public static final String MOUNT ="Mount Volume";
	public static final String FIXDSE ="Volume Recovery Task";
	public static final String SNAP ="Take Snapshot";
	public static final String EXPANDVOL ="Expand Volume";
	public static final String DELFILE ="Delete File";
	private static HashMap<String,SDFSTask> runningTasks = new HashMap<String,SDFSTask>();

}
