package org.opendedup.sdfs.replication;

import java.util.concurrent.ConcurrentHashMap;

public class ReplicationSnapShotManager {
	public static ConcurrentHashMap<String,String> replicationSnapshots = new ConcurrentHashMap<String,String>();

	public void addSnapshot(){}
	
}
