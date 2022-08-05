package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class SyncFromConnectedVolume implements Runnable {
	public SDFSEvent evt;
	public long volumeid;
	public boolean overwrite;

	public void getResult(long id) throws IOException {
			RemoteVolumeInfo[] l = FileReplicationService.getConnectedVolumes();
			for (RemoteVolumeInfo lv : l) {
				if (lv.id == id) {
					if(evt == null)
						evt = SDFSEvent.syncVolEvent("Syncing from [" + id + "]");
					HCServiceProxy.syncVolume(id,true,overwrite,evt);
					return;
				}
			}
			throw new IOException("volume [" + id + "] not found");
		
	}

	public void syncVolume(long id) {
		this.volumeid = id;
		evt = SDFSEvent.syncVolEvent("Syncing from [" + id + "]");
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			this.getResult(volumeid);
		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}


	}

}
