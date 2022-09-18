package org.opendedup.sdfs.io.events;

import org.opendedup.grpc.Storage.VolumeEvent;

public class ReplEvent extends GenericEvent { 
    VolumeEvent ve = null;
    public ReplEvent(VolumeEvent ve) {
		super();
		this.ve = ve;
	}

    public VolumeEvent getVolumeEvent() {
        return this.ve;
    }
    
}
