package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.Volume;

public class VolumeWritten {
	
	public Volume vol;
	public VolumeWritten(Volume v) {
		this.vol = v;
	}

}
