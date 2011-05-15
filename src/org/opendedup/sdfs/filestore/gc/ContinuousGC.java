package org.opendedup.sdfs.filestore.gc;

public class ContinuousGC implements GCControllerImpl {

	@Override
	public void runGC() {
		ManualGC.clearChunks(1);
	}

}
