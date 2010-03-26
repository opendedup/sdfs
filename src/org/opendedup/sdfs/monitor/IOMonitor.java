package org.opendedup.sdfs.monitor;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class IOMonitor implements java.io.Serializable {

	private static final long serialVersionUID = 6582549274733666474L;
	private long virtualBytesWritten;
	private long actualBytesWritten;
	private long bytesRead;
	private long duplicateBlocks;
	private MetaDataDedupFile mf;

	public IOMonitor(MetaDataDedupFile mf) {
		this.mf = mf;
	}

	public long getVirtualBytesWritten() {
		return virtualBytesWritten;
	}

	public long getActualBytesWritten() {
		return actualBytesWritten;
	}

	public long getBytesRead() {
		return bytesRead;
	}

	public void addBytesRead(int len) {
		this.bytesRead = this.bytesRead + len;
	}

	public void addActualBytesWritten(int len) {
		this.actualBytesWritten = this.actualBytesWritten + len;
	}

	public void addVirtualBytesWritten(int len) {
		this.virtualBytesWritten = this.virtualBytesWritten + len;
	}

	public void setVirtualBytesWritten(long len) {
		this.virtualBytesWritten = len;
	}

	public long getDuplicateBlocks() {
		return duplicateBlocks;
	}

	public void setDuplicateBlocks(long duplicateBlocks) {
		this.duplicateBlocks = duplicateBlocks;
	}

	public void setActualBytesWritten(long actualBytesWritten) {
		this.actualBytesWritten = actualBytesWritten;
	}

	public void setBytesRead(long bytesRead) {
		this.bytesRead = bytesRead;
	}

	public void removeDuplicateBlock() {
			this.duplicateBlocks = this.duplicateBlocks - Main.CHUNK_LENGTH;
	}

	public void addDulicateBlock() {
		this.duplicateBlocks = this.duplicateBlocks + Main.CHUNK_LENGTH;;
	}
}
