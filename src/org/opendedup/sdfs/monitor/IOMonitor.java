package org.opendedup.sdfs.monitor;

import java.nio.ByteBuffer;

import org.opendedup.sdfs.Main;

public class IOMonitor implements java.io.Serializable {

	private static final long serialVersionUID = 6582549274733666474L;
	private long virtualBytesWritten;
	private long actualBytesWritten;
	private long bytesRead;
	private long duplicateBlocks;

	public IOMonitor() {
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
	
	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.wrap(new byte[32]);
		buf.putLong(this.virtualBytesWritten);
		buf.putLong(this.actualBytesWritten);
		buf.putLong(this.bytesRead);
		buf.putLong(this.duplicateBlocks);
		return buf.array();
	}


	public void fromByteArray(byte [] b) {
		ByteBuffer buf = ByteBuffer.wrap(b);
		this.virtualBytesWritten = buf.getLong();
		this.actualBytesWritten = buf.getLong();
		this.bytesRead = buf.getLong();
		this.duplicateBlocks = buf.getLong();
	}
	
	public String toXML() {
		StringBuffer sb = new StringBuffer();
		sb.append("<file-io-info virtual-bytes-written=\"");
		sb.append(this.virtualBytesWritten);
		sb.append("\" actual-bytes-written=\"");
		sb.append(this.actualBytesWritten);
		sb.append("\" bytes-read=\"");
		sb.append(this.bytesRead);
		sb.append("\" duplicate-blocks=\"");
		sb.append(this.duplicateBlocks);
		sb.append("\"/>");
		return sb.toString();
	}
}
