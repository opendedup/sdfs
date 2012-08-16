package org.opendedup.sdfs.monitor;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class IOMonitor implements java.io.Serializable {

	private static final long serialVersionUID = 6582549274733666474L;
	private long virtualBytesWritten;
	private long actualBytesWritten;
	private long bytesRead;
	private long duplicateBlocks;
	private long readOperations;
	private long writeOperations;
	private final ReentrantLock updateLock = new ReentrantLock();

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
		this.updateLock.lock();
		this.addRIO();
		this.bytesRead = this.bytesRead + len;
		this.updateLock.unlock();
		Main.volume.addReadBytes(len);
	}

	public void addActualBytesWritten(int len) {
		this.updateLock.lock();
		this.actualBytesWritten = this.actualBytesWritten + len;
		this.updateLock.unlock();
		Main.volume.addActualWriteBytes(len);
	}
	
	public void addWIO() {
		if(this.writeOperations == Long.MAX_VALUE)
			this.writeOperations = 0;
		this.writeOperations++;
	}
	
	public void addRIO() {
		if(this.readOperations == Long.MAX_VALUE)
			this.readOperations = 0;
		this.readOperations++;
	}

	public void addVirtualBytesWritten(int len) {
		this.updateLock.lock();
		this.addWIO();
		this.virtualBytesWritten = this.virtualBytesWritten + len;
		this.updateLock.unlock();
		Main.volume.addVirtualBytesWritten(len);
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
		Main.volume.addDuplicateBytes(-1 * Main.CHUNK_LENGTH);
	}

	public void clearAllCounters() {
		this.updateLock.lock();
		Main.volume.addReadBytes(-1 * this.bytesRead);
		Main.volume.addDuplicateBytes(-1 * this.duplicateBlocks);
		Main.volume.addActualWriteBytes(-1 * this.actualBytesWritten);
		Main.volume.addVirtualBytesWritten(-1 * this.virtualBytesWritten);
		this.bytesRead = 0;
		this.duplicateBlocks = 0;
		this.actualBytesWritten = 0;
		this.virtualBytesWritten = 0;
		this.updateLock.unlock();
	}
	
	public void clearFileCounters() {
		this.updateLock.lock();
		this.bytesRead = 0;
		this.duplicateBlocks = 0;
		this.actualBytesWritten = 0;
		this.virtualBytesWritten = 0;
		this.updateLock.unlock();
	}

	public void addDulicateBlock() {
		this.updateLock.lock();
		this.duplicateBlocks = this.duplicateBlocks + Main.CHUNK_LENGTH;
		this.updateLock.unlock();
		Main.volume.addDuplicateBytes(Main.CHUNK_LENGTH);
	}

	public byte[] toByteArray() {
		ByteBuffer buf = ByteBuffer.wrap(new byte[32]);
		buf.putLong(this.virtualBytesWritten);
		buf.putLong(this.actualBytesWritten);
		buf.putLong(this.bytesRead);
		buf.putLong(this.duplicateBlocks);
		return buf.array();
	}

	public void fromByteArray(byte[] b) {
		ByteBuffer buf = ByteBuffer.wrap(b);
		this.virtualBytesWritten = buf.getLong();
		this.actualBytesWritten = buf.getLong();
		this.bytesRead = buf.getLong();
		this.duplicateBlocks = buf.getLong();
	}

	public Element toXML(Document doc) throws ParserConfigurationException {
		Element root = doc.createElement("io-info");
		root.setAttribute("virtual-bytes-written",
				Long.toString(this.virtualBytesWritten));
		root.setAttribute("actual-bytes-written",
				Long.toString(this.actualBytesWritten));
		root.setAttribute("bytes-read", Long.toString(this.bytesRead));
		root.setAttribute("duplicate-blocks",
				Long.toString(this.duplicateBlocks));
		root.setAttribute("readops", Long.toString(this.readOperations));
		root.setAttribute("writeops", Long.toBinaryString(this.writeOperations));
		return root;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("virtual-bytes-written=\"");
		sb.append(this.virtualBytesWritten);
		sb.append("\"\n actual-bytes-written=\"");
		sb.append(this.actualBytesWritten);
		sb.append("\"\n bytes-read=\"");
		sb.append(this.bytesRead);
		sb.append("\"\n duplicate-blocks=\"");
		sb.append(this.duplicateBlocks);
		sb.append("\"\n read-ops=\"");
		sb.append(this.readOperations);
		sb.append("\"\n write-ops=\"");
		sb.append(this.writeOperations);
		return sb.toString();
	}
}
