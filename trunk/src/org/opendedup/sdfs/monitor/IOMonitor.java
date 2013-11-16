package org.opendedup.sdfs.monitor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
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
	private static ArrayList<IOMonitorListener> iofListeners = new ArrayList<IOMonitorListener>();
	private int riops = -1;
	private int wiops = -1;
	private int iops = -1;
	private long rbps = -1;
	private long wbps = -1;
	private long bps = -1;
	private int qos = -1;
	private String iopProfile = "none";
	private final MetaDataDedupFile mf;

	public IOMonitor(MetaDataDedupFile mf) {
		this.mf = mf;
	}

	public static void addIOMonListener(IOMonitorListener l) {
		iofListeners.add(l);
	}

	public static void removeIOMonListener(IOMonitorListener l) {
		iofListeners.remove(l);
	}

	public static ArrayList<IOMonitorListener> getIOMonListeners() {
		return iofListeners;
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

	public void addBytesRead(int len, boolean propigateEvent) {
		this.updateLock.lock();
		this.addRIO(true);
		this.bytesRead = this.bytesRead + len;
		this.updateLock.unlock();
		Main.volume.addReadBytes(len, true);
	}

	public void addActualBytesWritten(int len, boolean propigateEvent) {
		this.updateLock.lock();
		this.actualBytesWritten = this.actualBytesWritten + len;
		this.updateLock.unlock();
		Main.volume.addActualWriteBytes(len, true);
	}

	public void addWIO(boolean propigateEvent) {
		if (this.writeOperations == Long.MAX_VALUE)
			this.writeOperations = 0;
		this.writeOperations++;
	}

	public void addRIO(boolean propigateEvent) {
		if (this.readOperations == Long.MAX_VALUE)
			this.readOperations = 0;
		this.readOperations++;
	}

	public void addVirtualBytesWritten(int len, boolean propigateEvent) {
		this.updateLock.lock();
		this.addWIO(true);
		this.virtualBytesWritten = this.virtualBytesWritten + len;
		this.updateLock.unlock();
		Main.volume.addVirtualBytesWritten(len, true);
	}

	public void setVirtualBytesWritten(long len, boolean propigateEvent) {
		this.virtualBytesWritten = len;
	}

	public long getDuplicateBlocks() {
		return duplicateBlocks;
	}

	public void setDuplicateBlocks(long duplicateBlocks, boolean propigateEvent) {
		this.duplicateBlocks = duplicateBlocks;
	}

	public void setActualBytesWritten(long actualBytesWritten,
			boolean propigateEvent) {
		this.actualBytesWritten = actualBytesWritten;
	}

	public void setBytesRead(long bytesRead, boolean propigateEvent) {
		this.bytesRead = bytesRead;

	}

	public void removeDuplicateBlock(boolean propigateEvent) {
		this.duplicateBlocks = this.duplicateBlocks - Main.CHUNK_LENGTH;
		Main.volume.addDuplicateBytes(-1 * Main.CHUNK_LENGTH, true);
	}

	public void clearAllCounters(boolean propigateEvent) {
		this.updateLock.lock();
		Main.volume.addReadBytes(-1 * this.bytesRead, true);
		Main.volume.addDuplicateBytes(-1 * this.duplicateBlocks, true);
		Main.volume.addActualWriteBytes(-1 * this.actualBytesWritten, true);
		Main.volume.addVirtualBytesWritten(-1 * this.virtualBytesWritten, true);
		this.bytesRead = 0;
		this.duplicateBlocks = 0;
		this.actualBytesWritten = 0;
		this.virtualBytesWritten = 0;
		this.updateLock.unlock();
	}

	public void clearFileCounters(boolean propigateEvent) {
		this.updateLock.lock();
		this.bytesRead = 0;
		this.duplicateBlocks = 0;
		this.actualBytesWritten = 0;
		this.virtualBytesWritten = 0;
		this.updateLock.unlock();
	}

	public void addDulicateBlock(boolean propigateEvent) {
		this.updateLock.lock();
		this.duplicateBlocks = this.duplicateBlocks + Main.CHUNK_LENGTH;
		this.updateLock.unlock();
		Main.volume.addDuplicateBytes(Main.CHUNK_LENGTH, true);
	}

	public byte[] toByteArray() {
		byte[] ip = this.iopProfile.getBytes();
		ByteBuffer buf = ByteBuffer.wrap(new byte[8 + 8 + 8 + 8 + 4 + ip.length
				+ 4 + 4 + 4 + 4 + 8 + 8 + 8 + 4]);
		buf.putLong(this.virtualBytesWritten);
		buf.putLong(this.actualBytesWritten);
		buf.putLong(this.bytesRead);
		buf.putLong(this.duplicateBlocks);
		buf.putInt(ip.length);
		buf.put(ip);
		buf.putInt(this.riops);
		buf.putInt(this.wiops);
		buf.putInt(this.wiops);
		buf.putInt(this.iops);
		buf.putLong(this.rbps);
		buf.putLong(this.wbps);
		buf.putLong(this.bps);
		buf.putInt(this.qos);
		return buf.array();
	}

	public void fromByteArray(byte[] b) {
		ByteBuffer buf = ByteBuffer.wrap(b);
		this.virtualBytesWritten = buf.getLong();
		this.actualBytesWritten = buf.getLong();
		this.bytesRead = buf.getLong();
		this.duplicateBlocks = buf.getLong();
		if ((buf.position() + 1) < buf.capacity()) {
			byte[] ip = new byte[buf.getInt()];
			this.iopProfile = new String(ip);
			this.riops = buf.getInt();
			this.wiops = buf.getInt();
			this.iops = buf.getInt();
			this.rbps = buf.getLong();
			this.wbps = buf.getLong();
			this.bps = buf.getLong();
			this.qos = buf.getInt();
		}
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
		root.setAttribute("max-readops", Integer.toString(this.riops));
		root.setAttribute("max-writeops", Integer.toString(this.wiops));
		root.setAttribute("max-iops", Integer.toString(this.iops));
		root.setAttribute("max-readmbps",
				Long.toString(this.rbps / (1024 * 1024)));
		root.setAttribute("max-writembps",
				Long.toString(this.wbps / (1024 * 1024)));
		root.setAttribute("max-mbps", Long.toString(this.bps / (1024 * 1024)));
		root.setAttribute("io-qos", Integer.toString(this.qos));
		root.setAttribute("io-profile", this.iopProfile);
		return root;
	}

	@Override
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

	public int getRiops() {
		return riops;
	}

	public void setRiops(int riops, boolean propigateEvent) {
		this.riops = riops;
	}

	public int getWiops() {
		return wiops;
	}

	public void setWiops(int wiops, boolean propigateEvent) {
		this.wiops = wiops;
	}

	public int getIops() {
		return iops;
	}

	public void setIops(int iops, boolean propigateEvent) {
		this.iops = iops;
	}

	public int getQos() {
		return iops;
	}

	public void setQos(int qos, boolean propigateEvent) {
		this.qos = qos;
	}

	public long getRmbps() {
		return rbps;
	}

	public void setRmbps(long rmbps, boolean propigateEvent) {
		this.rbps = rmbps;
	}

	public long getWmbps() {
		return wbps;
	}

	public void setWmbps(long wmbps, boolean propigateEvent) {
		this.wbps = wmbps;
	}

	public long getMbps() {
		return bps;
	}

	public void setMbps(long mbps, boolean propigateEvent) {
		this.bps = mbps;
	}

	public String getIopProfile() {
		return iopProfile;
	}

	public void setIopProfile(String iopProfile, boolean propigateEvent) {
		this.iopProfile = iopProfile;
	}

	public MetaDataDedupFile getMf() {
		return mf;
	}
}
