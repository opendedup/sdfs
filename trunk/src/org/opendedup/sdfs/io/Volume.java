package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Volume implements java.io.Serializable {

	/**
	 * Represents the mounted volume associated with file system
	 */
	static long tbc = 1099511627776L;
	static long gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;

	private static final long serialVersionUID = 5505952237500542215L;
	private final ReentrantLock updateLock = new ReentrantLock();
	long capacity;
	String name;
	String capString = null;
	long currentSize;
	String path;
	final int blockSize = 128 * 1024;
	double fullPercentage = -1;
	private final ReentrantLock dbLock = new ReentrantLock();
	private final ReentrantLock vbLock = new ReentrantLock();
	private final ReentrantLock rbLock = new ReentrantLock();
	private final ReentrantLock wbLock = new ReentrantLock();
	private long absoluteLength = -1;
	private long duplicateBytes = 0;
	private long virtualBytesWritten = 0;
	private long readBytes = 0;
	private long actualWriteBytes = 0;
	private boolean closedGracefully = false;

	public String getName() {
		return name;
	}

	public Volume(Element vol) throws IOException {
		File f = new File(vol.getAttribute("path"));
		
		SDFSLogger.getLog().info("Mounting volume " + f.getPath());
		if (!f.exists())
			f.mkdirs();
		this.path = f.getPath();
		capString = vol.getAttribute("capacity");
		this.capacity = StringUtils.parseSize(capString);
		this.currentSize = Long.parseLong(vol.getAttribute("current-size"));
		if (vol.hasAttribute("duplicate-bytes"))
			this.addDuplicateBytes(Long.parseLong(vol
					.getAttribute("duplicate-bytes")));
		if (vol.hasAttribute("read-bytes"))
			this.addReadBytes(Long.parseLong(vol.getAttribute("read-bytes")));
		if (vol.hasAttribute("write-bytes"))
			this.addActualWriteBytes(Long.parseLong(vol
					.getAttribute("write-bytes")));
		if (vol.hasAttribute("maximum-percentage-full")) {
			this.fullPercentage = Double.parseDouble(vol
					.getAttribute("maximum-percentage-full")) / 100;
			this.absoluteLength = (long) (this.capacity * this.fullPercentage);
		} if(vol.hasAttribute("closed-gracefully"))
			Main.closedGracefully = Boolean.parseBoolean(vol.getAttribute("closed-gracefully"));
		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
		if (this.fullPercentage > 0)
			SDFSLogger.getLog().info(
					"Setting maximum capacity to " + this.absoluteLength);
		else
			SDFSLogger.getLog().info("Setting maximum capacity to infinite");
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public long getCurrentSize() {
		return currentSize;
	}

	public String getPath() {
		return path;
	}

	public boolean isFull() {
		if (this.fullPercentage < 0 || this.currentSize == 0)
			return false;
		else {
			return (this.currentSize > this.absoluteLength);
		}

	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void updateCurrentSize(long sz) {
		this.updateLock.lock();
		try {
			this.currentSize = this.currentSize + sz;
			if (this.currentSize < 0)
				this.currentSize = 0;
		} finally {
			this.updateLock.unlock();
		}
	}

	public boolean isClosedGracefully() {
		return closedGracefully;
	}

	public void setClosedGracefully(boolean closedGracefully) {
		this.closedGracefully = closedGracefully;
	}

	public long getTotalBlocks() {
		return (this.capacity / this.blockSize);
	}

	public long getUsedBlocks() {
		return (this.currentSize / this.blockSize);
	}

	public long getNumberOfFiles() {
		return Paths.get(path).getNameCount();
	}

	public Element toXMLElement(Document doc)
			throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", this.capString);
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage*100));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes));
		root.setAttribute("read-bytes", Long.toString(this.readBytes));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes));
		root.setAttribute("closed-gracefully", Boolean.toString(this.closedGracefully));
		return root;
	}

	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("volume");
		Element root = doc.getDocumentElement();
		root.setAttribute("path", path);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", Long.toString(this.capacity));
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes));
		root.setAttribute("read-bytes", Long.toString(this.readBytes));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes));
		root.setAttribute("dse-size", Long.toString(HashChunkService.getSize()* HashChunkService.getPageSize()));
		root.setAttribute("closed-gracefully", Boolean.toString(this.closedGracefully));
		return doc;
	}

	public void addVirtualBytesWritten(long virtualBytesWritten) {
		this.vbLock.lock();
		this.virtualBytesWritten += virtualBytesWritten;
		if(this.virtualBytesWritten < 0)
			this.virtualBytesWritten = 0;
		this.vbLock.unlock();
	}

	public long getVirtualBytesWritten() {
		return virtualBytesWritten;
	}

	public void addDuplicateBytes(long duplicateBytes) {
		this.dbLock.lock();
		this.duplicateBytes += duplicateBytes;
		if(this.duplicateBytes <0)
			this.duplicateBytes = 0;
		this.dbLock.unlock();
	}

	public long getDuplicateBytes() {
		return duplicateBytes;
	}

	public void addReadBytes(long readBytes) {
		this.rbLock.lock();
		this.readBytes += readBytes;
		if(this.readBytes < 0)
			this.readBytes = 0;
		this.rbLock.unlock();
	}

	public long getReadBytes() {
		return readBytes;
	}

	public void addActualWriteBytes(long writeBytes) {
		this.wbLock.lock();
		this.actualWriteBytes += writeBytes;
		if(this.actualWriteBytes < 0)
			this.actualWriteBytes = 0;
		this.wbLock.unlock();
	}

	public long getActualWriteBytes() {
		return actualWriteBytes;
	}
}
