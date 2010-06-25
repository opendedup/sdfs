package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.nio.file.Paths;


import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Volume implements java.io.Serializable {
	
	/**
	 * Represents the mounted volume associated with file system
	 */
	static long tbc = 1024 * 1024 * 1024 * 1024;
	static long gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;

	private static final long serialVersionUID = 5505952237500542215L;
	long capacity;
	String capString = null;
	long currentSize;
	String path;
	final int blockSize = 1024;

	public Volume(String path, long capacity, long currentSize) {
		File f = new File(path);
		if (!f.exists())
			f.mkdirs();
		this.path = f.getPath();
		this.capacity = capacity;
		this.currentSize = currentSize;
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
		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
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

	public void setPath(String path) {
		this.path = path;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void updateCurrentSize(long sz) {
		synchronized (this) {
			this.currentSize = this.currentSize + sz;
			if (this.currentSize < 0)
				this.currentSize = 0;
		}
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

	public Element toXML(Document doc) throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", this.capString);
		return root;
	}
}
