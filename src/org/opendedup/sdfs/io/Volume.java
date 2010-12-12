package org.opendedup.sdfs.io;

import java.io.File;


import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;


import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;
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
	private ReentrantLock updateLock = new ReentrantLock();
	long capacity;
	String name;
	String capString = null;
	long currentSize;
	String path;
	final int blockSize = 128 * 1024;
	double fullPercentage = -1;
	private long absoluteLength= -1;
	

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
		if(vol.hasAttribute("maximum-percentage-full")) {
			this.fullPercentage = Double.parseDouble(vol.getAttribute("maximum-percentage-full"))/100;
			this.absoluteLength = (long)(this.capacity * this.fullPercentage);
		}
		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
		if(this.fullPercentage > 0)
			SDFSLogger.getLog().info("Setting maximum capacity to " + this.absoluteLength);
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
		if(this.fullPercentage < 0 || this.currentSize == 0)
			return false;
		else {
			return(this.currentSize > this.absoluteLength);
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
		try{
			this.currentSize = this.currentSize + sz;
			if (this.currentSize < 0)
				this.currentSize = 0;
		}finally {
			this.updateLock.unlock();
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
		root.setAttribute("maximum-percentage-full", Double.toString(this.fullPercentage));
		return root;
	}
}
