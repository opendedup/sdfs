package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class Volume implements java.io.Serializable {
	private static Logger log = Logger.getLogger("sdfs");
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
	final int blockSize = 32768;

	public Volume(String path, long capacity, long currentSize) {
		File f = new File(path);
		if(!f.exists())
			f.mkdirs();
		this.path = f.getPath();
		this.capacity = capacity;
		this.currentSize = currentSize;
	}
	
	public Volume(Element vol) throws IOException {
		
		File f = new File(vol.getAttribute("path"));
		log.info("Mounting volume " + f.getPath());
		if(!f.exists())
			f.mkdirs();
		this.path = f.getPath();
		capString =vol.getAttribute("capacity");
		String units = capString.substring(capString.length() - 2);
		int sz = Integer.parseInt(capString.substring(0,capString.length() - 2));
		long fSize = 0;
		if (units.equalsIgnoreCase("TB"))
			fSize = sz * tbc;
		else if (units.equalsIgnoreCase("GB"))
			fSize = sz * gbc;
		else if (units.equalsIgnoreCase("MB"))
			fSize = sz * mbc;
		else{
			log.severe(" error : unable to determine capacity of volume " + this.capString);
			throw new IOException("unable to determine capacity of volume " + this.capString);
			}
		this.currentSize = fSize;
		log.info("setting volume size to " + this.currentSize);
		Main.chunkStoreAllocationSize = this.currentSize;
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
			if(this.currentSize < 0)
				this.currentSize = 0;
		}
	}
	
	public long getTotalBlocks() {
		return (this.capacity/this.blockSize);
	}
	
	public long getUsedBlocks() {
		return (this.currentSize/this.blockSize);
	}
	
	
	public Element toXML(Document doc) throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", this.capString);
		return root;
	}
}
