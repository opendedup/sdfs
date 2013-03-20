package org.opendedup.sdfs.io;

import java.io.File;







import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.monitor.VolumeIOMeter;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.RandomGUID;
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
	static final long minFree = 2147483648L; //Leave at least 2 GB Free on the drive.
	private static final long serialVersionUID = 5505952237500542215L;
	private final ReentrantLock updateLock = new ReentrantLock();
	long capacity;
	String name;
	String capString = null;
	long currentSize;
	String path;
	File pathF;
	final int blockSize = 128 * 1024;
	double fullPercentage = -1;
	private final ReentrantLock dbLock = new ReentrantLock();
	private final ReentrantLock vbLock = new ReentrantLock();
	private final ReentrantLock rbLock = new ReentrantLock();
	private final ReentrantLock wbLock = new ReentrantLock();
	long absoluteLength = -1;
	private long duplicateBytes = 0;
	private double virtualBytesWritten = 0;
	private double readBytes = 0;
	private long actualWriteBytes = 0;
	private boolean closedGracefully = false;
	private double readOperations;
	private double writeOperations;
	private boolean allowExternalSymlinks = true;
	private boolean useDSESize = false;
	private boolean useDSECapacity = false;
	private boolean usePerfMon = false;
	private String perfMonFile = "/var/log/sdfs/perf.json";
	private VolumeConfigWriterThread writer = null;
	private VolumeIOMeter ioMeter = null;
	private String configPath = null;
	private String uuid = null;

	protected boolean volumeFull = false;

	public boolean isAllowExternalSymlinks() {
		return allowExternalSymlinks;
	}

	public void setAllowExternalSymlinks(boolean allowExternalSymlinks, boolean propigateEvent) {
		this.allowExternalSymlinks = allowExternalSymlinks;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public Volume(Element vol,String path) throws IOException {
		this.configPath = path;
		pathF = new File(vol.getAttribute("path"));

		SDFSLogger.getLog().info("Mounting volume " + pathF.getPath());
		if (!pathF.exists())
			pathF.mkdirs();
		this.path = pathF.getPath();
		capString = vol.getAttribute("capacity");
		this.capacity = StringUtils.parseSize(capString);
		if(vol.hasAttribute("name"))
			this.name = vol.getAttribute("name");
		else
			this.name = pathF.getParentFile().getName();
		if(vol.hasAttribute("use-dse-size"))
			this.useDSESize = Boolean.parseBoolean(vol.getAttribute("use-dse-size"));
		if(vol.hasAttribute("uuid"))
			this.uuid = vol.getAttribute("uuid");
		else
			this.uuid = RandomGUID.getGuid();
		if(vol.hasAttribute("use-dse-capacity"))
			this.useDSECapacity = Boolean.parseBoolean(vol.getAttribute("use-dse-capacity"));
		if(vol.hasAttribute("use-perf-mon"))
			this.usePerfMon = Boolean.parseBoolean(vol.getAttribute("use-perf-mon"));
		if(vol.hasAttribute("perf-mon-file"))
			this.perfMonFile = vol.getAttribute("perf-mon-file");
		else
			this.perfMonFile = "/var/log/sdfs/volume-" +this.name + "-perf.json";
		this.currentSize = Long.parseLong(vol.getAttribute("current-size"));
		if (vol.hasAttribute("duplicate-bytes"))
			this.duplicateBytes = Long.parseLong(vol
					.getAttribute("duplicate-bytes"));
		if (vol.hasAttribute("read-bytes"))
			this.readBytes= Double.parseDouble(vol.getAttribute("read-bytes"));
		if (vol.hasAttribute("write-bytes"))
			this.actualWriteBytes = Long.parseLong(vol
					.getAttribute("write-bytes"));
		if (vol.hasAttribute("maximum-percentage-full")) {
			this.fullPercentage = Double.parseDouble(vol
					.getAttribute("maximum-percentage-full"));
			if(this.fullPercentage > 1)
				this.fullPercentage =this.fullPercentage/100;
			SDFSLogger.getLog().info("Volume write threshold is " + this.fullPercentage);
			this.absoluteLength = (long) (this.capacity * this.fullPercentage);
		}
		if (vol.hasAttribute("closed-gracefully"))
			Main.closedGracefully = Boolean.parseBoolean(vol
					.getAttribute("closed-gracefully"));
		if (vol.hasAttribute("allow-external-links"))
			Main.allowExternalSymlinks = Boolean.parseBoolean(vol
					.getAttribute("allow-external-links"));
		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
		if (this.fullPercentage > 0)
			SDFSLogger.getLog().info(
					"Setting maximum capacity to " + this.absoluteLength);
		else
			SDFSLogger.getLog().info("Setting maximum capacity to infinite");
		this.startThreads();
	}
	
	public Volume() {
		// TODO Auto-generated constructor stub
	}

	private void startThreads() {
		this.writer = new VolumeConfigWriterThread(this.configPath);
		new VolumeFullThread(this);
		if(this.usePerfMon)
			this.ioMeter = new VolumeIOMeter(this);
	}

	public long getCapacity() {
		if(this.useDSECapacity)
			return HCServiceProxy.getMaxSize()
					* HCServiceProxy.getPageSize();
		else
			return capacity;
	}

	public void setCapacity(long capacity, boolean propigateEvent) throws Exception {
		if(capacity <= this.currentSize)
			throw new IOException("Cannot resize volume to something less than current size. Current Size [" + this.currentSize + "] requested capacity [" + capacity + "]");
		this.capacity = capacity;
		SDFSLogger.getLog().info("Set Volume Capacity to " + capacity);
		writer.writeConfig();
		
	}
	
	public void setCapacity(String capString) throws Exception {
		this.setCapacity(StringUtils.parseSize(capString), true);
		this.capString = capString;
	}

	public long getCurrentSize() {
		if(this.useDSESize)
			return HCServiceProxy.getSize()
					* HCServiceProxy.getPageSize();
		else
			return currentSize;
	}

	public String getPath() {
		return path;
	}

	public boolean isFull() {
		return
				this.volumeFull;
				/*
		long avail = pathF.getUsableSpace();
		if(avail < minFree) {
			SDFSLogger.getLog().warn("Drive is almost full space left is [" + avail + "]");
			return true;
			
		}
		if (this.fullPercentage < 0 || this.currentSize == 0)
			return false;
		else {
			return (this.currentSize > this.absoluteLength);
		}
		*/
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void updateCurrentSize(long sz, boolean propigateEvent) {
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
	
	public void setUsePerfMon(boolean use, boolean propigateEvent) {
		if(this.usePerfMon == true && use == false) {
			this.ioMeter.close();
			this.ioMeter = null;
		}
		else if(this.usePerfMon == false && use == true) {
			this.ioMeter = new VolumeIOMeter(this);
		}
		this.usePerfMon = use;
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setClosedGracefully(boolean closedGracefully) {
		this.closedGracefully = closedGracefully;
		if(this.closedGracefully) {
			this.writer.stop();
			if(this.usePerfMon)
				this.ioMeter.close();
		}
			
	}

	public long getTotalBlocks() {
		return (this.getCapacity() / this.blockSize);
	}

	public long getUsedBlocks() {
		return (this.getCurrentSize() / this.blockSize);
	}
	
	public double getReadOperations() {
		return readOperations;
	}

	public double getWriteOperations() {
		return writeOperations;
	}

	public String getPerfMonFile() {
		return perfMonFile;
	}

	public void addWIO(boolean propigateEvent) {
		if(this.writeOperations == Long.MAX_VALUE)
			this.writeOperations = 0;
		this.writeOperations++;
	}
	
	public void addRIO(boolean propigateEvent) {
		if(this.readOperations == Long.MAX_VALUE)
			this.readOperations = 0;
		this.readOperations++;
	}

	public long getNumberOfFiles() {
		return Paths.get(path).getNameCount();
	}

	public Element toXMLElement(Document doc)
			throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("name", this.name);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", this.capString);
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes));
		root.setAttribute("read-bytes", Double.toString(this.readBytes));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes));
		root.setAttribute("closed-gracefully",
				Boolean.toString(this.closedGracefully));
		root.setAttribute("uuid", this.uuid);
		root.setAttribute("allow-external-links", Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-dse-capacity", Boolean.toString(this.useDSECapacity));
		root.setAttribute("use-dse-size", Boolean.toString(this.useDSESize));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("perf-mon-file", this.perfMonFile);
		return root;
	}

	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("volume");
		Element root = doc.getDocumentElement();
		root.setAttribute("path", path);
		root.setAttribute("name", this.name);
		root.setAttribute("current-size", Long.toString(this.currentSize));
		root.setAttribute("capacity", Long.toString(this.capacity));
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes));
		root.setAttribute("read-bytes", Double.toString(this.readBytes));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes));
		root.setAttribute("name", this.name);
		root.setAttribute(
				"dse-size",
				Long.toString(HCServiceProxy.getSize()
						* HCServiceProxy.getPageSize()));
		root.setAttribute("readops", Double.toString(this.readOperations));
		root.setAttribute("writeops", Double.toString(this.writeOperations));
		root.setAttribute("closed-gracefully",
				Boolean.toString(this.closedGracefully));
		root.setAttribute("allow-external-links", Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("uuid", this.uuid);
		root.setAttribute("perf-mon-file", this.perfMonFile);
		return doc;
	}

	public void addVirtualBytesWritten(long virtualBytesWritten, boolean propigateEvent) {
		this.vbLock.lock();
		this.addWIO(true);
		this.virtualBytesWritten += virtualBytesWritten;
		if (this.virtualBytesWritten < 0)
			this.virtualBytesWritten = 0;
		this.vbLock.unlock();
	}

	public double getVirtualBytesWritten() {
		return virtualBytesWritten;
	}
	
	public void addDuplicateBytes(long duplicateBytes, boolean propigateEvent) {
		this.dbLock.lock();
		this.duplicateBytes += duplicateBytes;
		if (this.duplicateBytes < 0)
			this.duplicateBytes = 0;
		this.dbLock.unlock();
	}

	public double getDuplicateBytes() {
		return duplicateBytes;
	}

	public void addReadBytes(long readBytes, boolean propigateEvent) {
		this.rbLock.lock();
		this.readBytes += readBytes;
		this.addRIO(true);
		if (this.readBytes < 0)
			this.readBytes = 0;
		this.rbLock.unlock();
	}

	public double getReadBytes() {
		return readBytes;
	}

	public void addActualWriteBytes(long writeBytes, boolean propigateEvent) {
		this.wbLock.lock();
		this.actualWriteBytes += writeBytes;
		if (this.actualWriteBytes < 0)
			this.actualWriteBytes = 0;
		this.wbLock.unlock();
	}

	public double getActualWriteBytes() {
		return actualWriteBytes;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
}
