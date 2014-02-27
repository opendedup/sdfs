package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import org.jgroups.Address;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.VolumeSocket;
import org.opendedup.sdfs.monitor.VolumeIOMeter;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.google.common.util.concurrent.AtomicDouble;

public class Volume implements java.io.Serializable {

	/**
	 * Represents the mounted volume associated with file system
	 */

	private static final long serialVersionUID = 5505952237500542215L;
	long capacity;
	String name;
	AtomicLong currentSize=new AtomicLong(0);
	String path;
	File pathF;
	final int blockSize = 128 * 1024;
	double fullPercentage = -1;
	long absoluteLength = -1;
	private AtomicLong duplicateBytes = new AtomicLong(0);
	private AtomicDouble virtualBytesWritten = new AtomicDouble(0);
	private AtomicDouble readBytes = new AtomicDouble(0);
	private AtomicLong actualWriteBytes = new AtomicLong(0);
	private boolean closedGracefully = false;
	private AtomicLong readOperations= new AtomicLong(0);
	private AtomicLong writeOperations= new AtomicLong(0);
	private boolean allowExternalSymlinks = true;
	private boolean useDSESize = false;
	private boolean useDSECapacity = false;
	private boolean usePerfMon = false;
	private String perfMonFile = "/var/log/sdfs/perf.json";
	private transient VolumeConfigWriterThread writer = null;
	private transient VolumeIOMeter ioMeter = null;
	private String configPath = null;
	private String uuid = null;
	private byte clusterCopies = 2;
	private boolean clusterRackAware = false;
	public Address host = null;

	private boolean volumeFull = false;
	private boolean volumeOffLine = false;
	private boolean clustered = false;
	public ArrayList<BlockDev> devices = new ArrayList<BlockDev>();
	public transient VolumeSocket soc = null;
	
	public boolean isClustered() {
		return this.clustered;
	}
	
	public VolumeSocket getSoc() {
		return this.soc;
	}

	public void setVolumeFull(boolean full) {
		this.volumeFull = full;
	}

	public boolean isAllowExternalSymlinks() {
		return allowExternalSymlinks;
	}

	public void closeAllDevices() {
		for (BlockDev dev : devices) {
			try {
				dev.stopDev();
			} catch (IOException e) {

			}
		}
	}

	private void startAllOnStartupDevices() {
		synchronized (devices) {
			for (BlockDev dev : devices) {
				try {
					if (dev.startOnInit)
						this.startDev(dev.devName);
				} catch (IOException e) {

				}
			}
		}
	}

	public boolean isOffLine() {
		return this.volumeOffLine;
	}

	public void setOffLine(boolean offline) {
		if (offline && !this.volumeOffLine)
			SDFSLogger.getLog().warn("Setting Volume Offline");
		if (!offline && this.volumeOffLine)
			SDFSLogger.getLog().warn("Setting Volume Online");
		this.volumeOffLine = offline;
	}

	public void setAllowExternalSymlinks(boolean allowExternalSymlinks,
			boolean propigateEvent) {
		this.allowExternalSymlinks = allowExternalSymlinks;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Volume(Element vol, String path) throws IOException {
		this.configPath = path;
		pathF = new File(vol.getAttribute("path"));
		
		SDFSLogger.getLog().info("Mounting volume " + pathF.getPath());
		if (!pathF.exists())
			pathF.mkdirs();
		this.path = pathF.getPath();
		this.capacity = StringUtils.parseSize(vol.getAttribute("capacity"));
		if (vol.hasAttribute("name"))
			this.name = vol.getAttribute("name");
		else
			this.name = pathF.getParentFile().getName();
		if (vol.hasAttribute("use-dse-size"))
			this.useDSESize = Boolean.parseBoolean(vol
					.getAttribute("use-dse-size"));
		if (vol.hasAttribute("cluster-id"))
			this.uuid = vol.getAttribute("cluster-id");
		else
			this.uuid = RandomGUID.getGuid();
		if (vol.hasAttribute("use-dse-capacity"))
			this.useDSECapacity = Boolean.parseBoolean(vol
					.getAttribute("use-dse-capacity"));
		if (vol.hasAttribute("use-perf-mon"))
			this.usePerfMon = Boolean.parseBoolean(vol
					.getAttribute("use-perf-mon"));
		if (vol.hasAttribute("perf-mon-file"))
			this.perfMonFile = vol.getAttribute("perf-mon-file");
		else
			this.perfMonFile = "/var/log/sdfs/volume-" + this.name
					+ "-perf.json";
		this.currentSize.set(Long.parseLong(vol.getAttribute("current-size")));
		if (vol.hasAttribute("duplicate-bytes"))
			this.duplicateBytes.set(Long.parseLong(vol
					.getAttribute("duplicate-bytes")));
		if (vol.hasAttribute("read-bytes"))
			this.readBytes.set(Double.parseDouble(vol.getAttribute("read-bytes")));
		if (vol.hasAttribute("write-bytes"))
			this.actualWriteBytes.set(Long.parseLong(vol
					.getAttribute("write-bytes")));
		if (vol.hasAttribute("maximum-percentage-full")) {
			this.fullPercentage = Double.parseDouble(vol
					.getAttribute("maximum-percentage-full"));
			if (this.fullPercentage > 1)
				this.fullPercentage = this.fullPercentage / 100;
			SDFSLogger.getLog().info(
					"Volume write threshold is " + this.fullPercentage);
			this.absoluteLength = (long) (this.capacity * this.fullPercentage);
		}
		if (vol.hasAttribute("closed-gracefully"))
			Main.closedGracefully = Boolean.parseBoolean(vol
					.getAttribute("closed-gracefully"));
		if (vol.hasAttribute("allow-external-links"))
			Main.allowExternalSymlinks = Boolean.parseBoolean(vol
					.getAttribute("allow-external-links"));
		if (vol.hasAttribute("cluster-block-copies")) {
			this.clusterCopies = Byte.valueOf(vol
					.getAttribute("cluster-block-copies"));
			if (this.clusterCopies > 7) {
				this.clusterCopies = 7;
			}
		}
		if(vol.hasAttribute("volume-clustered")) {
			
			this.clustered = Boolean.parseBoolean(vol
					.getAttribute("volume-clustered"));
		}
		if (vol.hasAttribute("cluster-rack-aware")) {
			this.clusterRackAware = Boolean.parseBoolean(vol
					.getAttribute("cluster-rack-aware"));
		}
		if (vol.hasAttribute("cluster-response-timeout"))
			Main.ClusterRSPTimeout = Integer.parseInt(vol
					.getAttribute("cluster-response-timeout"));
		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
		if (this.fullPercentage > 0)
			SDFSLogger.getLog().info(
					"Setting maximum capacity to " + this.absoluteLength);
		else
			SDFSLogger.getLog().info("Setting maximum capacity to infinite");
		this.startThreads();
		if (vol.getElementsByTagName("blockdev").getLength() > 0) {
			NodeList lst = vol.getElementsByTagName("blockdev");
			for (int i = 0; i < lst.getLength(); i++) {
				Element el = (Element) lst.item(i);
				BlockDev dev = new BlockDev(el);
				this.devices.add(dev);
			}
		}
	}

	public Volume() {
		// TODO Auto-generated constructor stub
	}

	public void init() throws Exception {
		if(this.clustered) {
			this.soc = new VolumeSocket(this,Main.DSEClusterConfig);
		}
		if (Main.blockDev)
			this.startAllOnStartupDevices();
	}

	public synchronized void addBlockDev(BlockDev dev) throws Exception {
		synchronized (devices) {
			long sz = 0;
			for (BlockDev _dev : this.devices) {
				if (dev.devName.equalsIgnoreCase(_dev.devName))
					throw new IOException("Device Name [" + dev.devName
							+ "] already exists");
				if (dev.internalPath.equalsIgnoreCase(_dev.internalPath))
					throw new IOException("Device Internal Path ["
							+ dev.internalPath + "] already exists");
				sz = sz +_dev.size;
			}
			sz = sz + dev.size;
			if(sz > this.getCapacity())
				throw new IOException("Requested Block Device is too large for volume. " +
						"Volume capacity is [" +StorageUnit.of(this.getCapacity()).format(this.getCapacity()) + "] and requested size was [" + StorageUnit.of(dev.size).format(dev.size) + "]");
			this.devices.add(dev);
			/*
			if (dev.startOnInit && Main.blockDev) {
				try {
					this.startDev(dev.devName);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("unable to start device", e);
				}
			}
			*/
			this.writer.writeConfig();
		}
	}

	public synchronized BlockDev removeBlockDev(String devName)
			throws Exception {
		synchronized (devices) {
			for (BlockDev _dev : this.devices) {
				if (_dev.devName.equalsIgnoreCase(devName)) {
					try {
						_dev.stopDev();
					} catch (IOException e) {
						SDFSLogger.getLog().debug(
								"issue while stopping device during removal ",
								e);
					}
					this.devices.remove(_dev);
					this.writer.writeConfig();
					return _dev;
				}
			}
		}
		throw new IOException("Device not found [" + devName + "]");
	}

	public synchronized BlockDev getBlockDev(String devName) throws IOException {
		synchronized (devices) {
			for (BlockDev _dev : this.devices) {
				if (_dev.devName.equalsIgnoreCase(devName)) {
					return _dev;
				}
			}
		}
		throw new IOException("Device not found [" + devName + "]");
	}

	public void writeUpdate() throws Exception {
		this.writer.writeConfig();
	}

	private synchronized String getFreeDevice() throws IOException {
		for (int i = 0; i < 128; i++) {
			String blkDevP = "/dev/nbd" + i;
			boolean found = false;
			for (BlockDev _dev : this.devices) {
				if (_dev.devPath != null
						&& _dev.devPath.equalsIgnoreCase(blkDevP)) {
					found = true;
					break;
				}
			}
			if (!found)
				return blkDevP;
		}
		throw new IOException("no free block devices found");
	}

	public synchronized BlockDev startDev(String devname) throws IOException {

		BlockDev dev = this.getBlockDev(devname);
		if (Main.blockDev) {
			dev.startDev(this.getFreeDevice());
		}
		return dev;
	}

	private void startThreads() {
		this.writer = new VolumeConfigWriterThread(this.configPath);
		new VolumeFullThread(this);
		if (this.usePerfMon)
			this.ioMeter = new VolumeIOMeter(this);
	}

	public long getCapacity() {
		if (this.useDSECapacity)
			return HCServiceProxy.getMaxSize() * HCServiceProxy.getPageSize();
		else
			return capacity;
	}

	public void setCapacity(long capacity, boolean propigateEvent)
			throws Exception {
		if (capacity <= this.currentSize.get())
			throw new IOException(
					"Cannot resize volume to something less than current size. Current Size ["
							+ this.currentSize + "] requested capacity ["
							+ capacity + "]");
		this.capacity = capacity;
		SDFSLogger.getLog().info("Set Volume Capacity to " + capacity);
		writer.writeConfig();

	}

	public void setCapacity(String capString) throws Exception {
		this.setCapacity(StringUtils.parseSize(capString), true);
	}

	public long getCurrentSize() {
		if (this.useDSESize)
			return HCServiceProxy.getSize() * HCServiceProxy.getPageSize();
		else
			return currentSize.get();
	}

	public String getPath() {
		return path;
	}

	public boolean isFull() {
		return this.volumeFull;
		/*
		 * long avail = pathF.getUsableSpace(); if(avail < minFree) {
		 * SDFSLogger.getLog().warn("Drive is almost full space left is [" +
		 * avail + "]"); return true;
		 * 
		 * } if (this.fullPercentage < 0 || this.currentSize == 0) return false;
		 * else { return (this.currentSize > this.absoluteLength); }
		 */
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void updateCurrentSize(long sz, boolean propigateEvent) {
			long val = this.currentSize.addAndGet(sz);
			if (val < 0)
				this.currentSize.set(0);
	}

	public boolean isClosedGracefully() {
		return closedGracefully;
	}

	public void setUsePerfMon(boolean use, boolean propigateEvent) {
		if (this.usePerfMon == true && use == false) {
			this.ioMeter.close();
			this.ioMeter = null;
		} else if (this.usePerfMon == false && use == true) {
			this.ioMeter = new VolumeIOMeter(this);
		}
		this.usePerfMon = use;
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setClosedGracefully(boolean closedGracefully) {
		this.closedGracefully = closedGracefully;
		if (this.closedGracefully) {
			this.writer.stop();
			if (this.usePerfMon)
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
		return readOperations.get();
	}

	public double getWriteOperations() {
		return writeOperations.get();
	}

	public String getPerfMonFile() {
		return perfMonFile;
	}

	public void addWIO(boolean propigateEvent) {
		long val = this.writeOperations.incrementAndGet();
		if (this.writeOperations.get() == Long.MAX_VALUE)
			this.writeOperations.set(val);
		
	}

	public void addRIO(boolean propigateEvent) {
		long val = this.writeOperations.incrementAndGet();
		if (val == Long.MAX_VALUE)
			this.readOperations.set(0);
	}

	public long getNumberOfFiles() {
		return Paths.get(path).getNameCount();
	}

	public Element toXMLElement(Document doc)
			throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("name", this.name);
		root.setAttribute("current-size", Long.toString(this.currentSize.get()));
		root.setAttribute("capacity",
				StorageUnit.of(this.capacity).format(this.capacity));
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes.get()));
		root.setAttribute("read-bytes", Double.toString(this.readBytes.get()));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes.get()));
		root.setAttribute("closed-gracefully",
				Boolean.toString(this.closedGracefully));
		root.setAttribute("cluster-id", this.uuid);
		root.setAttribute("cluster-response-timeout",
				Integer.toString(Main.ClusterRSPTimeout));
		root.setAttribute("allow-external-links",
				Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-dse-capacity",
				Boolean.toString(this.useDSECapacity));
		root.setAttribute("use-dse-size", Boolean.toString(this.useDSESize));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("perf-mon-file", this.perfMonFile);
		root.setAttribute("cluster-block-copies", Byte.toString(clusterCopies));
		root.setAttribute("cluster-rack-aware",
				Boolean.toString(this.clusterRackAware));
		root.setAttribute("volume-clustered", Boolean.toString(clustered));
		for (BlockDev blk : this.devices) {
			Element el = blk.getElement();
			doc.adoptNode(el);
			root.appendChild(el);
		}
		return root;
	}

	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("volume");
		Element root = doc.getDocumentElement();
		root.setAttribute("path", path);
		root.setAttribute("name", this.name);
		root.setAttribute("current-size", Long.toString(this.currentSize.get()));
		root.setAttribute("capacity", Long.toString(this.capacity));
		root.setAttribute("maximum-percentage-full",
				Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes.get()));
		root.setAttribute("read-bytes", Double.toString(this.readBytes.get()));
		root.setAttribute("write-bytes", Long.toString(this.actualWriteBytes.get()));
		root.setAttribute("cluster-response-timeout",
				Integer.toString(Main.ClusterRSPTimeout));
		root.setAttribute("name", this.name);
		root.setAttribute(
				"dse-size",
				Long.toString(HCServiceProxy.getSize()
						* HCServiceProxy.getPageSize()));
		root.setAttribute("readops", Double.toString(this.readOperations.get()));
		root.setAttribute("writeops", Double.toString(this.writeOperations.get()));
		root.setAttribute("closed-gracefully",
				Boolean.toString(this.closedGracefully));
		root.setAttribute("allow-external-links",
				Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("cluster-id", this.uuid);
		root.setAttribute("perf-mon-file", this.perfMonFile);
		root.setAttribute("cluster-block-copies", Byte.toString(clusterCopies));
		root.setAttribute("cluster-rack-aware",
				Boolean.toString(this.clusterRackAware));
		root.setAttribute("volume-clustered", Boolean.toString(clustered));
		for (BlockDev blk : this.devices) {
			Element el = blk.getElement();
			doc.adoptNode(el);
			root.appendChild(el);
		}
		return doc;
	}

	public void addVirtualBytesWritten(long virtualBytesWritten,
			boolean propigateEvent) {
		this.addWIO(true);
		double val = this.virtualBytesWritten.addAndGet(virtualBytesWritten);
		if (val < 0)
			this.virtualBytesWritten.set(0);
	}

	public double getVirtualBytesWritten() {
		return virtualBytesWritten.get();
	}

	public void addDuplicateBytes(long duplicateBytes, boolean propigateEvent) {
		double val = this.duplicateBytes.addAndGet(duplicateBytes);
		if (val < 0)
			this.duplicateBytes.set(0);
	}

	public double getDuplicateBytes() {
		return duplicateBytes.get();
	}

	public void addReadBytes(long readBytes, boolean propigateEvent) {
		double val = this.readBytes.addAndGet(readBytes);
		this.addRIO(true);
		if (val < 0)
			this.readBytes.set(0);
	}

	public double getReadBytes() {
		return readBytes.get();
	}

	public void addActualWriteBytes(long writeBytes, boolean propigateEvent) {
		double val = this.actualWriteBytes.addAndGet(writeBytes);
		if (val < 0)
			this.actualWriteBytes.set(0);
	}

	public double getActualWriteBytes() {
		return actualWriteBytes.get();
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public byte getClusterCopies() {
		return clusterCopies;
	}

	public void setClusterCopies(byte clusterCopies) {
		this.clusterCopies = clusterCopies;
	}

	public boolean isClusterRackAware() {
		return clusterRackAware;
	}

	public void setClusterRackAware(boolean clusterRackAware) {
		this.clusterRackAware = clusterRackAware;
	}
}
