/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import com.google.common.util.concurrent.AtomicDouble;

import org.opendedup.grpc.VolumeServiceOuterClass.MessageQueueInfoResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.MessageQueueInfoResponse.MQType;
import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.monitor.VolumeIOMeter;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.OSValidator;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Volume {

	/**
	 * Represents the mounted volume associated with file system
	 */

	public long capacity;
	String name;
	AtomicLong currentSize = new AtomicLong(0);
	String path;
	File pathF;
	final int blockSize = 128 * 1024;
	double fullPercentage = -1;
	long absoluteLength = -1;
	private static boolean storageConnected = true;
	private AtomicLong duplicateBytes = new AtomicLong(0);
	private AtomicLong files = new AtomicLong(0);
	private AtomicDouble virtualBytesWritten = new AtomicDouble(0);
	private AtomicDouble readBytes = new AtomicDouble(0);
	private AtomicLong actualWriteBytes = new AtomicLong(0);
	private boolean closedGracefully = false;
	private AtomicLong readOperations = new AtomicLong(0);
	private AtomicLong writeOperations = new AtomicLong(0);
	private boolean allowExternalSymlinks = true;
	private boolean useDSESize = false;
	private boolean useDSECapacity = false;
	private boolean usePerfMon = false;
	private String perfMonFile = "/var/log/sdfs/perf.json";
	private transient VolumeConfigWriterThread writer = null;
	private transient VolumeIOMeter ioMeter = null;
	private String configPath = null;
	private String uuid = null;

	AtomicLong writeErrors = new AtomicLong(0);
	AtomicLong readErrors = new AtomicLong(0);
	private long serialNumber = 0;
	private boolean volumeFull = false;
	private boolean volumeOffLine = false;
	private boolean clustered = false;
	public String connicalPath;
	public String rabbitMQNode = null;
	public String rabbitMQTopic = "sdfs";
	public String rabbitMQUser = null;
	public String rabbitMQPassword = null;
	public int rabbitMQPort = 5672;
	public String pubsubTopic = null;
	public String pubsubSubscription = null;
	public String gcpProject = null;
	public String gcpCredsPath = null;

	public boolean isClustered() {
		return this.clustered;
	}

	public static void setStorageConnected(boolean connected) {
		if (connected && !storageConnected) {
			SDFSEvent.recoEvent();
		}

		if (!connected && storageConnected) {
			SDFSEvent.discoEvent();
		}
		storageConnected = connected;
	}

	public static boolean getStorageConnected() {
		return storageConnected;
	}

	public void setVolumeFull(boolean full) {
		this.volumeFull = full;
	}

	public void addWriteError() {
		long z = this.writeErrors.getAndIncrement();
		if (z == 0) {
			SDFSEvent.wrErrEvent();
		}
	}

	public void addFile() {

		this.files.incrementAndGet();
	}

	public long getFiles() {

		return this.files.get();
	}

	public void removeFile() {
		this.files.decrementAndGet();
	}

	public void addReadError() {
		long z = this.readErrors.getAndIncrement();
		if (z == 0) {
			SDFSEvent.rdErrEvent();
		}
	}

	public boolean isAllowExternalSymlinks() {
		return allowExternalSymlinks;
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

	public void setAllowExternalSymlinks(boolean allowExternalSymlinks, boolean propigateEvent) {
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
		if (OSValidator.isWindows()) {
			Files.setAttribute(Paths.get(pathF.getParentFile().getPath()), "dos:hidden", true,
					LinkOption.NOFOLLOW_LINKS);
		}
		this.path = pathF.getPath();
		this.connicalPath = pathF.getCanonicalPath();
		this.capacity = StringUtils.parseSize(vol.getAttribute("capacity"));
		if (vol.hasAttribute("name")) {
			this.name = vol.getAttribute("name");
			Main.sdfsVolName=this.name;
		} else {
			this.name = pathF.getParentFile().getName();
			Main.sdfsVolName=this.name;
		}
		if (vol.hasAttribute("read-timeout-seconds"))
			Main.readTimeoutSeconds = Integer.parseInt(vol.getAttribute("read-timeout-seconds"));
		if (vol.hasAttribute("write-timeout-seconds"))
			Main.writeTimeoutSeconds = Integer.parseInt(vol.getAttribute("write-timeout-seconds"));
		if (vol.hasAttribute("sync-files")) {
			boolean syncDL = Boolean.parseBoolean(vol.getAttribute("sync-files"));
			if (syncDL)
				Main.syncDL = true;
		}
		if (vol.hasAttribute("cluster-id") && vol.getAttribute("cluster-id").trim().length() > 0)
			this.uuid = vol.getAttribute("cluster-id");
		else
			this.uuid = RandomGUID.getGuid();
		if (vol.hasAttribute("use-dse-size"))
			this.useDSESize = Boolean.parseBoolean(vol.getAttribute("use-dse-size"));
		if (vol.hasAttribute("use-dse-capacity"))
			this.useDSECapacity = Boolean.parseBoolean(vol.getAttribute("use-dse-capacity"));
		if (vol.hasAttribute("use-perf-mon"))
			this.usePerfMon = Boolean.parseBoolean(vol.getAttribute("use-perf-mon"));
		if (vol.hasAttribute("perf-mon-file"))
			this.perfMonFile = vol.getAttribute("perf-mon-file");
		else
			this.perfMonFile = "/var/log/sdfs/volume-" + this.name + "-perf.json";
		this.currentSize.set(Long.parseLong(vol.getAttribute("current-size")));
		if (vol.hasAttribute("duplicate-bytes")) {
			this.duplicateBytes.set(Long.parseLong(vol.getAttribute("duplicate-bytes")));
		}
		if (vol.hasAttribute("read-bytes")) {

			this.readBytes.set(Double.parseDouble(vol.getAttribute("read-bytes")));
		}
		if (vol.hasAttribute("write-bytes")) {

			this.actualWriteBytes.set(Long.parseLong(vol.getAttribute("write-bytes")));
		}
		if (vol.hasAttribute("files")) {

			this.files.set(Long.parseLong(vol.getAttribute("files")));
		} else {
			File vf = new File(Main.dedupDBStore);
			if (vf.exists()) {
				this.files.set(FileCounts.getCounts(vf, false));
			}
		}

		if (vol.hasAttribute("serial-number")) {
			this.serialNumber = Long.parseLong(vol.getAttribute("serial-number"));
		} else {
			int sn = new Random().nextInt();
			if (sn < 0)
				sn = sn * -1;
			this.serialNumber = sn;
		}
		Main.DSEID = this.serialNumber;
		if (vol.hasAttribute("compress-metadata"))
			Main.COMPRESS_METADATA = Boolean.parseBoolean(vol.getAttribute("compress-metadata"));
		if (vol.hasAttribute("maximum-percentage-full")) {
			this.fullPercentage = Double.parseDouble(vol.getAttribute("maximum-percentage-full"));
			if (this.fullPercentage > 1)
				this.fullPercentage = this.fullPercentage / 100;
			SDFSLogger.getLog().info("Volume write threshold is " + this.fullPercentage);
			this.absoluteLength = (long) (this.capacity * this.fullPercentage);
		}
		if (vol.hasAttribute("closed-gracefully")) {
			Main.closedGracefully = Boolean.parseBoolean(vol.getAttribute("closed-gracefully"));
		}
		if (vol.hasAttribute("rebuild-hashtable")) {
			Main.runConsistancyCheck = true;
		}

		if (vol.hasAttribute("allow-external-links"))
			Main.allowExternalSymlinks = Boolean.parseBoolean(vol.getAttribute("allow-external-links"));

		SDFSLogger.getLog().info("Setting volume size to " + this.capacity);
		if (this.fullPercentage > 0)
			SDFSLogger.getLog().info("Setting maximum capacity to " + this.absoluteLength);
		else
			SDFSLogger.getLog().info("Setting maximum capacity to infinite");
		this.startThreads();

		if (vol.getElementsByTagName("rabbitmq-node").getLength() > 0) {
			Element el = (Element) vol.getElementsByTagName("rabbitmq-node").item(0);
			this.rabbitMQNode = el.getAttribute("hostname");
			if (el.hasAttribute("username"))
				this.rabbitMQUser = el.getAttribute("username");
			if (el.hasAttribute("password")) {
				this.rabbitMQPassword = el.getAttribute("password");
			}
			if (el.hasAttribute("port")) {
				this.rabbitMQPort = Integer.parseInt(el.getAttribute("port"));
			}
			if (el.hasAttribute("topic")) {
				this.rabbitMQTopic = el.getAttribute("topic");
			}

		}
		if (vol.getElementsByTagName("gcp-pubsub").getLength() > 0) {
			SDFSLogger.getLog().info("Reading Pubsub Settings");
			Element el = (Element) vol.getElementsByTagName("gcp-pubsub").item(0);
			if (el.hasAttribute("topic")) {
				this.pubsubTopic = el.getAttribute("topic");
			} else {
				this.pubsubTopic = "sdfsvolume";
			}
			if (el.hasAttribute("subscription")) {
				this.pubsubSubscription = el.getAttribute("subscription");
			} else {
				this.pubsubSubscription = Long.toString(serialNumber);
			}
			this.gcpProject = el.getAttribute("project-id");
			if (el.hasAttribute("auth-file")) {
				this.gcpCredsPath = el.getAttribute("auth-file");
			} else {
				this.gcpCredsPath = null;
			}
		}
	}

	public Volume() {

	}

	public void init() throws Exception {

		DedupFileStore.init();
	}

	public void writeUpdate() throws Exception {
		this.writer.writeConfig();
	}

	private void startThreads() {
		this.writer = new VolumeConfigWriterThread(this.configPath);
		new VolumeFullThread(this);
		if (this.usePerfMon)
			this.ioMeter = new VolumeIOMeter(this);
	}

	public long getCapacity() {
		if (this.useDSECapacity) {
			if (HashFunctionPool.max_hash_cluster == 1)
				return HCServiceProxy.getMaxSize() * HCServiceProxy.getPageSize();
			else
				return HCServiceProxy.getMaxSize() * HashFunctionPool.avg_page_size;
		} else
			return capacity;
	}

	public void setCapacity(long capacity, boolean propigateEvent) throws Exception {
		if (capacity <= this.currentSize.get())
			throw new IOException("Cannot resize volume to something less than current size. Current Size ["
					+ this.currentSize + "] requested capacity [" + capacity + "]");
		this.capacity = capacity;
		Main.chunkStoreAllocationSize = capacity;
		HCServiceProxy.setDseSize((capacity / HashFunctionPool.avg_page_size) + 8000);
		SDFSLogger.getLog().info("Set Volume Capacity to " + capacity);
		writer.writeConfig();

	}

	public void setCapacity(String capString) throws Exception {
		this.setCapacity(StringUtils.parseSize(capString), true);
	}

	public long getCurrentSize() {
		if (this.useDSESize)
			return HCServiceProxy.getDSECompressedSize();
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
		 * SDFSLogger.getLog().warn("Drive is almost full space left is [" + avail +
		 * "]"); return true;
		 *
		 * } if (this.fullPercentage < 0 || this.currentSize == 0) return false; else {
		 * return (this.currentSize > this.absoluteLength); }
		 */
	}

	public boolean isPartitionFull()
	{
		long avail = pathF.getUsableSpace();

		if (avail < (1400000000)) {
			if(!this.volumeFull) {
			SDFSLogger.getLog().warn(
					"Volume - Drive is almost full space left is [" + avail + "]");

			}
			this.volumeFull = true;
			return true;
		}
		return false;
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

	public Element toXMLElement(Document doc) throws ParserConfigurationException {
		Element root = doc.createElement("volume");
		root.setAttribute("path", path);
		root.setAttribute("name", this.name);
		root.setAttribute("current-size", Long.toString(this.currentSize.get()));
		root.setAttribute("capacity", StorageUnit.of(this.capacity).number_format(this.capacity));
		root.setAttribute("maximum-percentage-full", Double.toString(this.fullPercentage));

		root.setAttribute("duplicate-bytes", Long.toString(this.duplicateBytes.get()));
		root.setAttribute("read-bytes", Double.toString(this.readBytes.get()));

		root.setAttribute("write-bytes", Long.toString(this.getActualWriteBytes()));
		root.setAttribute("closed-gracefully", Boolean.toString(this.closedGracefully));
		root.setAttribute("serial-number", Long.toString(this.serialNumber));
		root.setAttribute("cluster-id", this.uuid);
		root.setAttribute("files", Long.toString(this.getFiles()));
		root.setAttribute("allow-external-links", Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-dse-capacity", Boolean.toString(this.useDSECapacity));
		root.setAttribute("use-dse-size", Boolean.toString(this.useDSESize));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("perf-mon-file", this.perfMonFile);
		root.setAttribute("offline", Boolean.toString(this.volumeOffLine));
		root.setAttribute("volume-clustered", Boolean.toString(clustered));
		root.setAttribute("read-timeout-seconds", Integer.toString(Main.readTimeoutSeconds));
		root.setAttribute("write-timeout-seconds", Integer.toString(Main.writeTimeoutSeconds));
		root.setAttribute("sync-files", Boolean.toString(Main.syncDL));
		root.setAttribute("compress-metadata", Boolean.toString(Main.COMPRESS_METADATA));

		try {
			root.setAttribute("dse-comp-size", Long.toString(HCServiceProxy.getDSECompressedSize()));
			root.setAttribute("dse-size", Long.toString(HCServiceProxy.getDSESize()));
		} catch (Exception e) {
			root.setAttribute("dse-comp-size", Long.toString(0));
			root.setAttribute("dse-size", Long.toString(0));
		}
		if (this.rabbitMQNode != null) {
			Element rmq = doc.createElement("rabbitmq-node");
			rmq.setAttribute("hostname", this.rabbitMQNode);
			rmq.setAttribute("port", Integer.toString(this.rabbitMQPort));
			rmq.setAttribute("topic", this.rabbitMQTopic);
			if (this.rabbitMQUser != null) {
				rmq.setAttribute("username", this.rabbitMQUser);
			}
			if (this.rabbitMQPassword != null) {
				rmq.setAttribute("password", this.rabbitMQPassword);
			}
			doc.adoptNode(rmq);
			root.appendChild(rmq);
		}
		if (this.gcpProject != null) {
			Element pbm = doc.createElement("gcp-pubsub");
			pbm.setAttribute("project-id", this.gcpProject);
			pbm.setAttribute("topic", this.pubsubTopic);
			pbm.setAttribute("subscription", this.pubsubSubscription);
			if (this.gcpCredsPath != null) {
				pbm.setAttribute("auth-file", this.gcpCredsPath);
			}
			doc.adoptNode(pbm);
			root.appendChild(pbm);
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
		root.setAttribute("maximum-percentage-full", Double.toString(this.fullPercentage));
		root.setAttribute("duplicate-bytes", Long.toString(this.getDuplicateBytes()));
		root.setAttribute("read-bytes", Double.toString(this.readBytes.get()));
		root.setAttribute("write-bytes", Long.toString(this.getActualWriteBytes()));
		root.setAttribute("serial-number", Long.toString(this.serialNumber));
		root.setAttribute("name", this.name);
		if (HashFunctionPool.max_hash_cluster == 1)
			root.setAttribute("max-size", Long.toString(HCServiceProxy.getMaxSize() * HCServiceProxy.getPageSize()));
		else
			root.setAttribute("max-size", Long.toString(HCServiceProxy.getMaxSize() * HashFunctionPool.avg_page_size));
		root.setAttribute("dse-size", Long.toString(HCServiceProxy.getDSESize()));
		root.setAttribute("dse-comp-size", Long.toString(HCServiceProxy.getDSECompressedSize()));
		root.setAttribute("readops", Double.toString(this.readOperations.get()));
		root.setAttribute("writeops", Double.toString(this.writeOperations.get()));
		root.setAttribute("readerrors", Long.toString(this.readErrors.get()));
		root.setAttribute("writeerrors", Long.toString(this.writeErrors.get()));
		root.setAttribute("files", Long.toString(this.getFiles()));
		root.setAttribute("closed-gracefully", Boolean.toString(this.closedGracefully));
		root.setAttribute("allow-external-links", Boolean.toString(Main.allowExternalSymlinks));
		root.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		root.setAttribute("cluster-id", this.uuid);
		root.setAttribute("perf-mon-file", this.perfMonFile);
		root.setAttribute("volume-clustered", Boolean.toString(clustered));
		root.setAttribute("read-timeout-seconds", Integer.toString(Main.readTimeoutSeconds));
		root.setAttribute("write-timeout-seconds", Integer.toString(Main.writeTimeoutSeconds));
		root.setAttribute("compress-metadata", Boolean.toString(Main.COMPRESS_METADATA));
		root.setAttribute("sync-files", Boolean.toString(Main.syncDL));

		if (this.rabbitMQNode != null) {
			Element rmq = doc.createElement("rabbitmq-node");
			rmq.setAttribute("hostname", this.rabbitMQNode);
			rmq.setAttribute("port", Integer.toString(this.rabbitMQPort));
			rmq.setAttribute("topic", this.rabbitMQTopic);
			if (this.rabbitMQUser != null) {
				rmq.setAttribute("username", this.rabbitMQUser);
			}
			if (this.rabbitMQPassword != null) {
				rmq.setAttribute("password", this.rabbitMQPassword);
			}
			doc.adoptNode(rmq);
			root.appendChild(rmq);
		}
		if (this.gcpProject != null) {
			Element pbm = doc.createElement("gcp-pubsub");
			pbm.setAttribute("project-id", this.gcpProject);
			pbm.setAttribute("topic", this.pubsubTopic);
			pbm.setAttribute("subscription", this.pubsubSubscription);
			if (this.gcpCredsPath != null) {
				pbm.setAttribute("auth-file", this.gcpCredsPath);
			}
			doc.adoptNode(pbm);
			root.appendChild(pbm);
		}
		return doc;
	}

	public VolumeInfoResponse toProtoc() {
		VolumeInfoResponse.Builder b = VolumeInfoResponse.newBuilder().setPath(path).setName(this.name)
				.setCurrentSize(this.currentSize.get()).setCapactity(this.capacity)
				.setMaxPercentageFull(this.fullPercentage).setDuplicateBytes(this.getDuplicateBytes())
				.setReadBytes(this.getReadBytes()).setWriteBytes(this.getActualWriteBytes())
				.setSerialNumber(this.serialNumber)
				.setMaxPageSize(HCServiceProxy.getMaxSize() * HashFunctionPool.avg_page_size)
				.setDseSize(HCServiceProxy.getDSESize()).setDseCompSize(HCServiceProxy.getDSECompressedSize())
				.setReadOps(this.readOperations.get()).setWriteOps(this.writeOperations.get())
				.setReadErrors(this.readErrors.get()).setWriteErrors(this.writeErrors.get()).setFiles(this.getFiles())
				.setClosedGracefully(this.closedGracefully).setAllowExternalLinks(this.allowExternalSymlinks)
				.setUsePerfMon(this.usePerfMon).setVolumeClustered(clustered).setClusterId(this.uuid)
				.setPerfMonFile(this.perfMonFile).setReadTimeoutSeconds(Main.readTimeoutSeconds)
				.setWriteTimeoutSeconds(Main.writeTimeoutSeconds).setCompressedMetaData(Main.COMPRESS_METADATA)
				.setSyncFiles(Main.syncDL).setOffline(this.isOffLine());
		if (this.rabbitMQNode != null) {
			MessageQueueInfoResponse.Builder mb = MessageQueueInfoResponse.newBuilder().setHostName(this.rabbitMQNode)
					.setMqType(MQType.RabbitMQ).setPort(this.rabbitMQPort).setTopic(this.rabbitMQTopic);
			b.addMessageQueue(mb);
		}
		if (this.gcpProject != null) {
			MessageQueueInfoResponse.Builder mb = MessageQueueInfoResponse.newBuilder().setMqType(MQType.PubSub)
					.setTopic(this.pubsubTopic).setSubScription(this.pubsubSubscription).setAuthInfo(this.gcpCredsPath)
					.setProject(this.gcpProject);
			b.addMessageQueue(mb);
		}
		return b.build();
	}

	public void addVirtualBytesWritten(long virtualBytesWritten, boolean propigateEvent) {
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

	public long getDuplicateBytes() {

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

	public long getActualWriteBytes() {

		return actualWriteBytes.get();
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Long getSerialNumber() {
		return serialNumber;
	}

	public void setSerialNumber(int serialNumber) {
		this.serialNumber = serialNumber;
	}
}
