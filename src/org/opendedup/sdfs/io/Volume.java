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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import com.google.common.util.concurrent.AtomicDouble;

import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationService;
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
	String evtPath;
	String replPath;
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
	private ReplicationService rService = null;

	AtomicLong writeErrors = new AtomicLong(0);
	AtomicLong readErrors = new AtomicLong(0);
	private long serialNumber = 0;
	private boolean volumeFull = false;
	private boolean volumeOffLine = false;
	private boolean clustered = false;
	public String connicalPath;
	private boolean replEnabled = false;
	public ArrayList<ReplicationClient> replClients = new ArrayList<ReplicationClient>();
	Thread rChecker = null;

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
		if (vol.hasAttribute("event-path")) {
			this.evtPath = vol.getAttribute("event-path");
		} else {
			this.evtPath = pathF.getParentFile().getPath() + File.separator + "evt";
		}
		if (vol.hasAttribute("repl-path")) {
			this.replPath = vol.getAttribute("repl-path");
		} else {
			this.replPath = pathF.getParentFile().getPath() + File.separator + "repl";
		}
		this.connicalPath = pathF.getCanonicalPath();
		this.capacity = StringUtils.parseSize(vol.getAttribute("capacity"));
		if (vol.hasAttribute("name")) {
			this.name = vol.getAttribute("name");
			Main.sdfsVolName = this.name;
		} else {
			this.name = pathF.getParentFile().getName();
			Main.sdfsVolName = this.name;
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
		if (vol.hasAttribute("enable-repl")) {
			this.replEnabled = Boolean.parseBoolean(vol.getAttribute("enable-repl"));
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
		if (vol.getElementsByTagName("replica-source").getLength() > 0) {
			for (int i = 0; i < vol.getElementsByTagName("replica-source").getLength(); i++) {
				Element el = (Element) vol.getElementsByTagName("replica-source").item(i);
				try {
					ReplicationClient r = new ReplicationClient(el.getAttribute("url"),
							Long.parseLong(el.getAttribute("volumeid")), Boolean.parseBoolean(el.getAttribute("mtls")));
					r.sequence = Long.parseLong(el.getAttribute("sequence"));
					this.replClients.add(r);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("Unable to add replication client", e);
				}
			}
		}
		
	}

	public Volume() {

	}

	public void startReplClients() throws IOException {
		if (replEnabled) {
			try {
				this.rService = new ReplicationService(this);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("Unable to start Replication Change Listener Server",e);
			}
		}
		for (ReplicationClient rClient : this.replClients) {
			try {
				rClient.connect();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("Unable to connect to " + rClient.url + " volumeid " + rClient.volumeid);
			}
		}
		ReplicationClient.RecoverReplicationClients();
		this.rChecker = new Thread(new ReplChecker(this));
		this.rChecker.start();

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

	public boolean isPartitionFull() {
		long avail = pathF.getUsableSpace();

		if (avail < (1400000000)) {
			if (!this.volumeFull) {
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

	public void addReplicationClient(String url, long volumeID, boolean mtls) throws Exception {
		url = url.toLowerCase();
		synchronized (this.replClients) {
			for (ReplicationClient rClient : this.replClients) {
				if (url.equalsIgnoreCase(url) && volumeID == rClient.volumeid) {
					throw new ReplicationClientExistsException();
				}
			}
			ReplicationClient rClient = new ReplicationClient(url, volumeID, mtls);
			rClient.connect();
			rClient.replicationSink();
			this.replClients.add(rClient);
		}
		writer.writeConfig();
	}

	public void removeReplicationClient(String url, long volumeID) throws Exception {
		url = url.toLowerCase();
		synchronized (this.replClients) {
			ReplicationClient aClient = null;
			for (ReplicationClient rClient : this.replClients) {
				if (url.equalsIgnoreCase(url) && volumeID == rClient.volumeid) {
					aClient = rClient;
					break;
				}
			}
			if (aClient != null) {
				this.replClients.remove(aClient);
				aClient.shutDown();
			} else {
				throw new ReplicationClientNotExistsException();
			}
		}
		writer.writeConfig();
	}

	public static class ReplicationClientExistsException extends Exception {
		public ReplicationClientExistsException() {
			super();
		}
	}

	public static class ReplicationClientNotExistsException extends Exception {
		public ReplicationClientNotExistsException() {
			super();
		}
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
		root.setAttribute("event-path", this.evtPath);
		root.setAttribute("repl-path", this.replPath);
		if (this.rService != null) {
			root.setAttribute("enable-repl", Boolean.toString(true));
		} else {
			root.setAttribute("enable-repl", Boolean.toString(false));
		}
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
		root.setAttribute("event-path", this.evtPath);

		try {
			root.setAttribute("dse-comp-size", Long.toString(HCServiceProxy.getDSECompressedSize()));
			root.setAttribute("dse-size", Long.toString(HCServiceProxy.getDSESize()));
		} catch (Exception e) {
			root.setAttribute("dse-comp-size", Long.toString(0));
			root.setAttribute("dse-size", Long.toString(0));
		}
		for (ReplicationClient rClient : this.replClients) {
			Element rmq = doc.createElement("replica-source");
			rmq.setAttribute("url", rClient.url);
			rmq.setAttribute("mtls", Boolean.toString(rClient.mtls));
			rmq.setAttribute("volumeid", Long.toString(rClient.volumeid));
			rmq.setAttribute("sequence", Long.toString(rClient.sequence));
			doc.adoptNode(rmq);
			root.appendChild(rmq);
		}
		return root;
	}

	public String getEvtPath() {
		return this.evtPath;
	}

	public ReplicationService getRSerivce() {
		return this.rService;
	}

	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("volume");
		Element root = doc.getDocumentElement();
		root.setAttribute("path", path);
		root.setAttribute("event-path", this.evtPath);
		root.setAttribute("repl-path", this.replPath);
		if (this.rService != null) {
			root.setAttribute("enable-repl", Boolean.toString(true));
		} else {
			root.setAttribute("enable-repl", Boolean.toString(false));
		}
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
		for (ReplicationClient rClient : this.replClients) {
			Element rmq = doc.createElement("replica-source");
			rmq.setAttribute("url", rClient.url);
			rmq.setAttribute("mtls", Boolean.toString(rClient.mtls));
			rmq.setAttribute("volumeid", Long.toString(rClient.volumeid));
			rmq.setAttribute("sequence", Long.toString(rClient.sequence));
			doc.adoptNode(rmq);
			root.appendChild(rmq);
		}
		return doc;
	}

	public String getReplPath() {
		return this.replPath;
	}

	public VolumeInfoResponse toProtoc() {
		VolumeInfoResponse.Builder b = VolumeInfoResponse.newBuilder().setPath(path).setName(this.name)
				.setCurrentSize(this.currentSize.get()).setCapactity(this.capacity)
				.setMaxPercentageFull(this.fullPercentage).setDuplicateBytes(this.getDuplicateBytes())
				.setReadBytes(this.getReadBytes()).setWriteBytes(this.getActualWriteBytes())
				.setSerialNumber(this.serialNumber).setEvtPath(this.evtPath).setReplPath(this.replPath)
				.setMaxPageSize(HCServiceProxy.getMaxSize() * HashFunctionPool.avg_page_size)
				.setDseSize(HCServiceProxy.getDSESize()).setDseCompSize(HCServiceProxy.getDSECompressedSize())
				.setReadOps(this.readOperations.get()).setWriteOps(this.writeOperations.get())
				.setReadErrors(this.readErrors.get()).setWriteErrors(this.writeErrors.get()).setFiles(this.getFiles())
				.setClosedGracefully(this.closedGracefully).setAllowExternalLinks(this.allowExternalSymlinks)
				.setUsePerfMon(this.usePerfMon).setVolumeClustered(clustered).setClusterId(this.uuid)
				.setPerfMonFile(this.perfMonFile).setReadTimeoutSeconds(Main.readTimeoutSeconds)
				.setWriteTimeoutSeconds(Main.writeTimeoutSeconds).setCompressedMetaData(Main.COMPRESS_METADATA)
				.setSyncFiles(Main.syncDL).setOffline(this.isOffLine());

		if (this.rService != null) {
			b.setReplEnabled(true);
		}
		for (ReplicationClient rClient : this.replClients) {
			org.opendedup.grpc.VolumeServiceOuterClass.ReplicationClient.Builder rb = org.opendedup.grpc.VolumeServiceOuterClass.ReplicationClient
					.newBuilder();
			rb.setMtls(rClient.mtls);
			rb.setUrl(rClient.url);
			rb.setVolumeID(rClient.volumeid);
			rb.setSequence(rClient.sequence);
			b.addReplicationClient(rb.build());
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

	public static class ReplChecker implements Runnable {
		Volume vol;

		public ReplChecker(Volume vol) {
			this.vol = vol;
		}

		@Override
		public void run() {
			for (;;) {
				synchronized (vol.replClients) {
					for (ReplicationClient rClient : vol.replClients) {
						rClient.checkConnection();
					}
				}
				try {
					Thread.sleep(30 * 1000);
				} catch (InterruptedException e) {
					break;
				}
			}

		}

	}
}
