package org.opendedup.sdfs.io;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.buse.driver.BUSEMkDev;
import org.opendedup.buse.sdfsdev.BlockDeviceBeforeClosedEvent;
import org.opendedup.buse.sdfsdev.BlockDeviceClosedEvent;
import org.opendedup.buse.sdfsdev.BlockDeviceOpenEvent;
import org.opendedup.buse.sdfsdev.SDFSBlockDev;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.util.ProcessWorker;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.eventbus.Subscribe;

public class BlockDev implements Externalizable{
	String devName;
	String devPath;
	String internalPath;
	String uuid;
	long size;
	boolean startOnInit;
	byte status = 0;
	String mappedDev;
	SDFSBlockDev dev = null;
	public static final byte STOPPED=0;
	public static final byte SYNC=1;
	public static final byte STARTED=2;
	private MetaDataDedupFile mf = null;
	
	
	public BlockDev(){
		
	}

	public BlockDev(String devName, String internalPath,
			long size, boolean start, String uuid) {
		if (uuid == null)
			this.uuid = RandomGUID.getGuid();
		this.devName = devName;
		this.size = size;
		this.internalPath = internalPath;
		this.startOnInit = start;
		File df = new File(this.internalPath);
		if(!df.exists())
			df.getParentFile().mkdirs();
		mf = MetaFileStore.getMF(df);
		mf.setDev(this);
	}

	public BlockDev(Element el) throws IOException {
		this.devName = el.getAttribute("devname");
		//this.devPath = el.getAttribute("devpath");
		this.size = StringUtils.parseSize(el.getAttribute("size"));
		this.internalPath = el.getAttribute("internal-path");
		if (el.hasAttribute("uuid"))
			this.uuid = el.getAttribute("uuid");
		else
			this.uuid = RandomGUID.getGuid();
		this.startOnInit = Boolean.parseBoolean(el
				.getAttribute("start-on-init"));
		File df = new File(this.internalPath);
		if(!df.exists())
			df.getParentFile().mkdirs();
		mf = MetaFileStore.getMF(df);
		mf.setDev(this);
	}
	
	public MetaDataDedupFile getMF() {
		return this.mf;
	}

	public String getDevName() {
		return devName;
	}

	public void setDevName(String devName) {
		this.devName = devName;
	}

	public String getDevPath() {
		return devPath;
	}

	public void setDevPath(String devPath) {
		this.devPath = devPath;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) throws IOException {
		if(size<this.size && this.status==2)
			throw new IOException("cannot shrink a block device while it is online");
			
		BUSEMkDev.setSize(devPath, size);
		this.size = size;
		
	}

	@Subscribe
	public void stopEvent(BlockDeviceClosedEvent evt) {
		this.status = 0;
		SDFSLogger.getLog().info(
				"Stopped [" + this.devName + "] at [" + this.internalPath
						+ "] on [" + this.devPath + "] with size [" + this.size
						+ "]");
		this.devPath = null;
	}

	@Subscribe
	public void beforeStopEvent(BlockDeviceBeforeClosedEvent evt) {
		SDFSLogger.getLog().info(
				"Stopping [" + this.devName + "] at [" + this.internalPath
						+ "] on [" + this.devPath + "] with size [" + this.size
						+ "]");
		String sh = "/bin/sh";
		String cop = "-c";
		String cmd = "umount /dev/mapper/" + this.devName;
		String[] exe = new String[] { sh, cop, cmd };
		try {
			ProcessWorker.runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Remove mounts to /dev/mapper/" + this.devName);
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to remove mounts to /dev/mapper/"
							+ this.devName, e);
		}
		sh = "/bin/sh";
		cop = "-c";
		cmd = "dmsetup remove -f " + this.devName;
		exe = new String[] { sh, cop, cmd };
		try {
			ProcessWorker.runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Remove references to /dev/mapper/" + this.devName);
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to remove references to /dev/mapper/"
							+ this.devName, e);
		}finally {
			this.mappedDev = null;
		}
	}

	@Subscribe
	public void startedEvent(BlockDeviceOpenEvent evt) {
		this.status = 0;
		SDFSLogger.getLog().info(
				"Started [" + this.devName + "] at [" + this.internalPath
						+ "] on [" + this.devPath + "] with size [" + this.size
						+ "]");
	}
	
	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("blockdev");
		Element root = doc.getDocumentElement();
		root.setAttribute("devname", this.devName);
		root.setAttribute("devpath", this.devPath);
		root.setAttribute("size", StorageUnit.of(this.size).format(size));
		root.setAttribute("internal-path", this.internalPath);
		root.setAttribute("start-on-init", Boolean.toString(this.startOnInit));
		root.setAttribute("uuid", this.uuid);
		root.setAttribute("status", Byte.toString(this.status));
		root.setAttribute("mappeddev", this.mappedDev);
		return doc;
	}

	public Element getElement() throws ParserConfigurationException {

		return (Element) this.toXMLDocument().getDocumentElement()
				.cloneNode(true);
	}

	public void startDev(String dp) throws IOException {
		if (!this.isStopped()) {
			throw new IOException("Device [" + this.devName
					+ "] already started");
		}
		this.devPath = dp;
		dev = new SDFSBlockDev(this);
		Thread th = new Thread(dev);
		th.start();
		String sh = "/bin/sh";
		String cop = "-c";
		String cmd = "dmsetup remove -f " + this.devName;
		String[] exe = new String[] { sh, cop, cmd };
		try {
			ProcessWorker.runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Remove old references to /dev/mapper/" + this.devName);
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to remove old references to /dev/mapper/"
							+ this.devName, e);
		}
		long bsz = this.size / 512L;
		sh = "/bin/sh";
		cop = "-c";
		cmd = "echo 0 " + bsz + " linear " + this.devPath
				+ " 0 | dmsetup create " + this.devName;
		exe = new String[] { sh, cop, cmd };
		SDFSLogger.getLog().info(
				"/bin/bash -c \"echo 0 " + bsz + " linear " + this.devPath
						+ " 0 | dmsetup create " + this.devName + "\"");
		try {
			ProcessWorker.runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Mapped device to /dev/mapper/" + this.devName);
			this.mappedDev = "/dev/mapper/" + this.devName;
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to map device to /dev/mapper/" + this.devName, e);
		}

	}

	public void stopDev() throws IOException {
		if (this.status == STOPPED)
			throw new IOException("Device [" + this.devName
					+ "] already stopped");
		dev.close();
		this.dev = null;
	}

	public String getInternalPath() {
		return internalPath;
	}

	public void setInternalPath(String internalPath) {
		this.internalPath = internalPath;
	}

	public boolean isStartOnInit() {
		return startOnInit;
	}

	public void setStartOnInit(boolean startOnInit) {
		this.startOnInit = startOnInit;
	}

	public boolean isStopped() {
		return this.status == STOPPED;
	}


	public static String toExternalTxt(Element el) {
		String status = "Stopped";
		byte stb = Byte.parseByte(el.getAttribute("status"));
		if(stb==1)
			status="Synchronizing";
		if(stb==2)
			status = "Started";
		String st = new String();
		st = st + "=============[" + el.getAttribute("devname")
				+ "]=============\n";
		st = st + "Device Name : " + el.getAttribute("devname") + "\n";
		st = st + "Block Device : " + el.getAttribute("devpath") + "\n";
		st = st + "Block Device Path : " + el.getAttribute("mappeddev") + "\n";
		st = st + "Size : " + el.getAttribute("size") + "\n";
		st = st + "UUID : " + el.getAttribute("uuid") + "\n";
		st = st + "Start on Init : " + el.getAttribute("start-on-init") + "\n";
		st = st + "Status : " + status + "\n";
		return st;

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.devName = StringUtils.readString(in);
		this.devPath = StringUtils.readString(in);
		this.uuid = StringUtils.readString(in);
		this.startOnInit = in.readBoolean();
		this.size = in.readLong();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		StringUtils.writeString(out,devName);
		StringUtils.writeString(out,devPath);
		StringUtils.writeString(out,uuid);
		out.writeBoolean(startOnInit);
		out.writeLong(size);
	}
	
}
