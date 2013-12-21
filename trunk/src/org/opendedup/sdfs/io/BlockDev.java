package org.opendedup.sdfs.io;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.buse.sdfsdev.BlockDeviceBeforeClosedEvent;
import org.opendedup.buse.sdfsdev.BlockDeviceClosedEvent;
import org.opendedup.buse.sdfsdev.BlockDeviceOpenEvent;
import org.opendedup.buse.sdfsdev.SDFSBlockDev;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.eventbus.Subscribe;

public class BlockDev {
	String devName;
	String devPath;
	String internalPath;
	String uuid;
	long size;
	boolean startOnInit;
	boolean stopped = true;

	SDFSBlockDev dev = null;

	public BlockDev(String devName, String devPath, String internalPath,
			long size, boolean start, String uuid) {
		if (uuid == null)
			this.uuid = RandomGUID.getGuid();
		this.devName = devName;
		this.devPath = devPath;
		this.size = size;
		this.internalPath = internalPath;
		this.startOnInit = start;
	}

	public BlockDev(Element el) throws IOException {
		this.devName = el.getAttribute("devname");
		this.devPath = el.getAttribute("devpath");
		this.size = StringUtils.parseSize(el.getAttribute("size"));
		this.internalPath = el.getAttribute("internal-path");
		if (el.hasAttribute("uuid"))
			this.uuid = el.getAttribute("uuid");
		else
			this.uuid = RandomGUID.getGuid();
		this.startOnInit = Boolean.parseBoolean(el
				.getAttribute("start-on-init"));
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

	public void setSize(long size) {
		this.size = size;
	}

	@Subscribe
	public void stopEvent(BlockDeviceClosedEvent evt) {
		this.stopped = true;
		SDFSLogger.getLog().info(
				"Stopped [" + this.devName + "] at [" + this.internalPath
						+ "] on [" + this.devPath + "] with size [" + this.size
						+ "]");
	}

	@Subscribe
	public void beforeStopEvent(BlockDeviceBeforeClosedEvent evt) {
		SDFSLogger.getLog().info(
				"Stopping [" + this.devName + "] at [" + this.internalPath
						+ "] on [" + this.devPath + "] with size [" + this.size
						+ "]");
		String sh = "/bin/sh";
		String cop = "-c";
		String cmd = "dmsetup remove -f " + this.devName;
		String[] exe = new String[] { sh, cop, cmd };
		try {
			runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Remove references to /dev/mapper/" + this.devName);
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to remove references to /dev/mapper/"
							+ this.devName, e);
		}
	}

	@Subscribe
	public void startedEvent(BlockDeviceOpenEvent evt) {
		this.stopped = false;
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
		root.setAttribute("stopped", Boolean.toString(this.stopped));
		return doc;
	}

	public Element getElement() throws ParserConfigurationException {

		return (Element) this.toXMLDocument().getDocumentElement()
				.cloneNode(true);
	}

	public void startDev() throws IOException {
		if (!this.isStopped()) {
			throw new IOException("Device [" + this.devName
					+ "] already started");
		}
		dev = new SDFSBlockDev(this);
		Thread th = new Thread(dev);
		th.start();
		String sh = "/bin/sh";
		String cop = "-c";
		String cmd = "dmsetup remove -f " + this.devName;
		String[] exe = new String[] { sh, cop, cmd };
		try {
			runProcess(exe, 1000);
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
			runProcess(exe, 1000);
			SDFSLogger.getLog().info(
					"Mapped device to /dev/mapper/" + this.devName);
		} catch (Exception e) {
			SDFSLogger.getLog().info(
					"Failed to map device to /dev/mapper/" + this.devName, e);
		}

	}

	public void stopDev() throws IOException {
		if (this.stopped)
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
		return stopped;
	}

	public void setStopped(boolean stopped) {
		this.stopped = stopped;
	}

	public static String toExternalTxt(Element el) {
		String st = new String();
		st = st + "=============[" + el.getAttribute("devname")
				+ "]=============\n";
		st = st + "Device Name : " + el.getAttribute("devname") + "\n";
		st = st + "Block Device : " + el.getAttribute("devpath") + "\n";
		st = st + "Size : " + el.getAttribute("size") + "\n";
		st = st + "UUID : " + el.getAttribute("uuid") + "\n";
		st = st + "Start on Init : " + el.getAttribute("start-on-init") + "\n";
		st = st + "Stopped : " + el.getAttribute("stopped") + "\n";
		return st;

	}

	private int runProcess(String[] pstr, int timeout) throws TimeoutException,
			InterruptedException, IOException {
		String cmdStr = "";
		for (String st : pstr) {
			cmdStr = cmdStr + " " + st;
		}
		SDFSLogger.getLog().info("Executing [" + cmdStr + "]");
		Process p = Runtime.getRuntime().exec(pstr);
		Worker worker = new Worker(p);
		worker.start();
		try {
			worker.join(timeout);
			if (worker.exit != null) {
				SDFSLogger.getLog().info(
						"[" + cmdStr + "] returned " + worker.exit);
				return worker.exit;
			} else
				throw new TimeoutException();
		} catch (InterruptedException ex) {
			worker.interrupt();
			Thread.currentThread().interrupt();
			throw ex;
		} finally {
			p.destroy();
		}
	}

	private static class Worker extends Thread {
		private final Process process;
		private Integer exit;

		private Worker(Process process) {
			this.process = process;
		}

		public void run() {
			try {
				exit = process.waitFor();
			} catch (InterruptedException ignore) {
				return;
			}
		}
	}
}
