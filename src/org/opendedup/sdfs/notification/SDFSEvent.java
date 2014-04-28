package org.opendedup.sdfs.notification;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSEventLogger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.FileCounts;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class SDFSEvent implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7418485011806466368L;
	public Type type = null;
	public Level level;
	public String shortMsg = null;
	public String longMsg = "";
	public String target = null;
	public long maxCt = 1;
	public long curCt = 1;
	public long startTime;
	public long endTime = -1;
	public long actionCount = 0;
	public boolean success = true;
	public String uid = null;
	public String extendedInfo = "";
	private ArrayList<SDFSEvent> children = new ArrayList<SDFSEvent>();
	public String puid;
	public transient static final Type GC = new Type("Garbage Collection");
	public transient static final Type FLUSHALL = new Type("Flush All Buffers");
	public transient static final Type FDISK = new Type("File Check");
	public transient static final Type CRCK = new Type(
			"Cluster Redundancy Check");
	public transient static final Type WAIT = new Type("Waiting to Run Again");
	public transient static final Type CLAIMR = new Type("Claim Records");
	public transient static final Type REMOVER = new Type("Remove Records");
	public transient static final Type AIMPORT = new Type(
			"Replication Meta-Data File Import");
	public transient static final Type IMPORT = new Type("Replication Import");
	public transient static final Type AOUT = new Type(
			"Replication Archive Out");
	public transient static final Type MOUNT = new Type("Mount Volume");
	public transient static final Type COMPACT = new Type("Compaction");
	public transient static final Type UMOUNT = new Type("Unmount Volume");
	public transient static final Type LHASHDB = new Type(
			"Loading Hash Database Task");
	public transient static final Type FSCK = new Type("Consistancy Check");
	public transient static final Type MIMPORT = new Type(
			"Replication Block Data" + " Import");
	public transient static final Type FIXDSE = new Type("Volume Recovery Task");
	public transient static final Type SNAP = new Type("Take Snapshot");
	public transient static final Type EXPANDVOL = new Type("Expand Volume");
	public transient static final Type DELFILE = new Type("Delete File");
	public transient static final Type PERFMON = new Type("Performance Monitor");
	public transient static final Type TEST = new Type("Testing 123");
	public transient static final Type CONVMAP = new Type("CONVMAP");
	public transient static final Type PSWD = new Type("Password Changed");
	public transient static final Type DSKFL = new Type("Disk Full");
	public transient static final Type RDER = new Type("Read Error");
	public transient static final Type WER = new Type("Write Error");
	public transient static final Level RUNNING = new Level("running");
	public transient static final Level INFO = new Level("info");
	public transient static final Level WARN = new Level("warning");
	public transient static final Level ERROR = new Level("error");
	private transient static LinkedHashMap<String, SDFSEvent> tasks = new LinkedHashMap<String, SDFSEvent>(
			50, .075F, false);

	SimpleDateFormat format = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss zzz yyyy");

	protected SDFSEvent(Type type, String target, String shortMsg, Level level) {
		this.type = type;
		this.target = target;
		this.startTime = System.currentTimeMillis();
		this.shortMsg = shortMsg;
		this.uid = RandomGUID.getGuid();
		tasks.put(uid, this);
		this.level = level;
		SDFSEventLogger.log(this);

	}

	public void endEvent(String msg, Level level) {
		this.shortMsg = msg;
		this.level = level;
		this.curCt = this.maxCt;
		this.endTime = System.currentTimeMillis();
		SDFSEventLogger.log(this);
	}

	public void addChild(SDFSEvent evt) throws IOException {
		if (evt.uid.equalsIgnoreCase(this.uid))
			throw new IOException("Cannot add child with same event id");
		evt.puid = this.uid;
		this.children.add(evt);
	}

	public ArrayList<SDFSEvent> getChildren() {
		return this.children;
	}

	public boolean isDone() {
		return this.endTime > 0;
	}

	public void endEvent(String msg, Level level, Throwable e) {
		for (int i = 0; i < this.children.size(); i++) {
			if (this.children.get(i).endTime == -1)
				this.children.get(i).endEvent(msg, level, e);
		}
		this.shortMsg = msg + " Exception : " + e.toString();
		this.level = level;
		this.endTime = System.currentTimeMillis();
		this.curCt = this.maxCt;
		this.success = false;
		SDFSEventLogger.log(this);
	}

	public void endEvent(String msg) {
		for (int i = 0; i < this.children.size(); i++) {
			if (this.children.get(i).endTime == -1)
				this.children.get(i).endEvent(msg);
		}
		this.shortMsg = msg;
		this.endTime = System.currentTimeMillis();
		this.level = SDFSEvent.INFO;
		this.curCt = this.maxCt;
		SDFSEventLogger.log(this);
	}

	public void endEvent() {
		for (int i = 0; i < this.children.size(); i++) {
			if (this.children.get(i).endTime == -1)
				this.children.get(i).endEvent();
		}
		this.endTime = System.currentTimeMillis();
		this.curCt = this.maxCt;
		SDFSEventLogger.log(this);
	}

	public static SDFSEvent archiveImportEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(AIMPORT, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent importEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(IMPORT, getTarget(), shortMsg, RUNNING);
		return event;
	}
	
	public static SDFSEvent passwdEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(PSWD, "Administrative User", shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent convMapEvent(String shortMsg, String file) {
		SDFSEvent event = new SDFSEvent(CONVMAP, file, shortMsg, RUNNING);
		return event;
	}

	public static void rdErrEvent() {
		SDFSEvent event = new SDFSEvent(RDER, getTarget(),
				"Read Error Detected", ERROR);
		event.maxCt = 1;
		event.endEvent();
	}

	public static void wrErrEvent() {
		SDFSEvent event = new SDFSEvent(WER, getTarget(),
				"Write Error Detected", ERROR);
		event.maxCt = 1;
		event.endEvent();
	}

	public static SDFSEvent testEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(TEST, "atestvolume", shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent perfMonEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(PERFMON, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent umountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(UMOUNT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent archiveOutEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(AOUT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent compactEvent() {
		SDFSEvent event = new SDFSEvent(COMPACT, getTarget(),
				"Running Compaction on DSE, this may take a while", RUNNING);
		return event;
	}

	public static SDFSEvent mountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent consistancyCheckEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(FSCK, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent loadHashDBEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(LHASHDB, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent flushAllBuffers() {
		SDFSEvent event = new SDFSEvent(FLUSHALL, getTarget(),
				"Flushing all buffers", RUNNING);
		return event;
	}

	public static SDFSEvent snapEvent(String shortMsg, File src)
			throws IOException {

		SDFSEvent event = new SDFSEvent(SNAP, getTarget(), shortMsg, RUNNING);
		if (src.isDirectory())
			event.maxCt = FileCounts.getCount(src, true);
		else
			event.maxCt = 1;
		return event;
	}

	public static BlockImportEvent metaImportEvent(String shortMsg,
			SDFSEvent evt) {
		BlockImportEvent event = new BlockImportEvent(getTarget(), shortMsg,
				RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent deleteFileEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, getTarget(), "File "
				+ f.getPath() + " deleted", RUNNING);
		event.endEvent("File " + f.getPath() + " deleted", INFO);
		return event;
	}

	public static SDFSEvent deleteFileFailedEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, getTarget(), "File "
				+ f.getPath() + " delete failed", WARN);
		event.endEvent("File " + f.getPath() + " delete failed", WARN);
		return event;
	}

	public static SDFSEvent claimInfoEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(CLAIMR, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent waitEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(WAIT, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent removeInfoEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(REMOVER, getTarget(), shortMsg, RUNNING);
		event.level = INFO;
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent gcInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent fdiskInfoEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(FDISK, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent crckInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(FDISK, getTarget(), shortMsg, RUNNING);
		return event;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.uid);
		sb.append(format.format(new Date(this.startTime)));
		sb.append(",");
		sb.append(this.startTime);
		sb.append(",");
		if (this.endTime > 0)
			sb.append(format.format(new Date(this.endTime)));
		else
			sb.append("");
		sb.append(",");
		sb.append(this.endTime);
		sb.append(",");
		sb.append(this.level);
		sb.append(",");
		sb.append(this.type);
		sb.append(",");
		sb.append(this.target);
		sb.append(",");
		sb.append(this.shortMsg);
		sb.append(",");
		sb.append(this.longMsg);
		sb.append(",");
		if (this.maxCt == 0 || this.curCt == 0)
			sb.append("0");
		else
			sb.append(Double.toString(this.curCt / this.maxCt));
		sb.append(",");
		sb.append(this.curCt);
		sb.append(",");
		sb.append(this.maxCt);
		sb.append(",");
		sb.append(this.extendedInfo);
		sb.append(",");
		sb.append(this.success);
		return sb.toString();

	}

	public Element toXML() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("event");
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(this.toString());
		Element root = doc.getDocumentElement();
		root.setAttribute("start-date", format.format(new Date(this.startTime)));
		root.setAttribute("start-timestamp", Long.toString(this.startTime));
		if (this.endTime > 0) {
			root.setAttribute("end-date", format.format(new Date(this.endTime)));
		}
		root.setAttribute("end-timestamp", Long.toString(this.endTime));
		root.setAttribute("level", this.level.toString());
		root.setAttribute("type", this.type.toString());
		root.setAttribute("target", this.target);
		root.setAttribute("short-msg", this.shortMsg);
		root.setAttribute("long-msg", this.longMsg);
		try {
			root.setAttribute("percent-complete",
					Double.toString((this.curCt / this.maxCt)));
		} catch (Exception e) {
			root.setAttribute("percent-complete", "0");
		}
		root.setAttribute("max-count", Long.toString(this.maxCt));
		root.setAttribute("current-count", Long.toString(this.curCt));
		root.setAttribute("uuid", this.uid);
		root.setAttribute("parent-uid", this.puid);
		root.setAttribute("extended-info", this.extendedInfo);
		root.setAttribute("success", Boolean.toString(this.success));
		for (int i = 0; i < this.children.size(); i++) {
			Element el = this.children.get(i).toXML();
			doc.adoptNode(el);
			root.appendChild(el);
		}
		return (Element) root.cloneNode(true);
	}

	public static SDFSEvent fromXML(Element el) {
		SDFSEvent evt = null;
		if (el.getAttribute("type").equalsIgnoreCase(MIMPORT.type)) {
			BlockImportEvent _evt = new BlockImportEvent(
					el.getAttribute("target"), el.getAttribute("short-msg"),
					new Level(el.getAttribute("level")));
			_evt.blocksImported = Long.parseLong(el
					.getAttribute("blocks-imported"));
			_evt.bytesImported = Long.parseLong(el
					.getAttribute("bytes-imported"));
			_evt.filesImported = Long.parseLong(el
					.getAttribute("files-imported"));
			_evt.virtualDataImported = Long.parseLong(el
					.getAttribute("virtual-data-imported"));
			evt = _evt;
		} else {
			evt = new SDFSEvent(new Type(el.getAttribute("type")),
					el.getAttribute("target"), el.getAttribute("short-msg"),
					new Level(el.getAttribute("level")));
		}
		evt.maxCt = Long.parseLong(el.getAttribute("max-count"));
		evt.curCt = Long.parseLong(el.getAttribute("current-count"));
		evt.uid = el.getAttribute("uuid");
		evt.startTime = Long.parseLong(el.getAttribute("start-timestamp"));
		evt.endTime = Long.parseLong(el.getAttribute("end-timestamp"));
		evt.puid = el.getAttribute("parent-uid");
		evt.extendedInfo = el.getAttribute("extended-info");
		evt.success = Boolean.parseBoolean(el.getAttribute("success"));
		int le = el.getElementsByTagName("event").getLength();
		if (le > 0) {
			for (int i = 0; i < le; i++) {
				Element _el = (Element) el.getElementsByTagName("event")
						.item(i);
				evt.children.add(fromXML(_el));
			}
		}
		return evt;

	}

	public static String getEvents() {
		Iterator<SDFSEvent> iter = SDFSEvent.tasks.values().iterator();
		StringBuffer sb = new StringBuffer();
		while (iter.hasNext()) {
			sb.append(iter.next());
			sb.append("/n");
		}
		return sb.toString();
	}

	public static Element getXMLEvent(String uuid)
			throws ParserConfigurationException {
		if (tasks.containsKey(uuid))
			return tasks.get(uuid).toXML();
		else
			throw new NullPointerException(uuid + " could not be found");

	}

	public static Element getXMLEvents() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("events");
		Element root = doc.getDocumentElement();
		Iterator<SDFSEvent> iter = tasks.values().iterator();
		while (iter.hasNext()) {
			Element el = iter.next().toXML();
			doc.adoptNode(el);
			root.appendChild(el);
		}
		return (Element) root.cloneNode(true);
	}

	public static class Level implements java.io.Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2246117933197717794L;
		private String type = "";

		protected Level(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}
	}

	public static class Type implements java.io.Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String type = "";

		protected Type(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}
	}

	public static String getTarget() {
		if (Main.standAloneDSE)
			return "Storage node " + Main.DSEClusterMemberID;
		else
			return Main.volume.getName();
	}

}
