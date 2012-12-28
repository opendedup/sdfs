package org.opendedup.sdfs.notification;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.opendedup.util.FileCounts;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class SDFSEvent {
	public Type type = null;
	public Level level;
	public String shortMsg = null;
	public String longMsg = "";
	public String target = null;
	public long maxCt = 1;
	public long curCt = 1;
	public long startTime;
	public long endTime = -1;
	public String uid = null;
	public String extendedInfo = null;
	private ArrayList<SDFSEvent> children = new ArrayList<SDFSEvent>();
	public String puid;
	public static final Type GC = new Type("Garbage Collection");
	public static final Type FLUSHALL = new Type("Flush All Buffers");
	public static final Type FDISK = new Type("File Check");
	public static final Type WAIT = new Type("Waiting to Run Again");
	public static final Type CLAIMR = new Type("Claim Records");
	public static final Type REMOVER = new Type("Remove Records");
	public static final Type AIMPORT = new Type("Replication Meta-Data Import");
	public static final Type AOUT = new Type("Replication Archive Out");
	public static final Type MOUNT = new Type("Mount Volume");
	public static final Type COMPACT = new Type("Compaction");
	public static final Type UMOUNT = new Type("Unmount Volume");
	public static final Type LHASHDB = new Type("Loading Hash Database Task");
	public static final Type FSCK = new Type("Consistancy Check");
	public static final Type MIMPORT = new Type("Replication Block Data"
			+ " Import");
	public static final Type FIXDSE = new Type("Volume Recovery Task");
	public static final Type SNAP = new Type("Take Snapshot");
	public static final Type EXPANDVOL = new Type("Expand Volume");
	public static final Type DELFILE = new Type("Delete File");
	public static final Level INFO = new Level("info");
	public static final Level WARN = new Level("warning");
	public static final Level ERROR = new Level("error");
	private static LinkedHashMap<String, SDFSEvent> tasks = new LinkedHashMap<String, SDFSEvent>(
			50, .075F, false);

	SimpleDateFormat format = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss zzz yyyy");

	protected SDFSEvent(Type type, String target, String shortMsg) {
		this.type = type;
		this.target = target;
		this.startTime = System.currentTimeMillis();
		this.shortMsg = shortMsg;
		this.uid = RandomGUID.getGuid();
		tasks.put(uid, this);
	}

	public void endEvent(String msg, Level level) {
		this.shortMsg = msg;
		this.level = level;
		this.curCt = this.maxCt;
		this.endTime = System.currentTimeMillis();
	}
	
	public void addChild(SDFSEvent evt) {
		evt.puid = this.uid;
		this.children.add(evt);
	}

	public boolean isDone() {
		return this.endTime > 0;
	}

	public void endEvent(String msg, Level level, Exception e) {
		for(int i  = 0;i<this.children.size();i++) {
			if(this.children.get(i).endTime == -1)
				this.children.get(i).endEvent(msg,level,e);
		}
		this.shortMsg = msg + " Exception : " + e.toString();
		this.level = level;
		this.endTime = System.currentTimeMillis();
		this.curCt = this.maxCt;
	}

	public void endEvent(String msg) {
		for(int i  = 0;i<this.children.size();i++) {
			if(this.children.get(i).endTime == -1)
				this.children.get(i).endEvent(msg);
		}
		this.shortMsg = msg;
		this.endTime = System.currentTimeMillis();
		this.level = SDFSEvent.INFO;
		this.curCt = this.maxCt;
	}

	public static SDFSEvent archiveImportEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(AIMPORT, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent umountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(UMOUNT, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent archiveOutEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(AOUT, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent compactEvent() {
		SDFSEvent event = new SDFSEvent(COMPACT, Main.volume.getName(),
				"Running Compaction on DSE, this may take a while");
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent mountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent consistancyCheckEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(FSCK, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent loadHashDBEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(LHASHDB, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent flushAllBuffers() {
		SDFSEvent event = new SDFSEvent(FLUSHALL, Main.volume.getName(),
				"Flushing all buffers");
		event.level = INFO;
		return event;
	}

	public static SDFSEvent snapEvent(String shortMsg, File src)
			throws IOException {

		SDFSEvent event = new SDFSEvent(SNAP, Main.volume.getName(), shortMsg);
		if (src.isDirectory())
			event.maxCt = FileCounts.getCount(src, true);
		else
			event.maxCt = 1;
		event.level = INFO;
		return event;
	}

	public static SDFSEvent metaImportEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MIMPORT, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent deleteFileEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, Main.volume.getName(),
				"File " + f.getPath() + " deleted");
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent deleteFileFailedEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, Main.volume.getName(),
				"File " + f.getPath() + " delete failed");
		event.level = WARN;
		return event;
	}

	public static SDFSEvent claimInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(CLAIMR, Main.volume.getName(), shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent waitEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(WAIT, Main.volume.getName(), shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent removeInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(REMOVER, Main.volume.getName(),
				shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent gcInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC, Main.volume.getName(), shortMsg);
		event.level = INFO;
		return event;
	}

	public static SDFSEvent fdiskInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(FDISK, Main.volume.getName(), shortMsg);
		event.level = INFO;
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
		return sb.toString();

	}

	public Element toXML() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("event");
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
		for (int i = 0; i < this.children.size(); i++) {
			Element el = this.children.get(i).toXML();
			doc.adoptNode(el);
			root.appendChild(el);
		}
		return (Element) root.cloneNode(true);
	}

	public static SDFSEvent fromXML(Element el) {
		SDFSEvent evt = new SDFSEvent(new Type(el.getAttribute("type")),
				el.getAttribute("target"), el.getAttribute("short-msg"));
		evt.maxCt = Long.parseLong(el.getAttribute("max-count"));
		evt.curCt = Long.parseLong(el.getAttribute("current-count"));
		evt.uid = el.getAttribute("uuid");
		evt.level = new Level(el.getAttribute("level"));
		evt.startTime = Long.parseLong(el.getAttribute("start-timestamp"));
		evt.endTime = Long.parseLong(el.getAttribute("end-timestamp"));
		evt.puid = el.getAttribute("parent-uid");
		evt.extendedInfo = el.getAttribute("extended-info");
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

	private static class Level {
		private String type = "";

		protected Level(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}
	}

	private static class Type {
		private String type = "";

		protected Type(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}
	}

}
