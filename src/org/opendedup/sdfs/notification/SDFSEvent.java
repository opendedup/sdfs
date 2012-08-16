package org.opendedup.sdfs.notification;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.opendedup.sdfs.Main;
import org.opendedup.util.RandomGUID;


public class SDFSEvent {
	public String type = null;
	public String level;
	public String shortMsg = null;
	public String longMsg = "";
	public String target = null;
	public long time;
	public String uid = null;
	public static final String GC= "gc";
	public static final String MOUNT ="Mount Volume";
	public static final String FIXDSE ="Volume Recovery Task";
	public static final String SNAP ="Take Snapshot";
	public static final String EXPANDVOL ="Expand Volume";
	public static final String DELFILE ="Delete File";
	public static final String INFO = "info";
	public static final String WARN = "warning";
	public static final String ERROR = "error";
	private static LinkedHashMap<String,SDFSEvent> runningTasks = new LinkedHashMap<String,SDFSEvent>(20,.075F,false);
	  SimpleDateFormat format =
	            new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
	
	protected SDFSEvent(String type,String target,String shortMsg) {
		this.type = type;
		this.target = target;
		this.time = System.currentTimeMillis();
		this.shortMsg = shortMsg;
		this.uid = RandomGUID.getGuid();
		runningTasks.put(uid, this);
	}
	
	public static SDFSEvent gcInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC,Main.volume.getName(),shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent gcErrorEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC,Main.volume.getName(),shortMsg);
		event.level = ERROR;
		return event;
	}
	
	public static SDFSEvent gcWarnEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC,Main.volume.getName(),shortMsg);
		event.level = ERROR;
		return event;
	}
	
	public static SDFSEvent mountInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT,Main.volume.getName(),shortMsg);
		event.level = INFO;
		return event;
	}
	
	public static SDFSEvent mountErrorEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT,Main.volume.getName(),shortMsg);
		event.level = ERROR;
		return event;
	}
	
	public static SDFSEvent mountWarnEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT,Main.volume.getName(),shortMsg);
		event.level = WARN;
		return event;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(format.format(new Date(this.time)));
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
		return sb.toString();
	}
	
	public String toXMLString() {
		StringBuffer sb = new StringBuffer();
		sb.append("<event date=\"");
		sb.append(format.format(new Date(this.time)));
		sb.append("\" level=\"");
		sb.append(this.level);
		sb.append("\" type=\"");
		sb.append(this.type);
		sb.append("\" target=\"");
		sb.append(this.target);
		sb.append("\" shortMsg=\"");
		sb.append(this.shortMsg);
		sb.append("\" longMsg=\"");
		sb.append(this.longMsg);
		sb.append("\" />");
		return sb.toString();
	}
	
	public static String getEvents() {
		Iterator<SDFSEvent> iter = SDFSEvent.runningTasks.values().iterator();
		StringBuffer sb = new StringBuffer();
		while(iter.hasNext()) {
			sb.append(iter.next());
			sb.append("/n");
		}
		return sb.toString();
	}
	
	public static String getXMLEvents() {
		Iterator<SDFSEvent> iter = SDFSEvent.runningTasks.values().iterator();
		StringBuffer sb = new StringBuffer();
		sb.append("<events>");
		while(iter.hasNext()) {
			sb.append(iter.next().toXMLString());
		}
		sb.append("</events>");
		return sb.toString();
	}
	

}
