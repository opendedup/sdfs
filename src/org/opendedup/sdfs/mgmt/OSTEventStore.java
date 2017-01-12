package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.OSTEvent;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class OSTEventStore {

	private static TreeMap<Long, OSTEvent> map = new TreeMap<Long, OSTEvent>();
	private static AtomicLong seqnum = new AtomicLong(0);

	public static Element reserverSeqNum() throws ParserConfigurationException {
		long sqnum = seqnum.incrementAndGet();
		SDFSLogger.getLog().debug("reserve sequence number " + sqnum);
		Document doc = XMLUtils.getXMLDoc("seq");
		/*
		 * if (SDFSLogger.isDebug()) SDFSLogger.getLog().debug(this.toString());
		 */
		Element root = doc.getDocumentElement();
		root.setAttribute("num",Long.toString(sqnum));
		return (Element) root.cloneNode(true);
	}

	public static Element getCurrentSeqNum() throws ParserConfigurationException {
		long sqnum = seqnum.get();
		SDFSLogger.getLog().debug("getting sequence number " + sqnum);
		Document doc = XMLUtils.getXMLDoc("seq");
		/*
		 * if (SDFSLogger.isDebug()) SDFSLogger.getLog().debug(this.toString());
		 */
		Element root = doc.getDocumentElement();
		root.setAttribute("num",Long.toString(sqnum));
		return (Element) root.cloneNode(true);
	}

	public static void AddOSTEvent(long id, String data) {
		SDFSLogger.getLog().debug("adding event " + id + " evt=" + data);
		synchronized (map) {
			OSTEvent evt = new OSTEvent();
			evt.ev_seqno = id;
			evt.event = data;
			map.put(id, evt);
		}
	}
	
	public static void AddOSTEvent(long id, String data,String payload) {
		SDFSLogger.getLog().debug("adding event " + id +  "event=" +data+ " payload=" + payload);
		synchronized (map) {
			OSTEvent evt = new OSTEvent();
			evt.ev_seqno = id;
			evt.event = data;
			evt.payload = payload;
			map.put(id, evt);
		}
	}
	
	public static void SetOSTEventPayload(long id, String payload) {
		SDFSLogger.getLog().debug("setting event " + id + " payload=" + payload);
		synchronized (map) {
			map.get(id).payload = payload;
		}
	}
	
	public static void DeleteOSTEvent(long id) {
		SDFSLogger.getLog().debug("deleting event " + id);
		synchronized (map) {
			map.remove(id);
		}
	}

	public static Element getOSTEvent(long id) throws IOException {
		SDFSLogger.getLog().info("getting event " + id);
		try {
			synchronized (map) {
				//OSTEvent evt = map.get(id);
				/*
				if(evt != null)
					SDFSLogger.getLog().debug("getting event id " + id + " data=" + evt.event + " payload=" + evt.payload);
				*/
				return map.get(id).toXML();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request on id " + id, e);
			throw new IOException("request to fetch attributes failed because " + e.toString());
		}
	}

	public static Element getOSTEvents() throws IOException {
		SDFSLogger.getLog().info("getting all events");
		try {
			Document doc = XMLUtils.getXMLDoc("events");
			Element root = doc.getDocumentElement();
			synchronized (map) {
				Iterator<OSTEvent> iter = map.values().iterator();
				while (iter.hasNext()) {
					Element el = iter.next().toXML();
					doc.adoptNode(el);
					root.appendChild(el);
				}
				return (Element) root.cloneNode(true);

			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request", e);
			throw new IOException("request to fetch attributes failed because " + e.toString());
		}
	}

}
