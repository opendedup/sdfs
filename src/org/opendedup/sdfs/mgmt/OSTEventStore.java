package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.OSTEvent;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class OSTEventStore {

	private static AtomicLong seqnum = new AtomicLong(0);
	private static File dir = new File(new File(Main.volume.getPath()).getParentFile().getPath() + File.separator + "ostevents" + File.separator);
	static  {
		dir.mkdirs();
		File[] fls = dir.listFiles();
		for(File f : fls) {
			long id = Long.parseLong(f.getName().substring(0,f.getName().length()-4));
			if(id > seqnum.get())
				seqnum.set(id);
		}
	}

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

	public static void AddOSTEvent(long id, String data) throws TransformerException, IOException, ParserConfigurationException {
		SDFSLogger.getLog().debug("adding event " + id + " evt=" + data);
		synchronized (seqnum) {
			OSTEvent evt = new OSTEvent();
			evt.ev_seqno = id;
			evt.event = data;
			XMLUtils.toXMLFile(evt.toXML(), new File(dir.getPath() + id + ".xml").getPath());
		}
	}
	
	public static void AddOSTEvent(long id, String data,String payload) throws TransformerException, IOException, ParserConfigurationException {
		SDFSLogger.getLog().debug("adding event " + id +  "event=" +data+ " payload=" + payload);
		synchronized (seqnum) {
			OSTEvent evt = new OSTEvent();
			evt.ev_seqno = id;
			evt.event = data;
			evt.payload = payload;
			XMLUtils.toXMLFile(evt.toXML(), new File(dir.getPath() + id + ".xml").getPath());
		}
	}
	
	public static void SetOSTEventPayload(long id, String payload) throws Exception {
		SDFSLogger.getLog().debug("setting event " + id + " payload=" + payload);
		synchronized (seqnum) {
			Document doc = XMLUtils.toXMLDocument(dir.getPath() + id + ".xml");
			Element root = doc.getDocumentElement();
			root.setAttribute("payload", payload);
			XMLUtils.toXMLFile(doc, new File(dir.getPath() + id + ".xml").getPath());
		}
	}
	
	public static void DeleteOSTEvent(long id) {
		SDFSLogger.getLog().debug("deleting event " + id);
		synchronized (seqnum) {
			File f = new File(dir.getPath() + id + ".xml");
			f.delete();
		}
	}

	public static Element getOSTEvent(long id) throws IOException {
		SDFSLogger.getLog().debug("getting event " + id);
		try {
			synchronized (seqnum) {
				//OSTEvent evt = map.get(id);
				/*
				if(evt != null)
					SDFSLogger.getLog().debug("getting event id " + id + " data=" + evt.event + " payload=" + evt.payload);
				*/
					return XMLUtils.toXMLElement(dir.getPath() + id + ".xml");
					
			}
		} catch(FileNotFoundException e) {
			throw new IOException("request to fetch attributes failed because " + e.toString());
		}catch (Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request on id " + id, e);
			throw new IOException("request to fetch attributes failed because " + e.toString());
		}
	}

	public static Element getOSTEvents() throws IOException {
		SDFSLogger.getLog().debug("getting all events");
		try {
			Document doc = XMLUtils.getXMLDoc("events");
			Element root = doc.getDocumentElement();
			synchronized (seqnum) {
				File[] fls = dir.listFiles();
				for(File f : fls) {
					Element el = XMLUtils.toXMLElement(f.getPath());
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
