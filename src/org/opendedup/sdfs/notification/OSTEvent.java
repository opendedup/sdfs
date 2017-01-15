package org.opendedup.sdfs.notification;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class OSTEvent {
	public long ev_seqno;
	public String event;
	public String payload;
	
	public Document toXML() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("event");
		/*
		 * if (SDFSLogger.isDebug()) SDFSLogger.getLog().debug(this.toString());
		 */
		Element root = doc.getDocumentElement();
		root.setAttribute("ev_seqno", Long.toString(ev_seqno));
		root.setAttribute("event", event);
		if(payload != null)
			root.setAttribute("payload", payload);
		return doc;
	}
}
