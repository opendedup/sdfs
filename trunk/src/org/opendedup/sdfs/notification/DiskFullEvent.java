package org.opendedup.sdfs.notification;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;

public class DiskFullEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public long currentSz;
	public long maxSz;
	public long dseSz;
	public long maxDseSz;
	public long dskUsage;
	public long maxDskUsage;

	public DiskFullEvent(String shortMsg) {
		super(DSKFL, getTarget(), shortMsg, SDFSEvent.ERROR);
	}

	@Override
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("current-size", Long.toString(this.currentSz));
		el.setAttribute("max-size", Long.toString(this.maxSz));
		el.setAttribute("dse-size", Long.toString(this.dseSz));
		el.setAttribute("dse-max-size", Long.toString(this.maxDseSz));
		el.setAttribute("disk-usage", Long.toString(this.dskUsage));
		el.setAttribute("max-disk-usage", Long.toString(this.maxDskUsage));
		return el;
	}

}
