package org.opendedup.sdfs.notification;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.w3c.dom.Element;

public class ReadAheadEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public MetaDataDedupFile mf;
	public boolean running = false;

	public ReadAheadEvent(String target,MetaDataDedupFile mf) {
		super(RAE, target, "Caching " +mf.getPath(), SDFSEvent.INFO);
		this.running = true;
	}
	
	public void cancelEvent() {
		this.running = false;
	}

	@Override
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("file", mf.getPath());
		return el;
	}

}
