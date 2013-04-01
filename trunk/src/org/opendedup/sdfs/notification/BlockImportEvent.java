package org.opendedup.sdfs.notification;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;

public class BlockImportEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public long blocksImported;
	public long bytesImported;
	public long filesImported;
	public long virtualDataImported;
	
	protected BlockImportEvent(String target, String shortMsg,Level level) {
		super(MIMPORT, target, shortMsg,level);
	}
	
	@Override
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("blocks-imported", Long.toString(this.blocksImported));
		el.setAttribute("bytes-imported", Long.toString(this.bytesImported));
		el.setAttribute("files-imported", Long.toString(this.filesImported));
		el.setAttribute("virtual-data-imported", Long.toString(this.virtualDataImported));
		return el;
	}

}
