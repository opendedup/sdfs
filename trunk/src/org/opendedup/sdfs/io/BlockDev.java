package org.opendedup.sdfs.io;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class BlockDev {
	String devName;
	String devPath;
	long size;
	boolean startOnInit;
	
	public BlockDev(String devName,String devPath,long size,boolean start) {
		this.devName = devName;
		this.devPath = devPath;
		this.size = size;
		this.startOnInit = start;
	}
	
	public BlockDev(Element el) {
		this.devName = el.getAttribute("devicename");
		this.devPath = el.getAttribute("devpath");
		this.size = Long.parseLong(el.getAttribute("size"));
		this.startOnInit = Boolean.parseBoolean(el.getAttribute("start-on-init"));
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
	
	public Document toXMLDocument() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("blockdev");
		Element root = doc.getDocumentElement();
		root.setAttribute("devname", this.devName);
		root.setAttribute("devpath", this.devPath);
		root.setAttribute("size", Long.toString(size));
		root.setAttribute("start-on-init", Boolean.toString(this.startOnInit));
		return doc;
	}
	
	public Element getElement() throws ParserConfigurationException {
		
		return (Element) this.toXMLDocument().getDocumentElement().cloneNode(true);
	}
}
