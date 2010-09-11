package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.SDFSLogger;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetAttributes implements XtendedCmd {

	public String getResult(String cmd, String file) {
		String internalPath = Main.volume.getPath() + File.separator + file;
		MetaDataDedupFile mf = MetaFileStore.getMF(internalPath);
		try {
			Document doc = this.getXMLDoc();
			Element fe = mf.toXML(doc);
			Element root = doc.getDocumentElement();
			root.appendChild(fe);
			return this.toXMLString(doc);
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request on file " + file,e);
			return("request filed because " + e.toString());
		}
	}
	
	private Document getXMLDoc () throws ParserConfigurationException {
		Document xmldoc = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		xmldoc = impl.createDocument(null, "files", null);
		return xmldoc;
	}
	
	private String toXMLString(Document doc) throws TransformerException {
		TransformerFactory transfac = TransformerFactory.newInstance();
		Transformer trans = transfac.newTransformer();
		trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		trans.setOutputProperty(OutputKeys.INDENT, "yes");
		// create string from xml tree
		StringWriter sw = new StringWriter();
		StreamResult result = new StreamResult(sw);
		DOMSource source = new DOMSource(doc);
		trans.transform(source, result);
		String xmlString = sw.toString();
		return xmlString;
	}
	

}
