package org.opendedup.util;

import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

public class DomXmlExample {

	/**
	 * Our goal is to create a DOM XML tree and then print the XML.
	 */
	public static void main(String args[]) {
		new DomXmlExample();
	}

	public DomXmlExample() {
		try {
			// ///////////////////////////
			// Creating an empty XML Document

			// We need a Document
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
			Document doc = docBuilder.newDocument();

			// //////////////////////
			// Creating the XML tree

			// create the root element and add it to the document
			Element root = doc.createElement("root");
			doc.appendChild(root);

			// create a comment and put it in the root element
			Comment comment = doc.createComment("Just a thought");
			root.appendChild(comment);

			// create child element, add an attribute, and add to root
			Element child = doc.createElement("child");
			child.setAttribute("name", "value");
			root.appendChild(child);

			// add a text element to the child
			Text text = doc
					.createTextNode("Filler, ... I could have had a foo!");
			child.appendChild(text);

			// ///////////////
			// Output the XML

			// set up a transformer
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

			// print xml
			System.out.println("Here's the xml:\n\n" + xmlString);

		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
