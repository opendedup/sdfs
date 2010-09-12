package org.opendedup.sdfs.mgmt.cli;


import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.w3c.dom.Document;

public class MgmtServerConnection {
	public static Document getResponse(String url) throws IOException {
		
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(connectAndGet(url));
			doc.getDocumentElement().normalize();
			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private static InputStream connectAndGet(String url) throws IOException {
		HttpClient client = new HttpClient();
		client.getParams().setParameter("http.useragent", "Test Client");

		GetMethod method = new GetMethod("http://localhost:6442/?" + url);
		try {
			int returnCode = client.executeMethod(method);
			if(returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was" + returnCode + " return msg was " + method.getResponseBodyAsString());
			return method.getResponseBodyAsStream();

		} catch (Exception e) {
			throw new IOException("Unable to process command "
					+ method.getQueryString());
		} 

	}

	public static void main(String[] args) throws IOException {
		Document doc = getResponse("file=/&cmd=info");
		System.out.println(doc.getDocumentElement().getAttribute("status"));
	}
}
