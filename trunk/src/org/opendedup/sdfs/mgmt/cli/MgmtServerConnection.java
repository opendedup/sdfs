package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.w3c.dom.Document;

public class MgmtServerConnection {
	public static int port = 6442;
	public static String server = "localhost";
	public static String userName = "admin";
	public static String password = null;
	public static Document getResponse(String url) throws IOException {

		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(connectAndGet(url,""));
			doc.getDocumentElement().normalize();
			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static InputStream connectAndGet(String url,String file) {
		HttpClient client = new HttpClient();
		client.getParams().setParameter("http.useragent", "SDFS Client");
		if(userName != null && password != null)
			if(url.trim().length() ==0)
				url = "username="+userName+"&password="+password;
			else
				url = url + "&username="+userName+"&password="+password;
		String req = "http://"+server+":"+port +"/"+file+ "?" + url;
		GetMethod method = new GetMethod(req);
		try {
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			return method.getResponseBodyAsStream();
		} catch (Exception e) {
			System.err
					.println("Error : It does not appear the SDFS volume is mounted or listening on tcp port 6442");
			System.err
			.println("Error Request : " + req);
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}
	
	public static Document getResponse(String server,int port,String password,String url) throws IOException {

		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(connectAndGet(server,port,password,url,""));
			doc.getDocumentElement().normalize();
			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static InputStream connectAndGet(String server,int port,String password,String url,String file) {
		HttpClient client = new HttpClient();
		client.getParams().setParameter("http.useragent", "SDFS Client");
		if(userName != null && password != null)
			if(url.trim().length() ==0)
				url = "username="+userName+"&password="+password;
			else
				url = url + "&username="+userName+"&password="+password;
		String req = "http://"+server+":"+port +"/"+file+ "?" + url;
		GetMethod method = new GetMethod(req);
		try {
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			return method.getResponseBodyAsStream();
		} catch (Exception e) {
			System.err
					.println("Error : It does not appear the SDFS volume is mounted or listening on tcp port 6442");
			System.err
			.println("Error Request : " + req);
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	public static void main(String[] args) throws IOException {
		Document doc = getResponse("file=/&cmd=info");
		System.out.println(doc.getDocumentElement().getAttribute("status"));
	}
}
