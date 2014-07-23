package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.contrib.ssl.EasySSLProtocolSocketFactory;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.opendedup.logging.SDFSLogger;
import org.w3c.dom.Document;

public class MgmtServerConnection {
	public static int port = 6442;
	public static String server = "localhost";
	public static String userName = "admin";
	public static String password = null;
	public static boolean useSSL = true;
	private static HttpClient client = new HttpClient();
	static {
		Protocol easyhttps = new Protocol("https",
				new EasySSLProtocolSocketFactory(), 443);
		Protocol.registerProtocol("https", easyhttps);
		client.getParams().setParameter("http.useragent", "SDFS Client");
		client.getParams().setParameter("http.socket.timeout", 60*1000);
		client.getParams().setParameter("http.connection.timeout", 60*1000);
	}

	public static Document getResponse(String url) throws IOException {
		InputStream in = null;
		GetMethod method = null;
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();

			if (userName != null && password != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
			String prot = "http";
			if (useSSL) {
				prot = "https";
			}
			String req = prot + "://" + server + ":" + port + "/?" + url;
			// SDFSLogger.getLog().info(req);
			method = new GetMethod(req);
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			in = method.getResponseBodyAsStream();
			Document doc = db.parse(in);
			doc.getDocumentElement().normalize();

			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
			if (method != null) {
				try {
					method.releaseConnection();
				} catch (Exception e) {
				}
			}
		}
	}

	public static GetMethod connectAndGet(String url, String file)
			throws IOException {
		return connectAndGet(url, file, useSSL);
	}

	public static GetMethod connectAndGet(String url, String file,
			boolean useSSL) throws IOException {
		if (userName != null && password != null)
			if (url.trim().length() == 0)
				url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
			else
				url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
		String prot = "http";
		if (useSSL) {
			prot = "https";
		}
		String req = prot + "://" + server + ":" + port + "/" + file + "?"
				+ url;
		// SDFSLogger.getLog().info(req);
		GetMethod method = new GetMethod(req);
		int returnCode = client.executeMethod(method);
		if (returnCode != 200)
			throw new IOException("Unable to process command "
					+ method.getQueryString() + " return code was" + returnCode
					+ " return msg was " + method.getResponseBodyAsString());
		return method;

	}

	public static Document getResponse(String server, int port,
			String password, String url) throws IOException {
		GetMethod m = null;
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			m = connectAndGet(server, port, password, url, "", useSSL);

			Document doc = db.parse(m.getResponseBodyAsStream());
			doc.getDocumentElement().normalize();
			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (m != null) {
				try {
					m.releaseConnection();
				} catch (Exception e) {
				}
			}
		}
	}

	public static GetMethod connectAndGet(String server, int port,
			String password, String url, String file, boolean useSSL)
			throws Exception {
		String req = null;
		try {
			if (userName != null && password != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
			String prot = "http";
			if (useSSL) {
				prot = "https";

			}
			req = prot + "://" + server + ":" + port + "/" + file + "?" + url;
			GetMethod method = new GetMethod(req);
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			return method;
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to connect " + server + " on port " + port);
			SDFSLogger.getLog().error("unable to connect url = " + req);
			throw e;
		}
	}

	public static PostMethod connectAndPost(String server, int port,
			String password, String url, String file, String postData,
			boolean useSSL) throws Exception {
		String req = null;
		try {
			if (userName != null && password != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");
			String prot = "http";
			if (useSSL) {
				prot = "https";

			}
			req = prot + "://" + server + ":" + port + "/" + file + "?" + url;
			// SDFSLogger.getLog().info(req);
			PostMethod method = new PostMethod(req);
			method.addParameter("data", postData);
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			return method;
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to connect " + server + " on port " + port);
			SDFSLogger.getLog().error("unable to connect url = " + req);
			throw e;
		}
	}

	public static void main(String[] args) throws IOException {
		Document doc = getResponse("file=/&cmd=info");
		System.out.println(doc.getDocumentElement().getAttribute("status"));
	}
}
