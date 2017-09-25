package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;


import java.io.InputStream;
import java.net.URLEncoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.rabin.utils.StringUtils;
import org.opendedup.sdfs.Main;
import org.opendedup.util.EasySSLProtocolSocketFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class MgmtServerConnection {
	public static int port = 6442;
	public static String server = "localhost";
	public static String userName = "admin";
	public static String hmac = null;
	public static boolean useSSL = true;
	private static HttpClient client = null;
	static {
		Protocol easyhttps = new Protocol("https",
				new EasySSLProtocolSocketFactory(), 443);
		Protocol.registerProtocol("https", easyhttps);
		 HttpConnectionManagerParams params = new HttpConnectionManagerParams();  
         params.setDefaultMaxConnectionsPerHost(Main.writeThreads);  
         params.setMaxTotalConnections(Main.writeThreads*2);  
         params.setConnectionTimeout(5000);  
         params.setSoTimeout(5000);  
         MultiThreadedHttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
         httpConnectionManager.setParams(params);
         client =new HttpClient(httpConnectionManager);
		client.getParams().setParameter("http.useragent", "SDFS Client");
		client.getParams().setParameter("http.socket.timeout", 60*1000);
		client.getParams().setParameter("http.connection.timeout", 60*1000);
	}
	
	public static String getAuth(String password) throws IOException {
		InputStream in = null;
		GetMethod method = null;
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			String prot = "http";
			if (useSSL) {
				prot = "https";
			}
			String req = prot + "://" + server + ":" + port + "/session/";
			SDFSLogger.getLog().debug(req);
			method = new GetMethod(req);
			int returnCode = client.executeMethod(method);
			if (returnCode != 200)
				throw new IOException("Unable to process command "
						+ method.getQueryString() + " return code was"
						+ returnCode + " return msg was "
						+ method.getResponseBodyAsString());
			in = method.getResponseBodyAsStream();
			Document doc = db.parse(in);
			Element el = doc.getDocumentElement();
			String salt = el.getAttribute("salt");
			String sessionid = el.getAttribute("session-id");
			String key = HashFunctions.getSHAHash(password.trim().getBytes(),
					salt.getBytes());
			String im = HashFunctions.getHmacSHA256(sessionid,StringUtils.getHexBytes(key))+ ":" +sessionid;
			//System.out.println(im);
			return URLEncoder.encode(im,"UTF-8");
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

	public static Document getResponse(String url) throws IOException {
		InputStream in = null;
		GetMethod method = null;
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();

			if (userName != null && hmac != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(hmac, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(hmac, "UTF-8");
			String prot = "http";
			if (useSSL) {
				prot = "https";
			}
			String req = prot + "://" + server + ":" + port + "/?" + url;
			SDFSLogger.getLog().debug(req);
			method = new GetMethod(req);
			int returnCode = client.executeMethod(method);
			if(returnCode == 403) {
				System.err.println("Authenitcation Failed");
				System.exit(1);
			}
			else if (returnCode != 200)
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
		if (userName != null && hmac != null)
			if (url.trim().length() == 0)
				url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(hmac, "UTF-8");
			else
				url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&password=" + URLEncoder.encode(hmac, "UTF-8");
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
			if(useSSL) {
				req = req.replaceAll("(?<!https:)//", "/");
			}else
				req = req.replaceAll("(?<!http:)//", "/");
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
	
	public static GetMethod connectAndGetHMAC(String server, int port,
			String password, String url, String file, boolean useSSL)
			throws Exception {
		String req = null;
		try {
			if (userName != null && password != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(password, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(password, "UTF-8");
			String prot = "http";
			if (useSSL) {
				prot = "https";

			}
			
			req = prot + "://" + server + ":" + port + "/" + file + "?" + url;
			if(useSSL) {
				req = req.replaceAll("(?<!https:)//", "/");
			}else
				req = req.replaceAll("(?<!http:)//", "/");
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
			String url, String hmac,String file, String postData,
			boolean useSSL) throws Exception {
		String req = null;
		try {
			if (userName != null && hmac != null)
				if (url.trim().length() == 0)
					url = "username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(hmac, "UTF-8");
				else
					url = url + "&username=" + URLEncoder.encode(userName, "UTF-8") + "&hmac=" + URLEncoder.encode(hmac, "UTF-8");
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

	
}