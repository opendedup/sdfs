package org.opendedup.sdfs.replication;

import java.io.File;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Formatter;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.opendedup.util.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ReplicationService {
	private static Properties properties = new Properties();
	public String remoteServer;
	public String remoteServerPassword;
	public String remoteServerFolder;
	public int remoteServerPort;
	public String archiveFolder;

	public String localServer;
	public String localServerPassword;
	public String localServerFolder;
	public int localServerPort;

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("sdfsreplicate <archive-config-file>");
			System.exit(-1);
		}
		System.out.println("Starting SDFS Replication Service");
		System.out.println("Reading properties from " + args[0]);
		try {
			properties.load(new FileInputStream(args[0]));
		} catch (IOException e) {
			System.err.println("Unable to load properties");
			e.printStackTrace();
			System.exit(-1);
		}
		SDFSLogger.setToFileAppender(properties.getProperty("logfile",
				"/var/log/sdfs/archiveout.log"));

		SDFSLogger.getLog().info(
				"Starting Replication " + properties.toString());
		new ReplicationService(properties);

	}

	public ReplicationService(Properties props) throws IOException {
		this.remoteServer = properties.getProperty("replication.master");
		this.remoteServerPassword = properties.getProperty(
				"replication.master.password", "admin");
		this.remoteServerFolder = properties.getProperty(
				"replication.master.folder", "");
		this.remoteServerPort = Integer.parseInt(properties.getProperty(
				"replication.master.port", "6442"));
		this.archiveFolder = properties.getProperty("archive.staging",
				System.getProperty("java.io.tmpdir"));
		this.localServer = properties.getProperty("replication.slave");
		this.localServerPassword = properties.getProperty(
				"replication.slave.password", "admin");
		this.localServerFolder = properties.getProperty(
				"replication.slave.folder", "");
		this.localServerPort = Integer.parseInt(properties.getProperty(
				"replication.slave.port", "6442"));
		String schedType = properties.getProperty("schedule.type", "single");
		if (schedType.equalsIgnoreCase("cron")) {
			// by default every hour
			String schedt = properties.getProperty("schedule.cron",
					"0 0 0/1 * * ?");
			SDFSLogger.getLog().info("will schedule replication " + schedt);
			ReplicationScheduler rsched = new ReplicationScheduler(schedt, this);
			Runtime.getRuntime().addShutdownHook(
					new ShutdownHook(rsched, this.remoteServer + ":"
							+ this.remoteServerPort + ":"
							+ this.remoteServerFolder));
		} else {
			SDFSLogger.getLog().info("Running replication once");
			this.replicate();
			SDFSLogger.getLog().info("done replicating");
		}
	}

	public void replicate() throws IOException {
		String archive = null;

		try {
			SDFSLogger.getLog().info(
					"replicating " + remoteServer + ":" + remoteServerPort
							+ ":" + remoteServerFolder + " to " + localServer
							+ ":" + localServerPort + ":" + localServerFolder);
			archive = getRemoteArchive(remoteServer, remoteServerPort,
					remoteServerPassword, archiveFolder, remoteServerFolder);
			SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmmss");
			String tmStr = df.format(new Date());
			String lFldr = localServerFolder.replaceAll("%d", tmStr)
					.replaceAll("%h", remoteServer);

			localArchiveImport(localServer, localServerPort,
					localServerPassword, archive, lFldr);
		} finally {
			if (archive != null) {
				File arcF = new File(archive);
				arcF.delete();
			}
		}
	}

	private synchronized Document getResponse(String server, int port,
			String password, String url) throws IOException {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(connectAndGet(server, port, password, url,
					""));
			doc.getDocumentElement().normalize();
			return doc;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private synchronized InputStream connectAndGet(String server, int port,
			String password, String url, String file) {
		HttpClient client = new HttpClient();
		client.getParams().setParameter("http.useragent", "SDFS Client");
		if (password != null)
			if (url.trim().length() == 0)
				url = "password=" + password;
			else
				url = url + "&password=" + password;
		String req = "http://" + server + ":" + port + "/" + file + "?" + url;
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
			System.err.println("Error Request : " + req);
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	private void localArchiveImport(String server, int port, String password,
			String archive, String path) throws IOException {
		SDFSLogger.getLog().debug(
				"importing [" + archive + "] destination is [" + path + "]");
		archive = URLEncoder.encode(archive, "UTF-8");
		path = URLEncoder.encode(path, "UTF-8");
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		formatter.format("file=%s&cmd=importarchive&options=%s", archive, path);
		Document doc = getResponse(server, port, password, sb.toString());
		Element root = doc.getDocumentElement();
		if (root.getAttribute("status").equals("failed"))
			SDFSLogger.getLog().error("msg");
		String status = root.getAttribute("status");
		String msg = root.getAttribute("msg");
		SDFSLogger.getLog().info(
				"Import [" + status + "] returned [" + msg + "]");
		if (status.equalsIgnoreCase("failed"))
			throw new IOException("Import failed because " + msg);

	}

	private String getRemoteArchive(String server, int port, String password,
			String tempDir, String file) throws IOException {
		SDFSLogger.getLog().debug("archive a copy of [" + file + "]");
		file = URLEncoder.encode(file, "UTF-8");
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		formatter.format("file=%s&cmd=archiveout&options=iloveanne", file);
		Document doc = getResponse(server, port, password, sb.toString());
		Element root = doc.getDocumentElement();
		String status = root.getAttribute("status");
		String msg = root.getAttribute("msg");
		SDFSLogger.getLog().debug("getting " + msg);
		InputStream in = connectAndGet(server, port, password, "", msg);
		File outFile = new File(tempDir + File.separator + msg);
		FileOutputStream out = new FileOutputStream(outFile);
		byte[] buf = new byte[32768];
		int len;
		while ((len = in.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		in.close();
		out.close();
		SDFSLogger.getLog().debug(
				"Copy Out [" + status + "] returned [" + msg + "]");
		if (status.equalsIgnoreCase("failed"))
			throw new IOException("archive failed because " + msg);
		return outFile.getPath();

	}

}
