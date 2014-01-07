package org.opendedup.sdfs.replication;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Formatter;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.cli.MgmtServerConnection;
import org.opendedup.sdfs.mgmt.cli.ProcessArchiveOutCmd;
import org.opendedup.sdfs.mgmt.cli.ProcessImportArchiveCmd;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ReplicationService implements Serializable {
	private static final long serialVersionUID = -1336095733377813747L;
	private static Properties properties = new Properties();
	public String remoteServer;
	public transient String remoteServerPassword;
	public String remoteServerFolder;
	public String remoteServerVolume;
	//public int remoteServerDataPort;
	public int remoteServerPort;
	public boolean useSSL;
	public boolean useMGR;
	public String archiveFolder;

	public String localServer;
	public transient String localServerPassword;
	public String localServerFolder;
	public String mLocalServerFolder;
	public int localServerPort;
	public int localCopies = -1;
	public static File defjobPersistanceFolder = null;
	public File jobPersistanceFolder = null;
	public int maxSz;
	public String schedType;
	public String schedt = "";

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("sdfsreplicate <archive-config-file>");
			System.exit(-1);
		}
		System.out.println("Starting SDFS Replication Service");
		System.out.println("Reading properties from " + args[0]);

		try {
			File f = new File(args[0]);
			defjobPersistanceFolder = new File("replhistory" + File.separator + f.getName());
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
		this.remoteServer = properties.getProperty("replication.master").trim();
		this.remoteServerPassword = properties.getProperty(
				"replication.master.password", "admin");
		this.remoteServerFolder = properties.getProperty(
				"replication.master.folder", "").trim();
		this.remoteServerPort = Integer.parseInt(properties.getProperty(
				"replication.master.port", "6442"));
		this.useSSL = Boolean.parseBoolean(properties.getProperty(
				"replication.master.useSSL", "true"));
		this.useMGR = Boolean.parseBoolean(properties.getProperty(
				"replication.master.usemgr", "false"));
		this.archiveFolder = properties.getProperty("archive.staging",
				System.getProperty("java.io.tmpdir"));
		this.localServer = properties.getProperty("replication.slave");
		this.localServerPassword = properties.getProperty(
				"replication.slave.password", "admin");
		this.localServerFolder = properties.getProperty(
				"replication.slave.folder", "");
		this.localServerPort = Integer.parseInt(properties.getProperty(
				"replication.slave.port", "6442"));
		this.maxSz = Integer.parseInt(properties.getProperty(
				"replication.batchsize", "-1"));
		this.jobPersistanceFolder = new File(properties.getProperty(
				"job.history.folder", defjobPersistanceFolder.getPath()));
		this.localCopies = Integer.parseInt(properties.getProperty(
				"replication.copies", "-1"));
		this.jobPersistanceFolder.mkdirs();
		SDFSLogger.getLog().info(
				"Will persist job info to "
						+ this.jobPersistanceFolder.getPath());
		this.schedType = properties.getProperty("schedule.type", "single");
		if (schedType.equalsIgnoreCase("cron")) {
			// by default every hour
			schedt = properties.getProperty("schedule.cron", "0 0 0/1 * * ?");
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
			SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd-HH-mm-ss");
			String tmStr = df.format(new Date());
			this.mLocalServerFolder = localServerFolder.replaceAll("%d", tmStr)
					.replaceAll("%h", remoteServer);

			localArchiveImport(localServer, localServerPort,
					localServerPassword, archive, this.mLocalServerFolder,
					remoteServer, remoteServerPassword, remoteServerPort);
			this.persistResults();

		} finally {
			if (archive != null) {
				File arcF = new File(archive);
				arcF.delete();
			}
			this.removeOldReplicationJobs();
		}
	}

	private synchronized void persistResults() {

		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();

			DOMImplementation impl = builder.getDOMImplementation();
			// Document.
			Document doc = impl.createDocument(null, "replication-job", null);
			// Root element.
			Element root = doc.getDocumentElement();
			root.setAttribute("version", Main.version);
			root.setAttribute("timestamp",
					Long.toString(System.currentTimeMillis()));
			root.setAttribute("slave", this.localServer);
			root.setAttribute("slave-port",
					Integer.toString(this.localServerPort));
			root.setAttribute("master", this.remoteServer);
			root.setAttribute("master-port",
					Integer.toString(this.remoteServerPort));
			root.setAttribute("master-folder", this.remoteServerFolder);
			root.setAttribute("slave-folder", this.mLocalServerFolder);
			/*
			 * root.setAttribute("result", impResults.getDocumentElement()
			 * .getAttribute("status")); root.setAttribute("result-msg",
			 * impResults.getDocumentElement() .getAttribute("msg"));
			 */
			root.setAttribute("schedule-type", schedType);
			root.setAttribute("cron-string", schedt);
			/*
			 * if (impResults.getDocumentElement()
			 * .getElementsByTagName("replication-import").item(0) != null) {
			 * Element iEl = (Element) impResults.getDocumentElement()
			 * .getElementsByTagName("replication-import").item(0)
			 * .cloneNode(true); doc.adoptNode(iEl); root.appendChild(iEl); }
			 */
			SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd-HH-mm-ss");
			String tmStr = df.format(new Date());
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			Result result = new StreamResult(jobPersistanceFolder.getPath()
					+ File.separator + tmStr);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.setOutputProperty(OutputKeys.INDENT, "yes");
			xformer.transform(source, result);
			SDFSLogger.getLog().info(
					"persisted replication results to "
							+ jobPersistanceFolder.getPath() + File.separator
							+ tmStr);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to persist results", e);
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
			String password, String url, String file) throws IOException {
		HttpClient client = new HttpClient();
		client.getParams().setParameter("http.useragent", "SDFS Client");
		if (password != null)
			if (url.trim().length() == 0)
				url = "password=" + password;
			else
				url = url + "&password=" + password;
		file = URLEncoder.encode(file, "UTF-8");
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
			String archive, String path, String rserver, String rpasswd,
			int rport) throws IOException {
		MgmtServerConnection.server = server;
		MgmtServerConnection.password = password;
		MgmtServerConnection.port = port;
		ProcessImportArchiveCmd.runCmd(archive, path, rserver, rpasswd, rport,
				false, maxSz);

	}

	private String getRemoteArchive(String server, int port, String password,
			String tempDir, String file) throws IOException {
		MgmtServerConnection.server = server;
		MgmtServerConnection.password = password;
		MgmtServerConnection.port = port;
		SDFSLogger.getLog().debug("archive a copy of [" + file + "]");
		return ProcessArchiveOutCmd.runCmd(file, tempDir);

	}

	private void removeOldReplicationJobs() {

		if (this.localCopies > 0) {
			SDFSLogger.getLog().info(
					"Will keep [" + this.localCopies
							+ "] copies of the replicated folder ["
							+ this.remoteServerFolder + "]");
			File[] jobsxml = this.jobPersistanceFolder.listFiles();
			ArrayList<JobHistory> jobs = new ArrayList<JobHistory>();

			for (int i = 0; i < jobsxml.length; i++) {
				try {
					JobHistory jh = parseHistoryFile(jobsxml[i].getPath());
					if (jh.success)
						jobs.add(jh);
				} catch (Exception e) {
					SDFSLogger
							.getLog()
							.error(jobsxml[i].getPath()
									+ " does not look like a replication history file",
									e);
				}
			}
			int diff = jobs.size() - this.localCopies;
			if (diff > 0) {
				SDFSLogger.getLog().info(
						"Will remove " + diff + " old replication jobs");
				Collections.sort(jobs, new CustomComparator());
				for (int i = 0; i < diff; i++) {
					try {
						String rfile = jobs.get(i).slaveFolder;
						this.removeFolder(rfile);
						File f = new File(jobs.get(i).xmlFile);
						f.delete();
						SDFSLogger
								.getLog()
								.warn("Successfully deleted "
										+ jobs.get(i).slaveFolder
										+ " and xml file" + jobs.get(i).xmlFile);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to delete " + jobs.get(i).slaveFolder
										+ " from xml file"
										+ jobs.get(i).xmlFile, e);
					}
				}
			}
		} else {
			SDFSLogger.getLog().info(
					"Will keep [ALL] copies of the replicated folder ["
							+ this.remoteServerFolder + "]");

		}
	}

	private void removeFolder(String file) throws IOException {
		if (file.equals(mLocalServerFolder))
			SDFSLogger.getLog().info(
					"ignoring deletion because file = "
							+ this.mLocalServerFolder);
		else {
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			SDFSLogger.getLog().debug("Deleting File [" + file + "] ");
			formatter.format("file=%s&cmd=%s&options=%s", file, "deletefile",
					"");
			Document doc = getResponse(this.localServer, this.localServerPort,
					this.localServerPassword, sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			if (status.equalsIgnoreCase("failed"))
				throw new IOException("delete of " + file + " failed because :"
						+ msg);
		}
	}

	private JobHistory parseHistoryFile(String fileName) throws Exception {
		SDFSLogger.getLog().debug("Parsing " + fileName);
		File file = new File(fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		Element root = doc.getDocumentElement();
		JobHistory hist = new JobHistory();
		hist.slaveFolder = root.getAttribute("slave-folder");
		hist.timeStamp = Long.parseLong(root.getAttribute("timestamp"));
		hist.xmlFile = fileName;
		if (root.getAttribute("result").equalsIgnoreCase("success"))
			hist.success = true;
		SDFSLogger.getLog().debug("parsed jobhistory " + hist);
		return hist;
	}

	private class JobHistory {
		long timeStamp;
		String slaveFolder;
		String xmlFile;
		boolean success;

		private Date getEndDate() {
			return new Date(timeStamp);
		}

		@Override
		public String toString() {
			return "jobhistory=" + timeStamp + ":" + slaveFolder + ":"
					+ xmlFile + ":" + success;
		}

	}

	private class CustomComparator implements Comparator<JobHistory> {
		@Override
		public int compare(JobHistory o1, JobHistory o2) {
			return o1.getEndDate().compareTo(o2.getEndDate());
		}
	}

}
