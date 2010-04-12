package org.opendedup.sdfs;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.servers.HCServer;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Config {
	private static Logger log = Logger.getLogger("sdfs");

	/**
	 * parse the hubstore config file
	 * 
	 * @param fileName
	 * @throws IOException 
	 */
	public synchronized static void parseHubStoreConfigFile(String fileName) throws IOException {
		try {
			File file = new File(fileName);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			doc.getDocumentElement().normalize();
			
			String version = Main.version;
			if(doc.getDocumentElement().hasAttribute("version")){
				version = doc.getDocumentElement().getAttribute("version");
				Main.version = version;
			}
			
			log.info("Parsing " + doc.getDocumentElement().getNodeName() + " version " + version);
			Element network = (Element) doc.getElementsByTagName("network")
					.item(0);
			Main.serverHostName = network.getAttribute("hostname");
			Main.serverPort = Integer.parseInt(network.getAttribute("port"));
			Main.useUDP = Boolean.parseBoolean(network.getAttribute("use-udp"));
			Element locations = (Element) doc.getElementsByTagName("locations")
					.item(0);
			log.info("parsing folder locations");
			Main.chunkStore = locations.getAttribute("chunk-store");
			Main.hashDBStore = locations.getAttribute("hash-db-store");;
			Element cbe = (Element) doc.getElementsByTagName("chunk-store")
					.item(0);
			Main.preAllocateChunkStore = Boolean.parseBoolean(cbe
					.getAttribute("pre-allocate"));
			Main.chunkStoreAllocationSize = Long.parseLong(cbe
					.getAttribute("allocation-size"));
			Main.chunkStorePageSize = Integer.parseInt(cbe
					.getAttribute("page-size"));
			Main.chunkStoreReadAheadPages = Integer.parseInt(cbe
					.getAttribute("read-ahead-pages"));
			Main.gcChunksSchedule = cbe.getAttribute("chunk-gc-schedule");
			Main.evictionAge = Integer.parseInt(cbe.getAttribute("eviction-age"));
			int awsSz = doc.getElementsByTagName("aws").getLength();
			if (awsSz > 0) {
				Main.AWSChunkStore = true;
				Element aws = (Element) doc.getElementsByTagName("aws").item(0);
				Main.awsAccessKey = aws.getAttribute("aws-access-key");
				Main.awsSecretKey = aws.getAttribute("aws-secret-key");
			}
			File f = new File(Main.chunkStore);
			if (!f.exists()) {
				log.info("creating chunk store at " + Main.chunkStore);
				f.mkdirs();
			}

			f = new File(Main.hashDBStore);
			if (!f.exists()) {
				log.info("creating hash database store at " + Main.chunkStore);
				f.mkdirs();
			}

		} catch (Exception e) {
			log.log(Level.SEVERE,"unable to parse config file ["+fileName+"]", e);
			throw new IOException(e);
		}
	}

	/**
	 * parse the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static void parseSDFSConfigFile(String fileName)
			throws Exception {
		File file = new File(fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		
		String version = "0.8.12";
		if(doc.getDocumentElement().hasAttribute("version")){
			version = doc.getDocumentElement().getAttribute("version");
			Main.version = version;
		}
		
		Main.version = version;
		log.info("Parsing " + doc.getDocumentElement().getNodeName() + " version " + version);
		Element locations = (Element) doc.getElementsByTagName("locations")
				.item(0);
		log.info("parsing folder locations");
		Main.metaDBStore = locations.getAttribute("meta-db-store");
		Main.dedupDBStore = locations.getAttribute("dedup-db-store");
		Main.ioLogFile = locations.getAttribute("io-log");
		Element cache = (Element) doc.getElementsByTagName("io").item(0);
		// Close files when close cmd is executed. This should be set to false
		// if running over nfs
		Main.safeClose = Boolean.parseBoolean(cache.getAttribute("safe-close"));
		// Makes sure writes are sync'd when set to true.
		Main.safeSync = Boolean.parseBoolean(cache.getAttribute("safe-sync"));
		Main.writeThreads = Short.parseShort(cache
				.getAttribute("write-threads"));
		Main.dedupFiles = Boolean.parseBoolean(cache
				.getAttribute("dedup-files"));
		Main.CHUNK_LENGTH = Integer.parseInt(cache.getAttribute("chunk-size")) * 1024;
		Main.multiReadTimeout = Integer.parseInt(cache
				.getAttribute("multi-read-timeout"));
		Main.blankHash = new byte[Main.CHUNK_LENGTH];
		Main.systemReadCacheSize = Integer.parseInt(cache
				.getAttribute("system-read-cache"));
		Main.maxWriteBuffers = Integer.parseInt(cache
				.getAttribute("max-file-write-buffers"));
		Main.maxOpenFiles = Integer.parseInt(cache
				.getAttribute("max-open-files"));
		Main.maxInactiveFileTime = Integer.parseInt(cache
				.getAttribute("max-file-inactive")) * 1000;
		Main.fDkiskSchedule =  cache.getAttribute("claim-hash-schedule");
		Element volume = (Element) doc.getElementsByTagName("volume").item(0);
		Main.volume = new Volume(volume);
		Element permissions = (Element) doc.getElementsByTagName("permissions")
				.item(0);
		Main.defaultGroup = Integer.parseInt(permissions
				.getAttribute("default-group"));
		Main.defaultOwner = Integer.parseInt(permissions
				.getAttribute("default-owner"));
		Main.defaultFilePermissions = Integer.parseInt(permissions
				.getAttribute("default-file"));
		Main.defaultDirPermissions = Integer.parseInt(permissions
				.getAttribute("default-folder"));
		Main.chunkStorePageSize = Main.CHUNK_LENGTH;
		
		log.fine("parsing local chunkstore parameters");
		Element localChunkStore = (Element) doc.getElementsByTagName(
				"local-chunkstore").item(0);
		Main.chunkStoreLocal = Boolean.parseBoolean(localChunkStore
				.getAttribute("enabled"));
		if (Main.chunkStoreLocal) {
			log.fine("this is a local chunkstore");
			Main.chunkStore = localChunkStore.getAttribute("chunk-store");
			// Main.chunkStoreMetaData =
			// localChunkStore.getAttribute("chunk-store-metadata");
			Main.chunkStoreAllocationSize = Long.parseLong(
					localChunkStore.getAttribute("allocation-size"));
			Main.gcChunksSchedule = localChunkStore.getAttribute("chunk-gc-schedule");
			Main.evictionAge = Integer.parseInt(localChunkStore.getAttribute("eviction-age"));
			Main.hashDBStore = localChunkStore.getAttribute("hash-db-store");
			Main.preAllocateChunkStore = Boolean.parseBoolean(localChunkStore
					.getAttribute("pre-allocate"));
			Main.chunkStoreReadAheadPages = Integer.parseInt(localChunkStore
					.getAttribute("read-ahead-pages"));
			log.info("######### Will allocate " + Main.chunkStoreAllocationSize
					+ " in chunkstore ##############");
		}

		/*
		IOMeter meter = new IOMeter(Main.ioLogFile);
		Thread th = new Thread(meter);
		th.start();
		*/
	}

	/**
	 * write the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static void writeSDFSConfigFile(String fileName)
			throws Exception {
		File file = new File(fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		Element volume = (Element) doc.getElementsByTagName("volume").item(0);
		System.out.println("Writing volume config = "
				+ volume.getAttribute("path"));
		volume.setAttribute("current-size", Long.toString(Main.volume
				.getCurrentSize()));
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
	}

	public static synchronized void parserRoutingFile(String fileName)
			throws IOException {
		if (Main.chunkStoreLocal)
			return;
		File file = new File(fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			log.log(Level.SEVERE, "unable to parse config file [" + fileName + "]",e1);
			throw new IOException(e1);
		}
		Document doc = null;
		try {
			doc = db.parse(file);
		} catch (SAXException e1) {
			log.log(Level.SEVERE, "unable to parse config file [" + fileName + "]",e1);
			throw new IOException(e1);
		}
		doc.getDocumentElement().normalize();
		log.info("Parsing " + doc.getDocumentElement().getNodeName());
		Element servers = (Element) doc.getElementsByTagName("servers").item(0);

		log.info("parsing Servers");
		NodeList server = servers.getElementsByTagName("server");
		for (int s = 0; s < server.getLength(); s++) {
			log.info("Connection to  Servers [" + server.getLength() + "]");
			Element _server = (Element) server.item(s);
			HCServer hcs = new HCServer(_server.getAttribute("host").trim(),
					Integer.parseInt(_server.getAttribute("port").trim()),
					Boolean.parseBoolean(_server.getAttribute("use-udp")),
					Boolean.parseBoolean(_server.getAttribute("compress")));
			try {
				HCServiceProxy.writeServers.put(_server.getAttribute("name")
						.trim(), new HashClientPool(hcs, _server.getAttribute(
						"name").trim(), Integer.parseInt(_server
						.getAttribute("network-threads"))));
				HCServiceProxy.readServers.put(_server.getAttribute("name")
						.trim(), new HashClientPool(hcs, _server.getAttribute(
						"name").trim(), Integer.parseInt(_server
						.getAttribute("network-threads"))));
			} catch (Exception e) {
				log.log(Level.WARNING, "unable to connect to server "
						+ _server.getAttribute("name").trim(), e);
				throw new IOException("unable to connect to server");
			}
			log.info("Added Server " + _server.getAttribute("name"));
		}
		Element _c = (Element) doc.getElementsByTagName("chunks").item(0);
		NodeList chunks = _c.getElementsByTagName("chunk");
		for (int s = 0; s < chunks.getLength(); s++) {
			Element chunk = (Element) chunks.item(s);
			HCServiceProxy.writehashRoutes.put(chunk.getAttribute("name")
					.trim(), HCServiceProxy.writeServers.get(chunk
					.getAttribute("server").trim()));
			HCServiceProxy.readhashRoutes.put(
					chunk.getAttribute("name").trim(),
					HCServiceProxy.readServers.get(chunk.getAttribute("server")
							.trim()));
		}

	}

}
