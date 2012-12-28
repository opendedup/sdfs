package org.opendedup.sdfs;

import java.io.File;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.servers.HCServer;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Config {

	/**
	 * parse the hubstore config file
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public synchronized static void parseDSEConfigFile(String fileName)
			throws IOException {
		try {
			File file = new File(fileName);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();

			Document doc = db.parse(file);
			doc.getDocumentElement().normalize();

			String version = Main.version;
			if (doc.getDocumentElement().hasAttribute("version")) {
				version = doc.getDocumentElement().getAttribute("version");
				Main.version = version;
			}

			SDFSLogger.getLog().info(
					"Parsing " + doc.getDocumentElement().getNodeName()
							+ " version " + version);
			Element network = (Element) doc.getElementsByTagName("network")
					.item(0);
			Main.serverHostName = network.getAttribute("hostname");
			Main.serverPort = Integer.parseInt(network.getAttribute("port"));
			Main.useUDP = Boolean.parseBoolean(network.getAttribute("use-udp"));
			Main.enableNetworkChunkStore = true;
			if (network.hasAttribute("upstream-enabled")) {
				Main.upStreamDSEHostEnabled = Boolean.parseBoolean(network
						.getAttribute("upstream-enabled"));
				Main.upStreamDSEHostName = network
						.getAttribute("upstream-host");
				Main.upStreamDSEPort = Integer.parseInt(network
						.getAttribute("upstream-host-port"));
				Main.upStreamPassword = network
						.getAttribute("upstream-password");
			}
			if(network.hasAttribute("use-ssl"))
				Main.serverUseSSL = Boolean.parseBoolean(network.getAttribute("use-ssl"));
			Element locations = (Element) doc.getElementsByTagName("locations")
					.item(0);
			SDFSLogger.getLog().info("parsing folder locations");
			Main.chunkStore = locations.getAttribute("chunk-store");
			Main.hashDBStore = locations.getAttribute("hash-db-store");
			Element cbe = (Element) doc.getElementsByTagName("chunk-store")
					.item(0);
			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.FileChunkStore";
			if (cbe.hasAttribute("chunkstore-class")) {
				Main.chunkStoreClass = cbe.getAttribute("chunkstore-class");
			}
			if (cbe.hasAttribute("hashdb-class"))
				Main.hashesDBClass = cbe.getAttribute("hashdb-class");
			if (cbe.getElementsByTagName("extended-config").getLength() > 0) {
				Main.chunkStoreConfig = (Element) cbe.getElementsByTagName(
						"extended-config").item(0);
			}
			Main.preAllocateChunkStore = Boolean.parseBoolean(cbe
					.getAttribute("pre-allocate"));
			Main.chunkStoreAllocationSize = Long.parseLong(cbe
					.getAttribute("allocation-size"));
			Main.chunkStorePageSize = Integer.parseInt(cbe
					.getAttribute("page-size"));
			Main.chunkStoreReadAheadPages = Integer.parseInt(cbe
					.getAttribute("read-ahead-pages"));
			Main.gcChunksSchedule = cbe.getAttribute("chunk-gc-schedule");
			if (cbe.hasAttribute("hash-size")) {
				short hsz = Short.parseShort(cbe.getAttribute("hash-size"));
				if (hsz == 16)
					Main.hashType = HashFunctionPool.TIGER_16;
				if (hsz == 24)
					Main.hashType = HashFunctionPool.TIGER_24;
				SDFSLogger.getLog().info(
						"Setting hash engine to " + Main.hashType);
			}
			if (cbe.hasAttribute("hash-type")) {
				Main.hashType = cbe.getAttribute("hash-type");
				SDFSLogger.getLog().info(
						"Setting hash engine to " + Main.hashType);
			}
			Main.evictionAge = Integer.parseInt(cbe
					.getAttribute("eviction-age"));
			if (cbe.hasAttribute("chunk-store-read-cache"))
				;
			Main.chunkStorePageCache = Integer.parseInt(cbe
					.getAttribute("chunk-store-read-cache"));
			if (cbe.hasAttribute("chunk-store-dirty-timeout"))
				Main.chunkStoreDirtyCacheTimeout = Integer.parseInt(cbe
						.getAttribute("chunk-store-dirty-timeout"));
			if (cbe.hasAttribute("encrypt")) {
				Main.chunkStoreEncryptionEnabled = Boolean.parseBoolean(cbe
						.getAttribute("encrypt"));
				Main.chunkStoreEncryptionKey = cbe
						.getAttribute("encryption-key");
			}
			if (cbe.hasAttribute("compress")) {
				Main.cloudCompress = Boolean.parseBoolean(cbe
						.getAttribute("compress"));
			}
			if (cbe.hasAttribute("max-repl-batch-sz"))
				Main.MAX_REPL_BATCH_SZ = Integer.parseInt(cbe.getAttribute("max-repl-batch-sz"));
			int awsSz = doc.getElementsByTagName("aws").getLength();
			if (awsSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
				Element aws = (Element) doc.getElementsByTagName("aws").item(0);
				Main.cloudChunkStore = Boolean.parseBoolean(aws
						.getAttribute("enabled"));
				Main.cloudAccessKey = aws.getAttribute("aws-access-key");
				Main.cloudSecretKey = aws.getAttribute("aws-secret-key");
				Main.cloudBucket = aws.getAttribute("aws-bucket-name");
				Main.cloudCompress = Boolean.parseBoolean(aws
						.getAttribute("compress"));
			}
			int azureSz = doc.getElementsByTagName("azure-store").getLength();
			if (azureSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.MAzureChunkStore";
				Element azure = (Element) doc.getElementsByTagName("azure")
						.item(0);
				Main.cloudChunkStore = Boolean.parseBoolean(azure
						.getAttribute("enabled"));
				Main.cloudAccessKey = azure.getAttribute("azure-access-key");
				Main.cloudSecretKey = azure.getAttribute("azure-secret-key");
				Main.cloudBucket = azure.getAttribute("azure-bucket-name");
				Main.cloudCompress = Boolean.parseBoolean(azure
						.getAttribute("compress"));
			}
			File f = new File(Main.chunkStore);
			if (!f.exists()) {
				SDFSLogger.getLog().info(
						"creating chunk store at " + Main.chunkStore);
				f.mkdirs();
			}

			f = new File(Main.hashDBStore);
			if (!f.exists()) {
				SDFSLogger.getLog().info(
						"creating hash database store at " + Main.chunkStore);
				f.mkdirs();
			}

		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to parse config file [" + fileName + "]", e);
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
		SDFSLogger.getLog().info("Running SDFS Version " + Main.version);
		if (doc.getDocumentElement().hasAttribute("version")) {
			version = doc.getDocumentElement().getAttribute("version");
			Main.version = version;
		}

		Main.version = version;
		SDFSLogger.getLog().info(
				"Parsing volume " + doc.getDocumentElement().getNodeName()
						+ " version " + version);
		Element locations = (Element) doc.getElementsByTagName("locations")
				.item(0);
		SDFSLogger.getLog().info("parsing folder locations");
		Main.dedupDBStore = locations.getAttribute("dedup-db-store");
		Main.ioLogFile = locations.getAttribute("io-log.log");
		Element cache = (Element) doc.getElementsByTagName("io").item(0);
		if (cache.hasAttribute("log-level")) {
			SDFSLogger.setLevel(Integer.parseInt(cache
					.getAttribute("log-level")));
		}
		// Close files when close cmd is executed. This should be set to false
		// if running over nfs
		Main.safeClose = Boolean.parseBoolean(cache.getAttribute("safe-close"));
		// Makes sure writes are sync'd when set to true.
		Main.safeSync = Boolean.parseBoolean(cache.getAttribute("safe-sync"));
		Main.writeThreads = Integer.parseInt(cache
				.getAttribute("write-threads"));
		if (cache.hasAttribute("hash-size")) {
			short hsz = Short.parseShort(cache.getAttribute("hash-size"));
			if (hsz == 16)
				Main.hashType = HashFunctionPool.TIGER_16;
			if (hsz == 24)
				Main.hashType = HashFunctionPool.TIGER_24;
			SDFSLogger.getLog().info("Setting hash engine to " + Main.hashType);
		}
		if (cache.hasAttribute("hash-type")) {
			Main.hashType = cache.getAttribute("hash-type");
			SDFSLogger.getLog().info("Setting hash engine to " + Main.hashType);
		}
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
		Main.fDkiskSchedule = cache.getAttribute("claim-hash-schedule");
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

		SDFSLogger.getLog().debug("parsing local chunkstore parameters");
		Element localChunkStore = (Element) doc.getElementsByTagName(
				"local-chunkstore").item(0);
		Main.chunkStoreLocal = Boolean.parseBoolean(localChunkStore
				.getAttribute("enabled"));
		
		if (Main.chunkStoreLocal) {
			SDFSLogger.getLog().debug("this is a local chunkstore");
			Main.chunkStore = localChunkStore.getAttribute("chunk-store");
			if (localChunkStore.hasAttribute("gc-name"))
				Main.gcClass = localChunkStore.getAttribute("gc-name");
			// Main.chunkStoreMetaData =
			// localChunkStore.getAttribute("chunk-store-metadata");
			Main.chunkStoreAllocationSize = Long.parseLong(localChunkStore
					.getAttribute("allocation-size"));
			Main.gcChunksSchedule = localChunkStore
					.getAttribute("chunk-gc-schedule");
			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.FileChunkStore";
			if (localChunkStore.hasAttribute("chunkstore-class"))
				Main.chunkStoreClass = localChunkStore
						.getAttribute("chunkstore-class");
			if (localChunkStore.hasAttribute("hashdb-class"))
				Main.hashesDBClass = localChunkStore
						.getAttribute("hashdb-class");
			if (localChunkStore.getElementsByTagName("extended-config")
					.getLength() > 0) {
				Main.chunkStoreConfig = (Element) localChunkStore
						.getElementsByTagName("extended-config").item(0);
			}
			if (localChunkStore.hasAttribute("max-repl-batch-sz"))
				Main.MAX_REPL_BATCH_SZ = Integer.parseInt(localChunkStore
						.getAttribute("max-repl-batch-sz"));

			Main.evictionAge = Integer.parseInt(localChunkStore
					.getAttribute("eviction-age"));
			if (localChunkStore.hasAttribute("encrypt")) {
				Main.chunkStoreEncryptionEnabled = Boolean
						.parseBoolean("encrypt");
				Main.chunkStoreEncryptionKey = localChunkStore
						.getAttribute("encryption-key");
			}
			Main.hashDBStore = localChunkStore.getAttribute("hash-db-store");
			Main.preAllocateChunkStore = Boolean.parseBoolean(localChunkStore
					.getAttribute("pre-allocate"));
			Main.chunkStoreReadAheadPages = Integer.parseInt(localChunkStore
					.getAttribute("read-ahead-pages"));
			Element networkcs = (Element) doc.getElementsByTagName("network")
					.item(0);
			if (networkcs != null) {
				Main.enableNetworkChunkStore = Boolean.parseBoolean(networkcs
						.getAttribute("enable"));
				Main.serverHostName = networkcs.getAttribute("hostname");
				Main.useUDP = Boolean.parseBoolean(networkcs
						.getAttribute("use-udp"));
				Main.serverPort = Integer.parseInt(networkcs
						.getAttribute("port"));
				if (networkcs.hasAttribute("upstream-enabled")) {
					Main.upStreamDSEHostEnabled = Boolean
							.parseBoolean(networkcs
									.getAttribute("upstream-enabled"));
					Main.upStreamDSEHostName = networkcs
							.getAttribute("upstream-host");
					Main.upStreamDSEPort = Integer.parseInt(networkcs
							.getAttribute("upstream-host-port"));
					Main.upStreamPassword = networkcs
							.getAttribute("upstream-password");
				}
			}
			if(networkcs.hasAttribute("use-ssl"))
				Main.serverUseSSL = Boolean.parseBoolean(networkcs.getAttribute("use-ssl"));
			SDFSLogger.getLog().info(
					"######### Will allocate " + Main.chunkStoreAllocationSize
							+ " in chunkstore ##############");
			int awsSz = localChunkStore.getElementsByTagName("aws").getLength();
			if (awsSz > 0) {
				
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
				Element aws = (Element) localChunkStore.getElementsByTagName(
						"aws").item(0);
				Main.cloudChunkStore = Boolean.parseBoolean(aws
						.getAttribute("enabled"));
				Main.cloudAccessKey = aws.getAttribute("aws-access-key");
				Main.cloudSecretKey = aws.getAttribute("aws-secret-key");
				Main.cloudBucket = aws.getAttribute("aws-bucket-name");
				Main.cloudCompress = Boolean.parseBoolean(aws
						.getAttribute("compress"));
			}
			int azureSz = doc.getElementsByTagName("azure-store").getLength();
			if (azureSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.MAzureChunkStore";
				Element azure = (Element) doc.getElementsByTagName("azure-store")
						.item(0);
				Main.cloudAccessKey = azure.getAttribute("azure-access-key");
				Main.cloudSecretKey = azure.getAttribute("azure-secret-key");
				Main.cloudBucket = azure.getAttribute("azure-bucket-name");
				Main.cloudCompress = Boolean.parseBoolean(azure
						.getAttribute("compress"));
				Main.cloudChunkStore = Boolean.parseBoolean(azure
						.getAttribute("enabled"));
			}
			int cliSz = doc.getElementsByTagName("sdfscli").getLength();
			if (cliSz > 0) {
				Element cli = (Element) doc.getElementsByTagName("sdfscli")
						.item(0);
				Main.sdfsCliEnabled = Boolean.parseBoolean(cli
						.getAttribute("enable"));
				Main.sdfsCliPassword = cli.getAttribute("password");
				Main.sdfsCliSalt = cli.getAttribute("salt");
				Main.sdfsCliPort = Integer.parseInt(cli.getAttribute("port"));
				Main.sdfsCliRequireAuth = Boolean.parseBoolean(cli
						.getAttribute("enable-auth"));
				Main.sdfsCliListenAddr = cli.getAttribute("listen-address");
			}

		}

		/*
		 * IOMeter meter = new IOMeter(Main.ioLogFile); Thread th = new
		 * Thread(meter); th.start();
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
		Element root = doc.getDocumentElement();
		doc.getDocumentElement().normalize();
		Element volume = (Element) root.getElementsByTagName("volume").item(0);
		root.removeChild(volume);
		volume = Main.volume.toXMLElement(doc);
		root.appendChild(volume);
		int cliSz = doc.getElementsByTagName("sdfscli").getLength();
		if (cliSz > 0) {
			Element cli = (Element) doc.getElementsByTagName("sdfscli").item(0);
			cli.setAttribute("enable", Boolean.toString(Main.sdfsCliEnabled));
			cli.setAttribute("password", Main.sdfsCliPassword);
			cli.setAttribute("salt", Main.sdfsCliSalt);
			cli.setAttribute("port", Integer.toString(Main.sdfsCliPort));
			cli.setAttribute("enable-auth",
					Boolean.toString(Main.sdfsCliRequireAuth));
			cli.setAttribute("listen-address", Main.sdfsCliListenAddr);
		}
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to write volume config " + fileName, e);
		}
		SDFSLogger.getLog().debug("Wrote volume config = " + fileName);
	}

	public static synchronized void parserRoutingFile(String fileName)
			throws IOException {
		if (Main.chunkStoreLocal)
			return;
		File file = new File(fileName);
		SDFSLogger.getLog().info("Parsing routing config " + fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			SDFSLogger.getLog().fatal(
					"unable to parse config file [" + fileName + "]", e1);
			throw new IOException(e1);
		}
		Document doc = null;
		try {
			doc = db.parse(file);
		} catch (SAXException e1) {
			SDFSLogger.getLog().fatal(
					"unable to parse config file [" + fileName + "]", e1);
			throw new IOException(e1);
		}
		doc.getDocumentElement().normalize();
		SDFSLogger.getLog().info(
				"Parsing " + doc.getDocumentElement().getNodeName());
		Element servers = (Element) doc.getElementsByTagName("servers").item(0);

		SDFSLogger.getLog().info("parsing Servers");
		NodeList server = servers.getElementsByTagName("server");
		for (int s = 0; s < server.getLength(); s++) {
			SDFSLogger.getLog().info(
					"Connection to  Servers [" + server.getLength() + "]");
			Element _server = (Element) server.item(s);
			HCServer hcs = new HCServer(_server.getAttribute("host").trim(),
					Integer.parseInt(_server.getAttribute("port").trim()),
					Boolean.parseBoolean(_server.getAttribute("use-udp")),
					Boolean.parseBoolean(_server.getAttribute("compress")));
			try {
				HCServiceProxy.dseServers.put(
						_server.getAttribute("name").trim(),
						new HashClientPool(hcs, _server.getAttribute("name")
								.trim(), Integer.parseInt(_server
								.getAttribute("network-threads"))));

			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"unable to connect to server "
								+ _server.getAttribute("name").trim(), e);
				throw new IOException("unable to connect to server");
			}
			SDFSLogger.getLog().info(
					"Added Server " + _server.getAttribute("name"));
		}
		Element _c = (Element) doc.getElementsByTagName("chunks").item(0);
		NodeList chunks = _c.getElementsByTagName("chunk");
		for (int s = 0; s < chunks.getLength(); s++) {
			Element chunk = (Element) chunks.item(s);
			HCServiceProxy.dseRoutes.put(chunk.getAttribute("name").trim(),
					HCServiceProxy.dseServers.get(chunk.getAttribute("server")
							.trim()));
		}
	}

	public static synchronized void parserLaunchConfig(String fileName)
			throws IOException {
		File file = new File(fileName);
		SDFSLogger.getLog().info("Parsing launch  config " + fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			SDFSLogger.getLog().fatal(
					"unable to parse config file [" + fileName + "]", e1);
			throw new IOException(e1);
		}
		Document doc = null;
		try {
			doc = db.parse(file);
		} catch (SAXException e1) {
			SDFSLogger.getLog().fatal(
					"unable to parse config file [" + fileName + "]", e1);
			throw new IOException(e1);
		}
		doc.getDocumentElement().normalize();
		Element launchParams = (Element) doc.getElementsByTagName(
				"launch-params").item(0);
		Main.classPath = launchParams.getAttribute("class-path");
		SDFSLogger.getLog().info("SDFS Classpath=" + Main.javaPath);
		Main.javaOptions = launchParams.getAttribute("java-options");
		SDFSLogger.getLog().info("SDFS Java options=" + Main.javaOptions);
		Main.javaPath = launchParams.getAttribute("java-path");
		SDFSLogger.getLog().info("SDFS java path=" + Main.javaPath);
	}

}
