package org.opendedup.sdfs;

import java.io.File;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.Volume;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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
			Main.enableNetworkChunkStore = true;
			if (network.hasAttribute("use-ssl"))
				Main.serverUseSSL = Boolean.parseBoolean(network
						.getAttribute("use-ssl"));
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
			Main.chunkStoreAllocationSize = Long.parseLong(cbe
					.getAttribute("allocation-size"));
			if(cbe.hasAttribute("average-chunk-size")) {
				HashFunctionPool.avg_page_size = Integer.parseInt(cbe.getAttribute("average-chunk-size"));
			}
			Main.chunkStorePageSize = Integer.parseInt(cbe
					.getAttribute("page-size"));
			Main.CHUNK_LENGTH = Main.chunkStorePageSize;
			if (cbe.hasAttribute("gc-class"))
				Main.gcClass = cbe.getAttribute("gc-class");
			Main.fDkiskSchedule = cbe.getAttribute("claim-hash-schedule");
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
			if (cbe.hasAttribute("encrypt")) {
				Main.chunkStoreEncryptionEnabled = Boolean.parseBoolean(cbe
						.getAttribute("encrypt"));
				Main.chunkStoreEncryptionKey = cbe
						.getAttribute("encryption-key");
			}
			if(cbe.hasAttribute("encryption-iv"))
				Main.chunkStoreEncryptionIV = cbe.getAttribute("encryption-iv");
			if (cbe.hasAttribute("compress")) {
				Main.compress = Boolean.parseBoolean(cbe
						.getAttribute("compress"));
			}
			if (cbe.hasAttribute("max-repl-batch-sz"))
				Main.MAX_REPL_BATCH_SZ = Integer.parseInt(cbe
						.getAttribute("max-repl-batch-sz"));
			int awsSz = doc.getElementsByTagName("aws").getLength();
			if (cbe.hasAttribute("cluster-id"))
				Main.DSEClusterID = cbe.getAttribute("cluster-id");
			if (cbe.hasAttribute("cluster-member-id"))
				Main.DSEClusterMemberID = Byte.parseByte(cbe
						.getAttribute("cluster-member-id"));
			if (cbe.hasAttribute("cluster-config"))
				Main.DSEClusterConfig = cbe.getAttribute("cluster-config");
			if (cbe.hasAttribute("sdfs-password")) {
				Main.sdfsPassword = cbe.getAttribute("dse-password");
				Main.sdfsPasswordSalt = cbe.getAttribute("dse-password-salt");
			}
			if (cbe.hasAttribute("cluster-node-rack")) {
				Main.DSEClusterNodeRack = cbe.getAttribute("cluster-node-rack");
			}
			if (cbe.hasAttribute("cluster-node-location")) {
				Main.DSEClusterNodeLocation = cbe
						.getAttribute("cluster-node-location");
			}
			if (cbe.hasAttribute("io-threads")) {
				Main.dseIOThreads = Integer.parseInt(cbe
						.getAttribute("io-threads"));
			}
			if (awsSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
				Element aws = (Element) doc.getElementsByTagName("aws").item(0);
				if(aws.hasAttribute("chunkstore-class"))
					Main.chunkStoreClass = aws.getAttribute("chunkstore-class");
				Main.cloudChunkStore = Boolean.parseBoolean(aws
						.getAttribute("enabled"));
				Main.cloudAccessKey = aws.getAttribute("aws-access-key");
				Main.cloudSecretKey = aws.getAttribute("aws-secret-key");
				Main.cloudBucket = aws.getAttribute("aws-bucket-name");
			}
			int azureSz = doc.getElementsByTagName("azure-store").getLength();
			if (azureSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.MAzureChunkStore";
				Element azure = (Element) doc.getElementsByTagName("azure")
						.item(0);
				if(azure.hasAttribute("chunkstore-class"))
					Main.chunkStoreClass = azure.getAttribute("chunkstore-class");
				Main.cloudChunkStore = Boolean.parseBoolean(azure
						.getAttribute("enabled"));
				Main.cloudAccessKey = azure.getAttribute("azure-access-key");
				Main.cloudSecretKey = azure.getAttribute("azure-secret-key");
				Main.cloudBucket = azure.getAttribute("azure-bucket-name");
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
				if (!f.mkdirs())
					throw new IOException("Unable to create " + f.getPath());
			}
			if(Main.chunkStoreEncryptionEnabled)
				SDFSLogger.getLog().info("################## Encryption is enabled ##################");
			else
				SDFSLogger.getLog().info("################## Encryption is NOT enabled ##################");
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
	public synchronized static void parseGCConfigFile(String fileName)
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
				"Parsing gc " + doc.getDocumentElement().getNodeName()
						+ " version " + version);
		Element gc = (Element) doc.getElementsByTagName("gc").item(0);
		if (gc.hasAttribute("log-level")) {
			SDFSLogger.setLevel(Integer.parseInt(gc.getAttribute("log-level")));
		}
		Main.fDkiskSchedule = gc.getAttribute("claim-hash-schedule");
		Main.DSEClusterConfig = gc.getAttribute("cluster-config");
		Main.DSEClusterID = gc.getAttribute("cluster-id");
		Main.DSEPassword = gc.getAttribute("cluster-dse-password");
		Main.DSEClusterVolumeList = gc.getAttribute("volume-list-file");
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
		Main.blankHash = new byte[Main.CHUNK_LENGTH];

		Main.maxWriteBuffers = Integer.parseInt(cache
				.getAttribute("max-file-write-buffers"));
		Main.maxOpenFiles = Integer.parseInt(cache
				.getAttribute("max-open-files"));
		Main.maxInactiveFileTime = Integer.parseInt(cache
				.getAttribute("max-file-inactive")) * 1000;
		Main.fDkiskSchedule = cache.getAttribute("claim-hash-schedule");

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
		if(localChunkStore.hasAttribute("average-chunk-size")) {
			HashFunctionPool.avg_page_size = Integer.parseInt(localChunkStore.getAttribute("average-chunk-size"));
		}
		if (localChunkStore.hasAttribute("cluster-id"))
			Main.DSEClusterID = localChunkStore.getAttribute("cluster-id");
		if (localChunkStore.hasAttribute("io-threads")) {
			Main.dseIOThreads = Integer.parseInt(localChunkStore
					.getAttribute("io-threads"));
		}
		if (localChunkStore.hasAttribute("cluster-config"))
			Main.DSEClusterConfig = localChunkStore
					.getAttribute("cluster-config");
		if (localChunkStore.hasAttribute("cluster-dse-password"))
			Main.DSEPassword = localChunkStore
					.getAttribute("cluster-dse-password");
		if (localChunkStore.hasAttribute("gc-class"))
			Main.gcClass = localChunkStore.getAttribute("gc-class");
		Element cli = (Element) doc.getElementsByTagName("sdfscli").item(0);
		Main.sdfsCliEnabled = Boolean.parseBoolean(cli.getAttribute("enable"));
		Main.sdfsPassword = cli.getAttribute("password");
		Main.sdfsPasswordSalt = cli.getAttribute("salt");
		Main.sdfsCliPort = Integer.parseInt(cli.getAttribute("port"));
		Main.sdfsCliRequireAuth = Boolean.parseBoolean(cli
				.getAttribute("enable-auth"));
		Main.sdfsCliListenAddr = cli.getAttribute("listen-address");
		SDFSLogger.getLog().debug("listen-address=" + Main.sdfsCliListenAddr);
		Element volume = (Element) doc.getElementsByTagName("volume").item(0);
		Main.volume = new Volume(volume, fileName);
		if (Main.chunkStoreLocal) {
			SDFSLogger.getLog().debug("this is a local chunkstore");
			Main.chunkStore = localChunkStore.getAttribute("chunk-store");
			// Main.chunkStoreMetaData =
			// localChunkStore.getAttribute("chunk-store-metadata");
			Main.chunkStoreAllocationSize = Long.parseLong(localChunkStore
					.getAttribute("allocation-size"));
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
			if (localChunkStore.hasAttribute("encrypt")) {
				Main.chunkStoreEncryptionEnabled = Boolean
						.parseBoolean(localChunkStore
								.getAttribute("encrypt"));
				Main.chunkStoreEncryptionKey = localChunkStore
						.getAttribute("encryption-key");
			}
			if(localChunkStore.hasAttribute("encryption-iv"))
				Main.chunkStoreEncryptionIV = localChunkStore.getAttribute("encryption-iv");
			Main.hashDBStore = localChunkStore.getAttribute("hash-db-store");
			Element networkcs = (Element) doc.getElementsByTagName("network")
					.item(0);
			if (localChunkStore.hasAttribute("compress")) {
				Main.compress = Boolean.parseBoolean(localChunkStore
						.getAttribute("compress"));
			}
			if (networkcs != null) {
				Main.enableNetworkChunkStore = Boolean.parseBoolean(networkcs
						.getAttribute("enable"));
				Main.serverHostName = networkcs.getAttribute("hostname");
				Main.serverPort = Integer.parseInt(networkcs
						.getAttribute("port"));
			}
			if (networkcs.hasAttribute("use-ssl"))
				Main.serverUseSSL = Boolean.parseBoolean(networkcs
						.getAttribute("use-ssl"));
			SDFSLogger.getLog().info(
					"######### Will allocate " + Main.chunkStoreAllocationSize
							+ " in chunkstore ##############");
			int awsSz = localChunkStore.getElementsByTagName("aws").getLength();
			if (awsSz > 0) {

				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
				Element aws = (Element) localChunkStore.getElementsByTagName(
						"aws").item(0);
				if(aws.hasAttribute("chunkstore-class"))
					Main.chunkStoreClass = aws.getAttribute("chunkstore-class");
				Main.cloudChunkStore = Boolean.parseBoolean(aws
						.getAttribute("enabled"));
				Main.cloudAccessKey = aws.getAttribute("aws-access-key");
				Main.cloudSecretKey = aws.getAttribute("aws-secret-key");
				Main.cloudBucket = aws.getAttribute("aws-bucket-name");
			}
			int azureSz = doc.getElementsByTagName("azure-store").getLength();
			if (azureSz > 0) {
				Main.chunkStoreClass = "org.opendedup.sdfs.filestore.MAzureChunkStore";
				Element azure = (Element) doc.getElementsByTagName(
						"azure-store").item(0);
				if(azure.hasAttribute("chunkstore-class"))
					Main.chunkStoreClass = azure.getAttribute("chunkstore-class");
				Main.cloudAccessKey = azure.getAttribute("azure-access-key");
				Main.cloudSecretKey = azure.getAttribute("azure-secret-key");
				Main.cloudBucket = azure.getAttribute("azure-bucket-name");
				Main.cloudChunkStore = Boolean.parseBoolean(azure
						.getAttribute("enabled"));
			}

		}
		if(Main.chunkStoreEncryptionEnabled)
			SDFSLogger.getLog().info("################## Encryption is enabled ##################");
		else
			SDFSLogger.getLog().info("################## Encryption is NOT enabled ##################");

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

		Element cli = (Element) doc.getElementsByTagName("sdfscli").item(0);
		cli.setAttribute("enable", Boolean.toString(Main.sdfsCliEnabled));
		cli.setAttribute("password", Main.sdfsPassword);
		cli.setAttribute("salt", Main.sdfsPasswordSalt);
		cli.setAttribute("port", Integer.toString(Main.sdfsCliPort));
		cli.setAttribute("enable-auth",
				Boolean.toString(Main.sdfsCliRequireAuth));
		cli.setAttribute("listen-address", Main.sdfsCliListenAddr);
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

}
