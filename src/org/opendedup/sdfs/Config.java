package org.opendedup.sdfs;

import java.io.File;

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
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.io.BaseEncoding;

public class Config {
	public static boolean encrypted = false;

	/**
	 * parse the client side config file
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static void parseGCConfigFile(String fileName) throws Exception {
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
		SDFSLogger.getLog().info("Parsing gc " + doc.getDocumentElement().getNodeName() + " version " + version);
		Element gc = (Element) doc.getElementsByTagName("gc").item(0);

		Main.fDkiskSchedule = gc.getAttribute("claim-hash-schedule");

	}

	/**
	 * parse the client side config file
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static void parseSDFSConfigFile(String fileName, String password) throws Exception {
		File file = new File(fileName);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		String version = "0.8.12";
		SDFSLogger.getLog().info("############ Running SDFS Version " + Main.version);
		if (doc.getDocumentElement().hasAttribute("version")) {
			version = doc.getDocumentElement().getAttribute("version");
			Main.version = version;
		}

		Element cli = (Element) doc.getElementsByTagName("sdfscli").item(0);
		Main.sdfsCliEnabled = Boolean.parseBoolean(cli.getAttribute("enable"));
		Main.sdfsPassword = cli.getAttribute("password");
		Main.sdfsPasswordSalt = cli.getAttribute("salt");
		Main.sdfsCliPort = Integer.parseInt(cli.getAttribute("port"));
		if (cli.hasAttribute("immutabilityPeriod")) {
			Main.immutabilityPeriod = Integer.parseInt(cli.getAttribute("immutabilityPeriod"));
		}
		if (cli.hasAttribute("use-ssl")) {
			Main.sdfsCliSSL = Boolean.parseBoolean(cli.getAttribute("use-ssl"));
		}
		if (cli.hasAttribute("permissions-file")) {
			Main.permissionsFile = cli.getAttribute("permissions-file");
			File f = new File(Main.permissionsFile);
			if (!f.exists()) {
				f.getParentFile().mkdirs();

			}

		}
		Main.sdfsCliRequireAuth = Boolean.parseBoolean(cli.getAttribute("enable-auth"));
		Main.sdfsCliRequireMutualTLSAuth = Boolean.parseBoolean(cli.getAttribute("enable-mutual-tls-auth"));
		Main.sdfsCliListenAddr = cli.getAttribute("listen-address");
		Main.authJarFilePath = cli.getAttribute("auth-utility-jar-file-path");
		Main.authClassInfo = cli.getAttribute("auth-class-info");
		Main.prodConfigFilePath = cli.getAttribute("prod-config-file-path");
		SDFSLogger.getLog().debug("listen-address=" + Main.sdfsCliListenAddr);
		SDFSLogger.getLog().debug("auth-utility-jar-file-path=" + Main.authJarFilePath);
		SDFSLogger.getLog().debug("auth-class-info=" + Main.authClassInfo);
		SDFSLogger.getLog().debug("prod-config-file-path=" + Main.prodConfigFilePath);
		Main.version = version;
		SDFSLogger.getLog().info("Parsing volume " + doc.getDocumentElement().getNodeName() + " version " + version);
		Element locations = (Element) doc.getElementsByTagName("locations").item(0);
		SDFSLogger.getLog().info("parsing folder locations");
		Main.dedupDBStore = locations.getAttribute("dedup-db-store");
		Main.dedupDBTrashStore = locations.getAttribute("dedup-dbtrash-store");
		Main.ioLogFile = locations.getAttribute("io-log.log");
		Element cache = (Element) doc.getElementsByTagName("io").item(0);
		if (cache.hasAttribute("encrypt-config")) {

		}

		// Close files when close cmd is executed. This should be set to false
		// if running over nfs
		if (cache.hasAttribute("log-level")) {
			SDFSLogger.setLevel(Integer.parseInt(cache.getAttribute("log-level")));
		}
		if (cache.hasAttribute("log-size")) {
			Main.logSize = cache.getAttribute("log-size");
			SDFSLogger.setLogSize(cache.getAttribute("log-size"));
		}
		if (cache.hasAttribute("log-index-size")) {
			Main.logFiles = Integer.parseInt(cache.getAttribute("log-index-size"));
		}
		if (cache.hasAttribute("read-ahead")) {
			Main.readAhead = Boolean.parseBoolean(cache.getAttribute("read-ahead"));
		}
		if (cache.hasAttribute("read-ahead-threads")) {
			Main.readAheadThreads = Integer.parseInt(cache.getAttribute("read-ahead-threads"));
		}
		Main.safeClose = Boolean.parseBoolean(cache.getAttribute("safe-close"));
		// Makes sure writes are sync'd when set to true.
		Main.safeSync = Boolean.parseBoolean(cache.getAttribute("safe-sync"));
		Main.writeThreads = Integer.parseInt(cache.getAttribute("write-threads"));
		if (cache.hasAttribute("max-open-sst-files")) {
			Main.MAX_OPEN_SST_FILES = Integer.parseInt(cache.getAttribute("max-open-sst-files"));
		}
		if (cache.hasAttribute("min-variable-segment-size")) {

			Main.MIN_CHUNK_LENGTH = (Integer.parseInt(cache.getAttribute("min-variable-segment-size")) * 1024) - 1;

		}
		if (cache.hasAttribute("hash-type")) {
			Main.hashType = cache.getAttribute("hash-type");
			SDFSLogger.getLog().info("Setting hash engine to " + Main.hashType);
		}
		if (cache.hasAttribute("hash-seed")) {
			Main.hashSeed = Integer.parseInt(cache.getAttribute("hash-seed"));
		}
		Main.dedupFiles = Boolean.parseBoolean(cache.getAttribute("dedup-files"));
		Main.CHUNK_LENGTH = Integer.parseInt(cache.getAttribute("chunk-size")) * 1024;

		if (cache.hasAttribute("variable-window-size"))
			HashFunctionPool.bytesPerWindow = Integer.parseInt(cache.getAttribute("variable-window-size"));
		if (cache.hasAttribute("max-variable-segment-size")) {
			HashFunctionPool.maxLen = Integer.parseInt(cache.getAttribute("max-variable-segment-size")) * 1024;

		} else {
			HashFunctionPool.maxLen = Main.CHUNK_LENGTH;
		}

		Main.blankHash = new byte[Main.CHUNK_LENGTH];

		if (cache.hasAttribute("replication-threads"))
			Main.REPLICATION_THREADS = Integer.parseInt(cache.getAttribute("replication-threads"));

		Main.maxWriteBuffers = Integer.parseInt(cache.getAttribute("max-file-write-buffers"));
		if (Main.maxWriteBuffers > 80)
			Main.maxWriteBuffers = 80;
		Main.maxOpenFiles = Integer.parseInt(cache.getAttribute("max-open-files"));
		Main.maxInactiveFileTime = Integer.parseInt(cache.getAttribute("max-file-inactive")) * 1000;
		Main.fDkiskSchedule = cache.getAttribute("claim-hash-schedule");

		Element permissions = (Element) doc.getElementsByTagName("permissions").item(0);
		Main.defaultGroup = Integer.parseInt(permissions.getAttribute("default-group"));
		Main.defaultOwner = Integer.parseInt(permissions.getAttribute("default-owner"));
		Main.defaultFilePermissions = Integer.parseInt(permissions.getAttribute("default-file"));
		Main.defaultDirPermissions = Integer.parseInt(permissions.getAttribute("default-folder"));
		Main.chunkStorePageSize = Main.CHUNK_LENGTH;

		SDFSLogger.getLog().debug("parsing local chunkstore parameters");
		Element localChunkStore = (Element) doc.getElementsByTagName("local-chunkstore").item(0);
		if (localChunkStore.hasAttribute("fpp")) {
			Main.fpp = Double.parseDouble(localChunkStore.getAttribute("fpp"));
		}

		if (localChunkStore.hasAttribute("average-chunk-size")) {
			HashFunctionPool.avg_page_size = Integer.parseInt(localChunkStore.getAttribute("average-chunk-size"));
		}
		if (localChunkStore.hasAttribute("disable-auto-gc")) {
			Main.disableAutoGC = Boolean.parseBoolean(localChunkStore.getAttribute("disable-auto-gc"));
		}
		if (localChunkStore.hasAttribute("compact-on-mount")) {
			Main.runCompact = Boolean.parseBoolean(localChunkStore.getAttribute("compact-on-mount"));
		}
		if (localChunkStore.hasAttribute("hashtable-rm-threshold")) {
			Main.HT_RM_THRESH = Long.parseLong(localChunkStore.getAttribute("hashtable-rm-threshold"));
			SDFSLogger.getLog().info("HT_RM_THRESH = " + Main.HT_RM_THRESH);
		}

		if (localChunkStore.hasAttribute("max-chunk-age")) {
			Main.maxAge = Long.parseLong(localChunkStore.getAttribute("max-chunk-age"));
		}
		if (localChunkStore.hasAttribute("io-threads")) {
			Main.dseIOThreads = Integer.parseInt(localChunkStore.getAttribute("io-threads"));
		}
		if (localChunkStore.hasAttribute("enable-lookup-filter")) {
			Main.enableLookupFilter = Boolean.parseBoolean(localChunkStore.getAttribute("enable-lookup-filter"));
		}
		if (localChunkStore.hasAttribute("enable-batch-gc")) {
			Main.DDB_TRASH_ENABLED = Boolean.parseBoolean(localChunkStore.getAttribute("enable-batch-gc"));
		}

		if (localChunkStore.hasAttribute("gc-class"))
			Main.gcClass = localChunkStore.getAttribute("gc-class");
		Element volume = (Element) doc.getElementsByTagName("volume").item(0);
		Main.volume = new Volume(volume, fileName);
		SDFSLogger.getLog().debug("this is a local chunkstore");
		Main.chunkStore = localChunkStore.getAttribute("chunk-store");
		// Main.chunkStoreMetaData =
		// localChunkStore.getAttribute("chunk-store-metadata");
		Main.chunkStoreAllocationSize = Long.parseLong(localChunkStore.getAttribute("allocation-size"));
		Main.chunkStoreClass = "org.opendedup.sdfs.filestore.FileChunkStore";
		if (localChunkStore.hasAttribute("chunkstore-class"))
			Main.chunkStoreClass = localChunkStore.getAttribute("chunkstore-class");
		if (localChunkStore.hasAttribute("parallel-db-count"))
			Main.parallelDBCount = Integer.parseInt(localChunkStore.getAttribute("parallel-db-count"));
		if (localChunkStore.hasAttribute("hashdb-class"))
			Main.hashesDBClass = localChunkStore.getAttribute("hashdb-class");
		if (localChunkStore.getElementsByTagName("extended-config").getLength() > 0) {
			Main.chunkStoreConfig = (Element) localChunkStore.getElementsByTagName("extended-config").item(0);
		}
		if (localChunkStore.hasAttribute("max-repl-batch-sz"))
			Main.MAX_REPL_BATCH_SZ = Integer.parseInt(localChunkStore.getAttribute("max-repl-batch-sz"));
		if (localChunkStore.hasAttribute("encrypt")) {
			Main.chunkStoreEncryptionEnabled = Boolean.parseBoolean(localChunkStore.getAttribute("encrypt"));
			Main.chunkStoreEncryptionKey = localChunkStore.getAttribute("encryption-key");
		}
		if (localChunkStore.hasAttribute("encryption-iv"))
			Main.chunkStoreEncryptionIV = localChunkStore.getAttribute("encryption-iv");
		Main.hashDBStore = localChunkStore.getAttribute("hash-db-store");

		if (localChunkStore.hasAttribute("compress")) {
			Main.compress = Boolean.parseBoolean(localChunkStore.getAttribute("compress"));
		}
		SDFSLogger.getLog()
				.info("######### Will allocate " + Main.chunkStoreAllocationSize + " in chunkstore ##############");
		int awsSz = localChunkStore.getElementsByTagName("aws").getLength();
		int googleSz = doc.getElementsByTagName("google-store").getLength();
		int fileSz = doc.getElementsByTagName("file-store").getLength();
		if (fileSz > 0) {
			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.GoogleChunkStore";
			Element aws = (Element) doc.getElementsByTagName("file-store").item(0);
			if (aws.hasAttribute("chunkstore-class"))
				Main.chunkStoreClass = aws.getAttribute("chunkstore-class");
			Main.cloudChunkStore = Boolean.parseBoolean(aws.getAttribute("enabled"));
			Main.cloudBucket = aws.getAttribute("bucket-name");
			Main.cloudAccessKey = aws.getAttribute("access-key");
			Main.cloudSecretKey = aws.getAttribute("secret-key");
		}
		if (googleSz > 0) {
			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
			Element aws = (Element) doc.getElementsByTagName("google-store").item(0);
			if (aws.hasAttribute("chunkstore-class"))
				Main.chunkStoreClass = aws.getAttribute("chunkstore-class");
			Main.cloudChunkStore = Boolean.parseBoolean(aws.getAttribute("enabled"));
			Main.cloudAccessKey = aws.getAttribute("gs-access-key");
			Main.cloudSecretKey = aws.getAttribute("gs-secret-key");
			Main.cloudBucket = aws.getAttribute("gs-bucket-name");
		}
		if (awsSz > 0) {

			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.S3ChunkStore";
			Element aws = (Element) localChunkStore.getElementsByTagName("aws").item(0);
			if (aws.hasAttribute("chunkstore-class"))
				Main.chunkStoreClass = aws.getAttribute("chunkstore-class");
			Main.cloudChunkStore = Boolean.parseBoolean(aws.getAttribute("enabled"));
			if (aws.hasAttribute("aws-aim")) {
				Main.useAim = Boolean.parseBoolean(aws.getAttribute("aws-aim"));
			}
			if (!Main.useAim) {
				Main.cloudAccessKey = aws.getAttribute("aws-access-key");
				Main.cloudSecretKey = aws.getAttribute("aws-secret-key");
			}
			Main.cloudBucket = aws.getAttribute("aws-bucket-name");
			Main.s3ApiCompatible = Boolean.parseBoolean(aws.getAttribute("s3-compatible-target"));
			if (aws.hasAttribute("encrypt-bucket")) {
				Main.encryptBucket = Boolean.parseBoolean(aws.getAttribute("encrypt-bucket"));
			}
		}
		int azureSz = doc.getElementsByTagName("azure-store").getLength();
		if (azureSz > 0) {
			Main.chunkStoreClass = "org.opendedup.sdfs.filestore.MAzureChunkStore";
			Element azure = (Element) doc.getElementsByTagName("azure-store").item(0);
			if (azure.hasAttribute("chunkstore-class"))
				Main.chunkStoreClass = azure.getAttribute("chunkstore-class");
			Main.cloudAccessKey = azure.getAttribute("azure-access-key");
			Main.cloudSecretKey = azure.getAttribute("azure-secret-key");
			Main.cloudBucket = azure.getAttribute("azure-bucket-name");
			Main.cloudChunkStore = Boolean.parseBoolean(azure.getAttribute("enabled"));
		}

		if (password != null) {
			Main.eSdfsPassword = Main.sdfsPassword;
			Main.eSdfsPasswordSalt = Main.sdfsPasswordSalt;
			try {
				byte[] dc = EncryptUtils.decryptCBC(BaseEncoding.base64Url().decode(Main.eSdfsPassword),
						password, Main.chunkStoreEncryptionIV);
				Main.sdfsPassword = new String(dc);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to decrypt sdfscli password ", e);
			}
			try {
				byte[] dc = EncryptUtils.decryptCBC(BaseEncoding.base64Url().decode(Main.eSdfsPasswordSalt),
						password, Main.chunkStoreEncryptionIV);
				Main.sdfsPasswordSalt = new String(dc);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to decrypt sdfscli password ", e);
			}

			if (Main.cloudSecretKey != null) {
				Main.eCloudSecretKey = Main.cloudSecretKey;

				byte[] dc = EncryptUtils.decryptCBC(BaseEncoding.base64Url().decode(Main.cloudSecretKey), password,
						Main.chunkStoreEncryptionIV);
				Main.cloudSecretKey = new String(dc);
			}
			if (Main.chunkStoreEncryptionKey != null) {
				String oc = Main.chunkStoreEncryptionKey;
				Main.eChunkStoreEncryptionKey = Main.chunkStoreEncryptionKey;
				try {
					byte[] dc = EncryptUtils.decryptCBC(BaseEncoding.base64Url().decode(Main.chunkStoreEncryptionKey),
							password, Main.chunkStoreEncryptionIV);
					Main.chunkStoreEncryptionKey = new String(dc);
				} catch (Exception e) {
					Main.chunkStoreEncryptionKey = oc;
					Main.eChunkStoreEncryptionKey = null;
					SDFSLogger.getLog().warn("unable to decrypt encrytion key " + oc, e);
					System.err.println("Unable to decrypt encryption key");
					System.exit(1);
				}

			}

		}
		if (Main.usePortRedirector) {
			Main.sdfsCliRequireMutualTLSAuth = false;
			Main.sdfsCliSSL = false;
			Main.sdfsCliListenAddr = "localhost";
		}
		if (Main.useDedicatedPort) {
			Main.sdfsCliRequireMutualTLSAuth = true;
			Main.sdfsCliSSL = true;
			Main.sdfsCliListenAddr = "0.0.0.0";
		}

		if (Main.chunkStoreEncryptionEnabled)
			SDFSLogger.getLog().info("################## Encryption is enabled ##################");
		else
			SDFSLogger.getLog().info("################## Encryption is NOT enabled ##################");

		/*
		 * IOMeter meter = new IOMeter(Main.ioLogFile); Thread th = new Thread(meter);
		 * th.start();
		 */
	}

	/**
	 * write the client side config file
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static void writeSDFSConfigFile(String fileName) throws Exception {
		File file = new File(fileName);
		Document doc = getSDFSConfigFile(fileName, false);
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance().newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to write volume config " + fileName, e);
		}
		SDFSLogger.getLog().debug("Wrote volume config = " + fileName);
	}

	/**
	 * write the client side config file
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public synchronized static Document getSDFSConfigFile(String fileName, boolean decrypt) throws Exception {
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
		if (Main.eSdfsPassword != null) {
			cli.setAttribute("password", Main.eSdfsPassword);
		} else {
			cli.setAttribute("password", Main.sdfsPassword);
		}
		if (Main.eSdfsPasswordSalt != null) {
			cli.setAttribute("salt", Main.eSdfsPasswordSalt);
		} else {
			cli.setAttribute("salt", Main.sdfsPasswordSalt);
		}

		cli.setAttribute("port", Integer.toString(Main.sdfsCliPort));
		cli.setAttribute("immutabilityPeriod", Integer.toString(Main.immutabilityPeriod));
		cli.setAttribute("enable-auth", Boolean.toString(Main.sdfsCliRequireAuth));
		cli.setAttribute("listen-address", Main.sdfsCliListenAddr);
		cli.setAttribute("auth-utility-jar-file-path", Main.authJarFilePath);
		cli.setAttribute("auth-class-info", Main.authClassInfo);
		cli.setAttribute("prod-config-file-path", Main.prodConfigFilePath);
		cli.setAttribute("use-ssl", Boolean.toString(Main.sdfsCliSSL));
		cli.setAttribute("enable-mutual-tls-auth", Boolean.toString(Main.sdfsCliRequireMutualTLSAuth));

		Element localChunkStore = (Element) doc.getElementsByTagName("local-chunkstore").item(0);
		if (localChunkStore.getElementsByTagName("extended-config").getLength() > 0) {
			Element chunkStoreConfig = (Element) localChunkStore.getElementsByTagName("extended-config").item(0);
			chunkStoreConfig.setAttribute("local-cache-size",
					StorageUnit.of(HCServiceProxy.getMaxCacheSize()).format(HCServiceProxy.getMaxCacheSize()));
			chunkStoreConfig.setAttribute("read-speed", Integer.toString(HCServiceProxy.getReadSpeed()));
			chunkStoreConfig.setAttribute("write-speed", Integer.toString(HCServiceProxy.getWriteSpeed()));
		}

		localChunkStore.setAttribute("allocation-size", Long.toString(Main.volume.capacity));

		if (Main.eChunkStoreEncryptionKey != null) {
			localChunkStore.setAttribute("encryption-key", Main.eChunkStoreEncryptionKey);
		}
		if (Main.eCloudSecretKey != null) {
			int awsSz = localChunkStore.getElementsByTagName("aws").getLength();
			int googleSz = localChunkStore.getElementsByTagName("google-store").getLength();
			int fileSz = localChunkStore.getElementsByTagName("file-store").getLength();
			int azureSz = doc.getElementsByTagName("azure-store").getLength();
			if (fileSz > 0) {
				Element aws = (Element) doc.getElementsByTagName("file-store").item(0);
				aws.setAttribute("secret-key", Main.eCloudSecretKey);
			}
			if (googleSz > 0) {
				Element aws = (Element) doc.getElementsByTagName("google-store").item(0);
				aws.setAttribute("gs-secret-key", Main.eCloudSecretKey);
			}
			if (awsSz > 0) {
				Element aws = (Element) localChunkStore.getElementsByTagName("aws").item(0);
				aws.setAttribute("aws-secret-key", Main.eCloudSecretKey);
				aws.setAttribute("s3-compatible-target", Boolean.toString(Main.s3ApiCompatible));
			}
			if (azureSz > 0) {
				Element azure = (Element) doc.getElementsByTagName("azure-store").item(0);
				azure.setAttribute("azure-secret-key", Main.eCloudSecretKey);
			}
		}
		return doc;
	}


/**
 * updates the config with new credentials
 *
 * @param fileName
 * @throws Exception
 */
public synchronized static int updateCloudCreds() throws Exception {

	File file = new File(OSValidator.getConfigPath() + Main.sdfsVolName.trim() + "-volume-cfg.xml");
	if (!file.exists()) {
		System.out.println("Volume doesn't exist, please provide a valid volume.");
		return -1;
	}
	
	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	DocumentBuilder db = dbf.newDocumentBuilder();
	Document doc = db.parse(file);

	Element localChunkStore = (Element) doc.getElementsByTagName("local-chunkstore").item(0);
	
	Element sdfsCli = (Element) doc.getElementsByTagName("sdfscli").item(0);
	if((sdfsCli.getAttribute("encrypt-config")).equals("true"))
	{
		if(Main.sdfsPassword.isEmpty())
		{
			System.out.println("--encrypt-config is required"
								+ " as the volume's cloud keys were encrypted during original volume creation.");
			return -1;
		}

		String password = Main.sdfsPassword;
		String iv = localChunkStore.getAttribute("encryption-iv");
		Main.chunkStoreEncryptionIV = iv;
		byte[] ec = EncryptUtils.encryptCBC(Main.cloudSecretKey.getBytes(), password, iv);
		Main.cloudSecretKey = BaseEncoding.base64Url().encode(ec);
	}

	int awsSz = localChunkStore.getElementsByTagName("aws").getLength();
	int googleSz = localChunkStore.getElementsByTagName("google-store").getLength();
	int azureSz = doc.getElementsByTagName("azure-store").getLength();

	if (awsSz > 0) {
		Element aws = (Element) doc.getElementsByTagName("aws").item(0);
		aws.setAttribute("aws-secret-key", Main.cloudSecretKey);
		aws.setAttribute("aws-access-key", Main.cloudAccessKey);
	}
	else if (azureSz > 0) {
		Element azure = (Element) doc.getElementsByTagName("azure-store").item(0);
		azure.setAttribute("azure-secret-key", Main.cloudSecretKey);
		azure.setAttribute("azure-access-key", Main.cloudAccessKey);
	}
	else if (googleSz > 0) {
		Element google = (Element) doc.getElementsByTagName("google-store").item(0);
		google.setAttribute("gs-secret-key", Main.cloudSecretKey);
		google.setAttribute("gs-access-key", Main.cloudAccessKey);
	}
	else {
		System.out.println("Not a valid cloud target");
		return -1;
	}

	try {
		// Prepare the DOM document for writing
		Source source = new DOMSource(doc);

		Result result = new StreamResult(file);

		// Write the DOM document to the file
		Transformer xformer = TransformerFactory.newInstance().newTransformer();
		xformer.transform(source, result);
	} catch (Exception e) {
		SDFSLogger.getLog().error("Unable to update the cloud credentials ", e);
		return -1;
	}
	
	return 1;
}

}
