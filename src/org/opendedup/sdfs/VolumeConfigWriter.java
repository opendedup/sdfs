package org.opendedup.sdfs;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.sdfs.filestore.S3ChunkStore;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class VolumeConfigWriter {
	/**
	 * write the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	String volume_name = null;
	String base_path = OSValidator.getProgramBasePath() + File.separator
			+ "volumes" + File.separator + volume_name;
	String dedup_db_store = base_path + File.separator + "ddb";
	String io_log = base_path + File.separator + "io.log";
	boolean safe_close = true;
	boolean safe_sync = false;
	int write_threads = (short) (Runtime.getRuntime().availableProcessors());
	boolean dedup_files = true;
	short chunk_size = 4;
	int max_file_write_buffers = 24;
	int max_open_files = 1024;
	int meta_file_cache = 1024;
	int write_timeout = Main.writeTimeoutSeconds;
	int read_timeout = Main.readTimeoutSeconds;
	String filePermissions = "0644";
	String dirPermissions = "0755";
	String owner = "0";
	String group = "0";
	String volume_capacity = null;
	String clusterDSEPassword = "admin";
	int avgPgSz = 8192;
	double max_percent_full = .95;
	boolean chunk_store_local = true;
	String chunk_store_data_location = null;
	String chunk_store_hashdb_location = null;
	long chunk_store_allocation_size = 0;
	// String chunk_gc_schedule = "0 0 0/4 * * ?";
	String fdisk_schedule = "0 59 23 * * ?";
	String volume_list_file = "/etc/sdfs/volume-list.xml";
	boolean azureEnabled = false;
	boolean awsEnabled = false;
	boolean gsEnabled = false;
	String cloudAccessKey = "";
	String cloudSecretKey = "";
	String cloudBucketName = "";
	int clusterRSPTimeout = 4000;
	int cloudThreads = 8;
	boolean compress = Main.compress;
	// int chunk_store_read_cache = Main.chunkStorePageCache;
	// int chunk_store_dirty_timeout = Main.chunkStoreDirtyCacheTimeout;
	String chunk_store_encryption_key = PassPhrase.getNext();
	String chunk_store_iv = PassPhrase.getIV();
	boolean chunk_store_encrypt = false;

	String hashType = HashFunctionPool.MURMUR3_16;
	String chunk_store_class = "org.opendedup.sdfs.filestore.FileChunkStore";
	String gc_class = "org.opendedup.sdfs.filestore.gc.PFullGC";
	String hash_db_class = Main.hashesDBClass;
	String sdfsCliPassword = "admin";
	String sdfsCliSalt = HashFunctions.getRandomString(6);
	String sdfsCliListenAddr = "localhost";
	boolean sdfsCliRequireAuth = false;
	int sdfsCliPort = 6442;
	boolean sdfsCliEnabled = true;

	int network_port = 2222;
	String list_ip = "0.0.0.0";
	boolean networkEnable = false;
	private boolean useDSESize = true;
	private boolean useDSECapacity = true;
	private boolean usePerfMon = false;
	private String clusterID = "sdfs-cluster";
	private String clusterConfig = "/etc/sdfs/jgroups.cfg.xml";
	private byte clusterCopies = 2;
	private String perfMonFile = "/var/log/sdfs/perf.json";
	private boolean clusterRackAware = false;

	public void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("--help")) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("chunk-store-local")) {
			this.chunk_store_local = Boolean.parseBoolean((cmd
					.getOptionValue("chunk-store-local")));
		}
		if (!cmd.hasOption("volume-name")) {
			System.out
					.println("--volume-name and --volume-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		if (this.chunk_store_local && !cmd.hasOption("volume-capacity")) {
			System.out
					.println("--volume-name and --volume-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		volume_name = cmd.getOptionValue("volume-name");
		if (StringUtils.getSpecialCharacterCount(volume_name) > 0) {
			System.out
					.println("--volume-name cannot contain any special characters");
			System.exit(-1);
		}

		this.perfMonFile = OSValidator.getProgramBasePath() + File.separator
				+ "logs" + File.separator + "volume-" + volume_name
				+ "-perf.json";
		if (OSValidator.isWindows())
			hash_db_class = "org.opendedup.collections.FileBasedCSMap";
		this.volume_capacity = cmd.getOptionValue("volume-capacity");
		base_path = OSValidator.getProgramBasePath() + "volumes"
				+ File.separator + volume_name;
		if (cmd.hasOption("base-path")) {
			this.base_path = cmd.getOptionValue("base-path");
		}
		this.io_log = this.base_path + File.separator + "ioperf.log";
		this.dedup_db_store = this.base_path + File.separator + "ddb";
		this.chunk_store_data_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "chunks";
		this.chunk_store_hashdb_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "hdb";
		if (cmd.hasOption("dedup-db-store")) {
			this.dedup_db_store = cmd.getOptionValue("dedup-db-store");
		}
		if (cmd.hasOption("io-log")) {
			this.io_log = cmd.getOptionValue("io-log");
		}
		if (cmd.hasOption("io-safe-close")) {
			this.safe_close = Boolean.parseBoolean(cmd
					.getOptionValue("io-safe-close"));
		}
		if (cmd.hasOption("io-max-file-write-buffers")) {
			this.max_file_write_buffers = Integer.parseInt(cmd
					.getOptionValue("io-max-file-write-buffers"));
		} else {
			this.max_file_write_buffers = 1;
		}
		if (cmd.hasOption("hash-type")) {
			String ht = cmd.getOptionValue("hash-type");
			if (ht.equalsIgnoreCase(HashFunctionPool.TIGER_16)
					|| ht.equalsIgnoreCase(HashFunctionPool.TIGER_24)
					|| ht.equalsIgnoreCase(HashFunctionPool.MURMUR3_16)
					|| ht.equalsIgnoreCase(HashFunctionPool.VARIABLE_MURMUR3))
				this.hashType = ht;
			else {
				System.out.println("Invalid Hash Type. Must be "
						+ HashFunctionPool.TIGER_16 + " "
						+ HashFunctionPool.TIGER_24 + " "
						+ HashFunctionPool.MURMUR3_16 + " "
						+ HashFunctionPool.VARIABLE_MURMUR3);
				System.exit(-1);
			}
			if (ht.equalsIgnoreCase(HashFunctionPool.VARIABLE_MURMUR3)) {
				this.chunk_store_class = "org.opendedup.sdfs.filestore.VariableFileChunkStore";
				this.chunk_size = 128;
				this.compress = true;
				this.max_file_write_buffers =16;
			} else if (cmd.hasOption("chunkstore-class")) {
				this.chunk_store_class = cmd.getOptionValue("chunkstore-class");
			}
		}
		if (cmd.hasOption("chunk-store-encrypt")) {
			this.chunk_store_encrypt = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-encrypt"));
			if (this.chunk_store_encrypt)
				this.chunk_store_class = "org.opendedup.sdfs.filestore.VariableFileChunkStore";
		}
		if (cmd.hasOption("chunk-store-encryption-key")) {
			String key = cmd.getOptionValue("chunk-store-encryption-key");
			if (key.length() < 8) {
				System.err
						.println("Encryption Key must be greater than 8 characters");
				System.exit(-1);
			} else {
				this.chunk_store_encryption_key = cmd
						.getOptionValue("chunk-store-encryption-key");
			}
		}
		if (cmd.hasOption("chunk-store-iv")) {
			String iv =  cmd.getOptionValue("chunk-store-iv");
			this.chunk_store_iv = iv;
		}

		if (cmd.hasOption("io-safe-sync")) {
			this.safe_sync = Boolean.parseBoolean(cmd
					.getOptionValue("io-safe-sync"));
		}
		if (cmd.hasOption("io-write-threads")) {
			this.write_threads = Short.parseShort(cmd
					.getOptionValue("io-write-threads"));
		}
		if (cmd.hasOption("io-chunk-size")) {
			this.chunk_size = Short.parseShort(cmd
					.getOptionValue("io-chunk-size"));
		}
		
		if (cmd.hasOption("io-max-open-files")) {
			this.max_open_files = Integer.parseInt(cmd
					.getOptionValue("io-max-open-files"));
		}
		if (cmd.hasOption("io-meta-file-cache")) {
			this.meta_file_cache = Integer.parseInt(cmd
					.getOptionValue("io-meta-file-cache"));
		}
		if (cmd.hasOption("io-claim-chunks-schedule")) {
			this.fdisk_schedule = cmd
					.getOptionValue("io-claim-chunks-schedule");
		}
		if (cmd.hasOption("permissions-file")) {
			this.filePermissions = cmd.getOptionValue("permissions-file");
		}
		if (cmd.hasOption("permissions-folder")) {
			this.dirPermissions = cmd.getOptionValue("permissions-folder");
		}
		if (cmd.hasOption("permissions-owner")) {
			this.owner = cmd.getOptionValue("permissions-owner");
		}
		if (cmd.hasOption("chunk-store-data-location")) {
			this.chunk_store_data_location = cmd
					.getOptionValue("chunk-store-data-location");
		}
		if (cmd.hasOption("chunk-store-hashdb-location")) {
			this.chunk_store_hashdb_location = cmd
					.getOptionValue("chunk-store-hashdb-location");
		}
		if (cmd.hasOption("chunk-store-hashdb-class")) {
			this.hash_db_class = cmd.getOptionValue("chunk-store-hashdb-class");
		}

		if (cmd.hasOption("aws-enabled")) {
			this.awsEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("azure-enabled")) {
			this.azureEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("azure-enabled"));
		}

		if (cmd.hasOption("gc-class")) {
			this.gc_class = cmd.getOptionValue("gc-class");
		}
		if (this.awsEnabled) {
			if (cmd.hasOption("cloud-secret-key")
					&& cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.compress = true;
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
				if (!S3ChunkStore.checkAuth(cloudAccessKey, cloudSecretKey)) {
					System.out.println("Error : Unable to create volume");
					System.out
							.println("cloud-access-key or cloud-secret-key is incorrect");
					System.exit(-1);
				}
				if (!S3ChunkStore.checkBucketUnique(cloudAccessKey,
						cloudSecretKey, cloudBucketName)) {
					System.out.println("Error : Unable to create volume");
					System.out.println("cloud-bucket-name is not unique");
					System.exit(-1);
				}

			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.out.println(cmd.getOptionValue("cloud-access-key"));
				System.out.println(cmd.getOptionValue("cloud-secret-key"));
				System.out.println(cmd.getOptionValue("cloud-bucket-name"));
				System.exit(-1);
			}
		} else if (this.gsEnabled) {
			if (cmd.hasOption("cloud-secret-key")
					&& cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.compress = true;
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
		}

		else if (this.azureEnabled) {
			if (cmd.hasOption("cloud-secret-key")
					&& cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.compress = true;
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
		}
		if (cmd.hasOption("chunk-store-io-threads")) {
			this.cloudThreads = Integer.parseInt(cmd
					.getOptionValue("cloud-io-threads"));
		}
		if (cmd.hasOption("chunk-store-compress")) {
			this.compress = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-compress"));
			if (this.compress && !this.awsEnabled && !this.gsEnabled
					&& this.azureEnabled) {
				this.chunk_store_class = "org.opendedup.sdfs.filestore.VariableFileChunkStore";
			}
		}
		if (cmd.hasOption("volume-maximum-full-percentage")) {
			this.max_percent_full = Double.parseDouble(cmd
					.getOptionValue("volume-maximum-full-percentage")) / 100;
		}
		if (cmd.hasOption("chunk-store-size")) {
			this.chunk_store_allocation_size = StringUtils.parseSize(cmd
					.getOptionValue("chunk-store-size"));
		} else {
			this.chunk_store_allocation_size = StringUtils
					.parseSize(this.volume_capacity);
		}
		if (cmd.hasOption("dse-enable-network")) {
			this.networkEnable = true;
		}
		if (cmd.hasOption("dse-listen-ip")) {
			this.list_ip = cmd.getOptionValue("dse-listen-ip");
			this.networkEnable = true;
		}
		if (cmd.hasOption("dse-listen-port")) {
			this.network_port = Integer.parseInt(cmd
					.getOptionValue("listen-port"));
		}

		if (cmd.hasOption("cluster-dse-password"))
			this.clusterDSEPassword = cmd
					.getOptionValue("cluster-dse-password");
		if (cmd.hasOption("cluster-id")) {
			this.clusterID = cmd.getOptionValue("cluster-id");
			if (StringUtils.getSpecialCharacterCount(this.clusterID) > 0) {
				System.out
						.println("--cluster-id cannot contain any special characters");
				System.exit(-1);
			}

		}
		if (cmd.hasOption("cluster-config"))
			this.clusterConfig = cmd.getOptionValue("cluster-config");
		if (cmd.hasOption("cluster-block-replicas")) {
			this.clusterCopies = Byte.parseByte(cmd
					.getOptionValue("cluster-block-replicas"));
			if (this.clusterCopies > 7)
				System.err
						.println("You can only specify up to 7 replica copies of unique blocks");
		}
		if (cmd.hasOption("cluster-rack-aware"))
			this.clusterRackAware = Boolean.parseBoolean(cmd
					.getOptionValue("cluster-rack-aware"));
		if (cmd.hasOption("enable-replication-master")) {
			this.sdfsCliRequireAuth = true;
			this.sdfsCliListenAddr = "0.0.0.0";
			this.networkEnable = true;
		}
		if (cmd.hasOption("sdfscli-password")) {
			this.sdfsCliPassword = cmd.getOptionValue("sdfscli-password");
		}
		if (cmd.hasOption("sdfscli-require-auth")) {
			this.sdfsCliRequireAuth = true;
		}
		if (cmd.hasOption("sdfscli-listen-port")) {
			this.sdfsCliPort = Integer.parseInt(cmd
					.getOptionValue("sdfscli-listen-port"));
		}
		if (cmd.hasOption("sdfscli-listen-addr"))
			this.sdfsCliListenAddr = cmd.getOptionValue("sdfscli-listen-addr");

		File file = new File(OSValidator.getConfigPath()
				+ this.volume_name.trim() + "-volume-cfg.xml");
		if (file.exists()) {
			throw new IOException("Volume [" + this.volume_name
					+ "] already exists");
		}
		if (cmd.hasOption("report-dse-size")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd
						.getOptionValue("report-dse-size"));
				// this.useDSECapacity = rp;
				this.useDSESize = rp;
			} catch (Throwable e) {
				System.err
						.println("value for report-dse-size must be true or false");
			}
		}
		if (cmd.hasOption("report-dse-capacity")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd
						.getOptionValue("report-dse-capacity"));
				this.useDSECapacity = rp;
				// this.useDSESize = rp;
			} catch (Throwable e) {
				System.err
						.println("value for report-dse-capacity must be true or false");
			}
		}
		if (cmd.hasOption("use-perf-mon")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd
						.getOptionValue("use-perf-mon"));
				this.usePerfMon = rp;
			} catch (Throwable e) {
				System.err
						.println("value for use-perf-mon must be true or false");
			}
		}
	}

	public void writeConfigFile() throws ParserConfigurationException,
			IOException {
		File dir = new File(OSValidator.getConfigPath());
		if (!dir.exists()) {
			System.out.println("making" + dir.getAbsolutePath());
			dir.mkdirs();
		}
		File file = new File(OSValidator.getConfigPath()
				+ this.volume_name.trim() + "-volume-cfg.xml");
		// Create XML DOM document (Memory consuming).
		Document xmldoc = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		xmldoc = impl.createDocument(null, "subsystem-config", null);
		// Root element.

		Element root = xmldoc.getDocumentElement();
		root.setAttribute("version", Main.version);
		Element locations = xmldoc.createElement("locations");

		locations.setAttribute("dedup-db-store", this.dedup_db_store);
		locations.setAttribute("io-log", this.io_log);
		root.appendChild(locations);

		Element io = xmldoc.createElement("io");
		io.setAttribute("log-level", "1");
		io.setAttribute("chunk-size", Short.toString(this.chunk_size));
		
		io.setAttribute("dedup-files", Boolean.toString(this.dedup_files));
		io.setAttribute("max-file-inactive", "900");
		io.setAttribute("max-file-write-buffers",
				Integer.toString(this.max_file_write_buffers));
		io.setAttribute("max-open-files", Integer.toString(this.max_open_files));
		io.setAttribute("meta-file-cache",
				Integer.toString(this.meta_file_cache));
		io.setAttribute("safe-close", Boolean.toString(this.safe_close));
		io.setAttribute("safe-sync", Boolean.toString(this.safe_sync));
		io.setAttribute("write-threads", Integer.toString(this.write_threads));
		io.setAttribute("claim-hash-schedule", this.fdisk_schedule);
		io.setAttribute("hash-type", this.hashType);
		root.appendChild(io);

		Element perm = xmldoc.createElement("permissions");
		perm.setAttribute("default-file", this.filePermissions);
		perm.setAttribute("default-folder", this.dirPermissions);
		perm.setAttribute("default-group", this.group);
		perm.setAttribute("default-owner", this.owner);
		root.appendChild(perm);
		Element vol = xmldoc.createElement("volume");
		vol.setAttribute("name", this.volume_name);
		vol.setAttribute("capacity", this.volume_capacity);
		vol.setAttribute("current-size", "0");
		vol.setAttribute("path", this.base_path + File.separator + "files");
		vol.setAttribute("maximum-percentage-full",
				Double.toString(this.max_percent_full));
		vol.setAttribute("closed-gracefully", "true");
		vol.setAttribute("use-dse-capacity",
				Boolean.toString(this.useDSECapacity));
		vol.setAttribute("use-dse-size", Boolean.toString(this.useDSESize));
		vol.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		vol.setAttribute("perf-mon-file", this.perfMonFile);
		vol.setAttribute("cluster-id", this.clusterID);
		vol.setAttribute("cluster-block-copies", Byte.toString(clusterCopies));
		vol.setAttribute("cluster-response-timeout",
				Integer.toString(chunk_size * 1000));
		vol.setAttribute("cluster-rack-aware",
				Boolean.toString(clusterRackAware));
		vol.setAttribute("read-timeout-seconds", Integer.toString(this.read_timeout));
		vol.setAttribute("write-timeout-seconds", Integer.toString(this.write_timeout));
		root.appendChild(vol);

		Element cs = xmldoc.createElement("local-chunkstore");
		cs.setAttribute("enabled", Boolean.toString(this.chunk_store_local));
		cs.setAttribute("average-chunk-size", Integer.toString(this.avgPgSz));
		cs.setAttribute("allocation-size",
				Long.toString(this.chunk_store_allocation_size));
		cs.setAttribute("gc-class", this.gc_class);
		cs.setAttribute("chunk-store", this.chunk_store_data_location);
		cs.setAttribute("encrypt", Boolean.toString(this.chunk_store_encrypt));
		cs.setAttribute("encryption-key", this.chunk_store_encryption_key);
		cs.setAttribute("encryption-iv", this.chunk_store_iv);
		cs.setAttribute("max-repl-batch-sz",
				Integer.toString(Main.MAX_REPL_BATCH_SZ));
		cs.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		cs.setAttribute("chunkstore-class", this.chunk_store_class);
		cs.setAttribute("hashdb-class", this.hash_db_class);
		cs.setAttribute("cluster-id", this.clusterID);
		cs.setAttribute("cluster-config", this.clusterConfig);
		cs.setAttribute("cluster-dse-password", this.clusterDSEPassword);
		cs.setAttribute("io-threads", Integer.toString(this.cloudThreads));

		cs.setAttribute("compress", Boolean.toString(this.compress));
		Element network = xmldoc.createElement("network");
		network.setAttribute("hostname", this.list_ip);
		network.setAttribute("enable", Boolean.toString(networkEnable));
		network.setAttribute("port", Integer.toString(this.network_port));
		network.setAttribute("use-ssl", "false");
		cs.appendChild(network);
		Element sdfscli = xmldoc.createElement("sdfscli");
		sdfscli.setAttribute("enable-auth",
				Boolean.toString(this.sdfsCliRequireAuth));
		sdfscli.setAttribute("listen-address", this.sdfsCliListenAddr);
		try {
			sdfscli.setAttribute("password", HashFunctions.getSHAHash(
					this.sdfsCliPassword.getBytes(),
					this.sdfsCliSalt.getBytes()));
		} catch (Exception e) {
			System.out.println("unable to create password ");
			e.printStackTrace();
			throw new IOException(e);
		}
		sdfscli.setAttribute("salt", this.sdfsCliSalt);
		sdfscli.setAttribute("port", Integer.toString(this.sdfsCliPort));
		sdfscli.setAttribute("enable", Boolean.toString(this.sdfsCliEnabled));

		root.appendChild(sdfscli);

		if (this.awsEnabled) {
			Element aws = xmldoc.createElement("aws");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("aws-access-key", this.cloudAccessKey);
			aws.setAttribute("aws-secret-key", this.cloudSecretKey);
			aws.setAttribute("aws-bucket-name", this.cloudBucketName);
			cs.appendChild(aws);
		} else if (this.gsEnabled) {
			Element aws = xmldoc.createElement("google-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("gs-access-key", this.cloudAccessKey);
			aws.setAttribute("gs-secret-key", this.cloudSecretKey);
			aws.setAttribute("gs-bucket-name", this.cloudBucketName);
			cs.appendChild(aws);
		} else if (this.azureEnabled) {
			Element aws = xmldoc.createElement("azure-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("azure-access-key", this.cloudAccessKey);
			aws.setAttribute("azure-secret-key", this.cloudSecretKey);
			aws.setAttribute("azure-bucket-name", this.cloudBucketName);
			cs.appendChild(aws);
		}
		root.appendChild(cs);
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(xmldoc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.setOutputProperty(OutputKeys.INDENT, "yes");
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	public void writeGCConfigFile() throws ParserConfigurationException,
			IOException {
		File dir = new File(OSValidator.getConfigPath());
		if (!dir.exists()) {
			System.out.println("making" + dir.getAbsolutePath());
			dir.mkdirs();
		}
		File file = new File(OSValidator.getConfigPath() + this.clusterID
				+ "-gc-cfg.xml");
		File vlf = new File(OSValidator.getConfigPath() + this.clusterID
				+ "-volume-list.xml");
		// Create XML DOM document (Memory consuming).
		Document xmldoc = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		xmldoc = impl.createDocument(null, "subsystem-config", null);
		// Root element.

		Element root = xmldoc.getDocumentElement();
		root.setAttribute("version", Main.version);
		Element io = xmldoc.createElement("gc");
		io.setAttribute("log-level", "1");
		io.setAttribute("claim-hash-schedule", this.fdisk_schedule);
		io.setAttribute("cluster-id", this.clusterID);
		io.setAttribute("cluster-config", this.clusterConfig);
		io.setAttribute("cluster-dse-password", this.clusterDSEPassword);
		io.setAttribute("volume-list-file", vlf.getPath());
		root.appendChild(io);
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(xmldoc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.setOutputProperty(OutputKeys.INDENT, "yes");
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Display these options.").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-password")
				.withDescription(
						"The password used to authenticate to the sdfscli management interface. Thee default password is \"admin\".")
				.hasArg(true).withArgName("password").create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-require-auth")
				.withDescription(
						"Require authentication to connect to the sdfscli managment interface")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-listen-port")
				.withDescription(
						"TCP/IP Listenting port for the sdfscli management interface")
				.hasArg(true).withArgName("tcp port").create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-listen-addr")
				.withDescription(
						"IP Listenting address for the sdfscli management interface. This defaults to \"localhost\"")
				.hasArg(true).withArgName("ip address or host name").create());
		options.addOption(OptionBuilder
				.withLongOpt("base-path")
				.withDescription(
						"the folder path for all volume data and meta data.\n Defaults to: \n "
								+ OSValidator.getProgramBasePath()
								+ "<volume name>").hasArg().withArgName("PATH")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("base-path")
				.withDescription(
						"the folder path for all volume data and meta data.\n Defaults to: \n "
								+ OSValidator.getProgramBasePath()
								+ "<volume name>").hasArg().withArgName("PATH")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("gc-class")
				.withDescription(
						"The class used for intelligent block garbage collection.\n Defaults to: \n "
								+ Main.gcClass).hasArg()
				.withArgName("CLASS NAME").create());
		options.addOption(OptionBuilder
				.withLongOpt("dedup-db-store")
				.withDescription(
						"the folder path to location for the dedup file database.\n Defaults to: \n --base-path + "
								+ File.separator + "ddb").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-log")
				.withDescription(
						"the file path to location for the io log.\n Defaults to: \n --base-path + "
								+ File.separator + "sdfs.log").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-safe-close")
				.withDescription(
						"If true all files will be closed on filesystem close call. Otherwise, files will be closed"
								+ " based on inactivity. Set this to false if you plan on sharing the file system over"
								+ " an nfs share. True takes less RAM than False. \n Defaults to: \n true")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-safe-sync")
				.withDescription(
						"If true all files will sync locally on filesystem sync call. Otherwise, by defaule (false), files will sync"
								+ " on close and data will per written to disk based on --max-file-write-buffers.  "
								+ "Setting this to true will ensure that no data loss will occur if the system is turned off abrubtly"
								+ " at the cost of slower speed. \n Defaults to: \n false")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-write-threads")
				.withDescription(
						"The number of threads that can be used to process data writted to the file system. \n Defaults to: \n 16")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-dedup-files")
				.withDescription(
						"True mean that all files will be deduped inline by default. This can be changed on a one off"
								+ "basis by using the command \"setfattr -n user.cmd.dedupAll -v 556:false <path to file on sdfs volume>\"\n Defaults to: \n true")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-chunk-size")
				.withDescription(
						"The unit size, in kB, of chunks stored. Set this to 4 if you would like to dedup VMDK files inline.\n Defaults to: \n 4")
				.hasArg().withArgName("SIZE in kB").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-max-file-write-buffers")
				.withDescription(
						"The amount of memory to have available for reading and writing per file. Each buffer in the size"
								+ " of io-chunk-size. \n Defaults to: \n 24")
				.hasArg().withArgName("SIZE in MB").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-max-open-files")
				.withDescription(
						"The maximum number of files that can be open at any one time. "
								+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-meta-file-cache")
				.withDescription(
						"The maximum number metadata files to be cached at any one time. "
								+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-claim-chunks-schedule")
				.withDescription(
						"The schedule, in cron format, to claim deduped chunks with the Volume(s). "
								+ " \n Defaults to: \n 0 59 23 * * ?").hasArg()
				.withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder
				.withLongOpt("permissions-file")
				.withDescription(
						"Default File Permissions. "
								+ " \n Defaults to: \n 0644").hasArg()
				.withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder
				.withLongOpt("permissions-folder")
				.withDescription(
						"Default Folder Permissions. "
								+ " \n Defaults to: \n 0755").hasArg()
				.withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-owner")
				.withDescription("Default Owner. " + " \n Defaults to: \n 0")
				.hasArg().withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-group")
				.withDescription("Default Group. " + " \n Defaults to: \n 0")
				.hasArg().withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder
				.withLongOpt("volume-capacity")
				.withDescription(
						"Capacity of the volume in [MB|GB|TB]. "
								+ " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("SIZE [MB|GB|TB]").create());
		options.addOption(OptionBuilder
				.withLongOpt("volume-name")
				.withDescription(
						"The name of the volume. "
								+ " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("STRING").create());
		options.addOption(OptionBuilder
				.withLongOpt("volume-maximum-full-percentage")
				.withDescription(
						"The maximum percentage of the volume capacity, as set by volume-capacity, before the volume starts"
								+ "reporting that the disk is full. If the number is negative then it will be infinite. This defaults to 95 "
								+ " \n e.g. --volume-maximum-full-percentage=95")
				.hasArg().withArgName("PERCENTAGE").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-local")
				.withDescription(
						"enables or disables local chunk store. The chunk store can be "
								+ "local(true or remote(false) provided you supply the routing config file "
								+ "and there is a storageHub listening on the remote server(s) when you "
								+ "mount the SDFS volume."
								+ " \nDefaults to: \n true").hasArg()
				.withArgName("true|flase").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-data-location")
				.withDescription(
						"The directory where chunks will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "chunks").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-hashdb-location")
				.withDescription(
						"The directory where hash database for chunk locations will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "hdb").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-pre-allocate")
				.withDescription(
						"Pre-allocate the chunk store if true."
								+ " \nDefaults to: \n false").hasArg()
				.withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunkstore-class")
				.withDescription(
						"The class for the specific chunk store to be used. \n Defaults to org.opendedup.sdfs.filestore.FileChunkStore")
				.hasArg().withArgName("Class Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-gc-schedule")
				.withDescription(
						"The schedule, in cron format, to check for unclaimed chunks within the Dedup Storage Engine. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 0 0 0/2 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-hashdb-class")
				.withDescription(
						"The class used to store hash values \n Defaults to: \n "
								+ Main.hashesDBClass).hasArg()
				.withArgName("class name").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-size")
				.withDescription(
						"The size in MB,TB,GB of the Dedup Storeage Engine. "
								+ "This . \n Defaults to: \n The size of the Volume")
				.hasArg().withArgName("MB|GB|TB").create());
		options.addOption(OptionBuilder
				.withLongOpt("hash-type")
				.withDescription(
						"This is the type of hash engine used to calculate a unique hash. The valid options for hash-type are "
								+ HashFunctionPool.TIGER_16
								+ " "
								+ HashFunctionPool.TIGER_24
								+ " "
								+ HashFunctionPool.MURMUR3_16
								+ " "
								+ HashFunctionPool.VARIABLE_MURMUR3
								+ " This Defaults to "
								+ HashFunctionPool.MURMUR3_16)
				.hasArg()
				.withArgName(
						HashFunctionPool.TIGER_16 + "|"
								+ HashFunctionPool.TIGER_24 + "|"
								+ HashFunctionPool.MURMUR3_16 + "|"
								+ HashFunctionPool.VARIABLE_MURMUR3).create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-encrypt")
				.withDescription(
						"Whether or not to Encrypt chunks within the Dedup Storage Engine. The encryption key is generated automatically."
								+ " For AWS this is a good option to enable. The default for this is"
								+ " false").hasArg().withArgName("true|false")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-encryption-key")
				.withDescription(
						"The encryption key used for encrypting data. If not specified a strong key will be generated automatically. They key must be at least 8 charaters long")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-iv")
				.withDescription(
						"The encryption  initialization vector (IV) used for encrypting data. If not specified a strong key will be generated automatically")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Amazon S3 Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("cloud-secret-key")
				.withDescription(
						"Set to the value of Cloud Storage secret key.")
				.hasArg().withArgName("Cloud Secret Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("cloud-access-key")
				.withDescription(
						"Set to the value of Cloud Storage access key.")
				.hasArg().withArgName("Cloud Access Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-io-threads")
				.withDescription(
						"Sets the number of io threads to use for io operations to the dse storage provider. This is set to 8 by default but can be changed to more or less based on bandwidth and io.")
				.hasArg().withArgName("integer").create());
		options.addOption(OptionBuilder
				.withLongOpt("cloud-bucket-name")
				.withDescription(
						"Set to the value of Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. aws-enabled, aws-secret-key, and aws-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique Cloud Bucket Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-compress")
				.withDescription(
						"Compress chunks before they are stored. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("gs-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Google Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("azure-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Microsoft Azure Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-listen-ip")
				.withDescription(
						"Host name or IPv4 Address to listen on for incoming connections. Defaults to \"0.0.0.0\"")
				.hasArg().withArgName("IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-listen-port")
				.withDescription(
						"TCP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("TCP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-enable-network")
				.withDescription(
						"Enable Network Services for Dedup Storage Enginge to serve remote hosts")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("enable-replication-master")
				.withDescription("Enable this volume as a replication master")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("report-dse-size")
				.withDescription(
						"If set to \"true\" this volume will used as the actual"
								+ " used statistics from the DSE. If this value is set to \"false\" it will"
								+ "report as virtual size of the volume and files. Defaults to \"true\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("report-dse-capacity")
				.withDescription(
						"If set to \"true\" this volume will report capacity the actual"
								+ "capacity statistics from the DSE. If this value is set to \"false\" it will"
								+ "report as virtual size of the volume and files. Defaults to \"true\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("use-perf-mon")
				.withDescription(
						"If set to \"true\" this volume will log io statistics to /etc/sdfs/ directory. Defaults to \"false\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-id")
				.withDescription(
						"The name used to identify the cluster group. This defaults to sdfs-cluster. This name should be the same on all members of this cluster")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-config")
				.withDescription(
						"The jgroups configuration used to configure this cluster node. This defaults to \"/etc/sdfs/jgroups.cfg.xml\". ")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-dse-password")
				.withDescription(
						"The jgroups configuration used to configure this cluster node. This defaults to \"/etc/sdfs/jgroups.cfg.xml\". ")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-block-replicas")
				.withDescription(
						"The number copies to distribute to descrete nodes for each unique block. As an example if this value is set to"
								+ "\"3\" the volume will attempt to write any unique block to \"3\" DSE nodes, if available.  This defaults to \"2\". ")
				.hasArg().withArgName("Value [1-7]").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-rack-aware")
				.withDescription(
						"If set to true, the clustered volume will be rack aware and make the best effort to distribute blocks to multiple racks"
								+ " based on the cluster-block-replicas. As an example, if cluster-block replicas is set to \"2\" and cluster-rack-aware is set to \"true\""
								+ " any unique block will be sent to two different racks if present. The mkdse option --cluster-node-rack should be used to distinguish racks per dse node "
								+ " for this cluster.").hasArg()
				.withArgName("true|false").create());
		return options;
	}

	public static void main(String[] args) {
		try {
			System.out.println("Attempting to create SDFS volume ...");
			File f = new File(OSValidator.getConfigPath());
			if (!f.exists())
				f.mkdirs();
			VolumeConfigWriter wr = new VolumeConfigWriter();
			wr.parseCmdLine(args);
			wr.writeConfigFile();
			System.out.println("Volume [" + wr.volume_name
					+ "] created with a capacity of [" + wr.volume_capacity
					+ "]");
			System.out
					.println("check ["
							+ OSValidator.getConfigPath()
							+ wr.volume_name.trim()
							+ "-volume-cfg.xml] for configuration details if you need to change anything");
			/*
			 * if (!wr.chunk_store_local) { File _f = new
			 * File(OSValidator.getConfigPath() + wr.clusterID + "-gc-cfg.xml");
			 * if (_f.exists()) System.out .println(
			 * "Existing Garbage Collection Service Configuration File already created at ["
			 * + OSValidator.getConfigPath() + wr.clusterID + "-gc-cfg.xml]");
			 * else { wr.writeGCConfigFile(); System.out .println(
			 * "New Garbage Collection Service Configuration File Created at ["
			 * + OSValidator.getConfigPath() + wr.clusterID + "-gc-cfg.xml]"); }
			 * }
			 */
		} catch (Exception e) {
			System.err.println("ERROR : Unable to create volume because "
					+ e.toString());
			e.printStackTrace();
			System.exit(-1);
		}
		System.exit(0);
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter
				.printHelp(
						"mkfs.sdfs --volume-name=sdfs --volume-capacity=100GB",
						options);
	}

	@SuppressWarnings("unused")
	private static long calcMem(long dseSize, int blocksz) {
		double mem = (dseSize / blocksz) * 25;
		mem = (mem / 1024) / 1024;
		double _dmem = mem / 1000;
		_dmem = Math.ceil(_dmem);
		long _mem = ((long) (_dmem * 1000)) + calcXmn(dseSize, blocksz);
		return _mem;
	}

	private static long calcXmn(long dseSize, int blocksz) {
		double mem = (dseSize / blocksz) * 25;
		mem = (mem / 1024) / 1024;
		double _dmem = mem / 400;
		_dmem = Math.ceil(_dmem);
		long _mem = ((long) (_dmem * 100));
		if (_mem > 2000)
			_mem = 2000;
		return _mem + Main.MAX_REPL_BATCH_SZ;
	}

}
