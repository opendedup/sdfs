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
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.opendedup.sdfs.filestore.S3ChunkStore;
import org.opendedup.util.HashFunctions;
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
	int write_threads = (short) (Runtime.getRuntime().availableProcessors() * 3);
	boolean dedup_files = true;
	int multi_read_timeout = 1000;
	int system_read_cache = 1000;
	short chunk_size = 128;
	int max_file_write_buffers = 24;
	int file_read_cache = 5;
	int max_open_files = 1024;
	int meta_file_cache = 1024;
	String filePermissions = "0644";
	String dirPermissions = "0755";
	String owner = "0";
	String group = "0";
	String volume_capacity = null;
	double max_percent_full = -1;
	boolean chunk_store_local = true;
	String chunk_store_data_location = null;
	String chunk_store_hashdb_location = null;
	boolean chunk_store_pre_allocate = false;
	long chunk_store_allocation_size = 0;
	Short chunk_read_ahead_pages = 4;
	String chunk_gc_schedule = "0 0 0/4 * * ?";
	String fdisk_schedule = "0 0 0/2 * * ?";
	int remove_if_older_than = 6;
	boolean awsEnabled = false;
	String awsAccessKey = "";
	String awsSecretKey = "";
	String awsBucketName = "";
	boolean gsCompress = Main.awsCompress;
	boolean gsEnabled = false;
	String gsAccessKey = "";
	String gsSecretKey = "";
	String gsBucketName = "";
	boolean awsCompress = Main.awsCompress;
	int chunk_store_read_cache = Main.chunkStorePageCache;
	int chunk_store_dirty_timeout = Main.chunkStoreDirtyCacheTimeout;
	String chunk_store_encryption_key = PassPhrase.getNext();
	boolean chunk_store_encrypt = false;

	int hashSize = 16;
	String chunk_store_class = "org.opendedup.sdfs.filestore.FileChunkStore";
	String gc_class = "org.opendedup.sdfs.filestore.gc.PFullGC";
	String sdfsCliPassword = "admin";
	String sdfsCliSalt = HashFunctions.getRandomString(6);
	String sdfsCliListenAddr = "localhost";
	boolean sdfsCliRequireAuth = false;
	int sdfsCliPort = 6442;
	boolean sdfsCliEnabled = true;
	
	boolean upstreamEnabled = false;
	String upstreamHost = null;
	String upstreamPassword = "admin";
	int upstreamPort = 2222;
	boolean use_udp = false;
	int network_port = 2222;
	String list_ip = "0.0.0.0";
	boolean networkEnable = false;
	

	public void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("--help")) {
			printHelp(options);
			System.exit(1);
		}
		if (!cmd.hasOption("volume-name") || !cmd.hasOption("volume-capacity")) {
			System.out
					.println("--volume-name and --volume-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		volume_name = cmd.getOptionValue("volume-name");
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
		if (cmd.hasOption("hash-size")) {
			int hs = Integer.parseInt(cmd.getOptionValue("hash-size"));
			if (hs == 16 || hs == 24)
				this.hashSize = hs;
			else
				throw new Exception("hash size must be 16 or 24");
		}
		if (cmd.hasOption("chunkstore-class")) {
			this.chunk_store_class = cmd.getOptionValue("chunkstore-class");
		}
		if (cmd.hasOption("io-safe-sync")) {
			this.safe_sync = Boolean.parseBoolean(cmd
					.getOptionValue("io-safe-sync"));
		}
		if (cmd.hasOption("io-write-threads")) {
			this.write_threads = Short.parseShort(cmd
					.getOptionValue("io-write-threads"));
		}
		if (cmd.hasOption("io-dedup-files")) {
			this.dedup_files = Boolean.parseBoolean(cmd
					.getOptionValue("io-dedup-files"));
		}
		if (cmd.hasOption("io-multi-read-timeout")) {
			this.multi_read_timeout = Integer.parseInt(cmd
					.getOptionValue("io-multi-read-timeout"));
		}
		if (cmd.hasOption("io-system-read-cache")) {
			this.system_read_cache = Integer.parseInt(cmd
					.getOptionValue("io-system-read-cache"));
		}
		if (cmd.hasOption("io-chunk-size")) {
			this.chunk_size = Short.parseShort(cmd
					.getOptionValue("io-chunk-size"));
		}
		if (cmd.hasOption("io-max-file-write-buffers")) {
			this.max_file_write_buffers = Integer.parseInt(cmd
					.getOptionValue("io-max-file-write-buffers"));
		} else {
			this.max_file_write_buffers = 1;
		}
		if (cmd.hasOption("io-max-open-files")) {
			this.max_open_files = Integer.parseInt(cmd
					.getOptionValue("io-max-open-files"));
		}
		if (cmd.hasOption("io-file-read-cache")) {
			this.file_read_cache = Integer.parseInt(cmd
					.getOptionValue("io-file-read-cache"));
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
		if (cmd.hasOption("chunk-store-pre-allocate")) {
			this.chunk_store_pre_allocate = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-pre-allocate"));
		}
		
		if(cmd.hasOption("sdfscli-password")) {
			this.sdfsCliPassword = cmd.getOptionValue("sdfscli-password");
		}
		if(cmd.hasOption("sdfscli-require-auth")) {
			this.sdfsCliRequireAuth = true;
		}
		if(cmd.hasOption("sdfscli-listen-port")) {
			this.sdfsCliPort = Integer.parseInt(cmd.getOptionValue("sdfscli-listen-port"));
		}
		if(cmd.hasOption("sdfscli-listen-addr"))
			this.sdfsCliListenAddr = cmd.getOptionValue("sdfscli-listen-addr");

		if (cmd.hasOption("chunk-read-ahead-pages")) {
			this.chunk_read_ahead_pages = Short.parseShort(cmd
					.getOptionValue("chunk-read-ahead-pages"));
		} else {
			if (this.chunk_size < 32) {
				this.chunk_read_ahead_pages = (short) (32 / this.chunk_size);
			} else {
				this.chunk_read_ahead_pages = 1;
			}
		}
		if (cmd.hasOption("chunk-store-local")) {
			this.chunk_store_local = Boolean.parseBoolean((cmd
					.getOptionValue("chunk-store-local")));
		}
		if (cmd.hasOption("aws-enabled")) {
			this.awsEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("chunk-store-read-cache")) {
			this.chunk_store_read_cache = Integer.parseInt(cmd
					.getOptionValue("chunk-store-read-cache"));
		}
		if (cmd.hasOption("chunk-store-encrypt")) {
			this.chunk_store_encrypt = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-encrypt"));
		}
		if (cmd.hasOption("chunk-store-dirty-timeout")) {
			this.chunk_store_dirty_timeout = Integer.parseInt(cmd
					.getOptionValue("chunk-store-dirty-timeout"));
		}
		if (cmd.hasOption("gc-class")) {
			this.gc_class = cmd.getOptionValue("gc-class");
		}
		if (this.awsEnabled) {
			if (cmd.hasOption("aws-secret-key")
					&& cmd.hasOption("aws-access-key")
					&& cmd.hasOption("aws-bucket-name")) {
				this.awsAccessKey = cmd.getOptionValue("aws-access-key");
				this.awsSecretKey = cmd.getOptionValue("aws-secret-key");
				this.awsBucketName = cmd.getOptionValue("aws-bucket-name");
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
				if(!S3ChunkStore.checkAuth(awsAccessKey, awsSecretKey)) {
					System.out.println("Error : Unable to create volume");
					System.out
							.println("aws-access-key or aws-secret-key is incorrect");
					System.exit(-1);
				}
				if(!S3ChunkStore.checkBucketUnique(awsAccessKey, awsSecretKey, awsBucketName)) {
					System.out.println("Error : Unable to create volume");
					System.out
							.println("aws-bucket-name is not unique");
					System.exit(-1);
				}
					
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("aws-access-key, aws-secret-key, and aws-bucket-name are required.");
				System.exit(-1);
			}
			if (cmd.hasOption("aws-compress"))
				this.awsCompress = Boolean.parseBoolean(cmd
						.getOptionValue("aws-compress"));
		} else if (this.gsEnabled) {
			if (cmd.hasOption("gs-secret-key")
					&& cmd.hasOption("gs-access-key")
					&& cmd.hasOption("gs-bucket-name")) {
				this.awsAccessKey = cmd.getOptionValue("gs-access-key");
				this.awsSecretKey = cmd.getOptionValue("gs-secret-key");
				this.awsBucketName = cmd.getOptionValue("gs-bucket-name");
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("gs-access-key, gs-secret-key, and gs-bucket-name are required.");
				System.exit(-1);
			}
			if (cmd.hasOption("gs-compress"))
				this.awsCompress = Boolean.parseBoolean(cmd
						.getOptionValue("aws-compress"));
		}

		if (cmd.hasOption("chunk-store-gc-schedule")) {
			this.chunk_gc_schedule = cmd
					.getOptionValue("chunk-store-gc-schedule");
		}
		if (cmd.hasOption("chunk-store-eviction")) {
			this.remove_if_older_than = Integer.parseInt(cmd
					.getOptionValue("chunk-store-eviction"));
		}
		if (cmd.hasOption("volume-maximum-full-percentage")) {
			this.max_percent_full = Double.parseDouble(cmd
					.getOptionValue("volume-maximum-full-percentage"));
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
		if (cmd.hasOption("dse-enable-udp")) {
			this.use_udp = true;
		}
		if (cmd.hasOption("dse-listen-ip")) {
			this.list_ip = cmd.getOptionValue("dse-listen-ip");
			this.networkEnable = true;
		}
		if (cmd.hasOption("dse-listen-port")) {
			this.network_port = Integer.parseInt(cmd
					.getOptionValue("listen-port"));
		}
		if(cmd.hasOption("dse-upstream-enabled")) {
			if(!cmd.hasOption("dse-upstream-host")) {
				throw new Exception("dse-upstream-host must be specified");
			} else {
				this.upstreamHost = cmd.getOptionValue("dse-upstream-host");
				if(cmd.hasOption("dse-upstream-host-port"))
					this.upstreamPort = Integer.parseInt(cmd.getOptionValue("dse-upstream-host-port"));
			}
		}
		if(cmd.hasOption("dse-upstream-password"))
			this.upstreamPassword = cmd.getOptionValue("dse-upstream-password");
		if(cmd.hasOption("enable-replication-master")) {
			this.sdfsCliRequireAuth = true;
			this.sdfsCliListenAddr = "0.0.0.0";
			this.networkEnable = true;
		}
		if(cmd.hasOption("enable-replication-slave")) {
			if(!cmd.hasOption("replication-master"))
				throw new Exception("replication-master must be specified");
			this.upstreamHost = cmd.getOptionValue("replication-master");
			this.upstreamEnabled= true;
		}
		if(cmd.hasOption("replication-master-password"))
			this.upstreamPassword = cmd.getOptionValue("replication-master-password");

		File file = new File(OSValidator.getConfigPath()
				+ this.volume_name.trim() + "-volume-cfg.xml");
		if (file.exists()) {
			throw new IOException("Volume [" + this.volume_name
					+ "] already exists");
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
		io.setAttribute("file-read-cache",
				Integer.toString(this.file_read_cache));
		io.setAttribute("max-file-inactive", "900");
		io.setAttribute("max-file-write-buffers",
				Integer.toString(this.max_file_write_buffers));
		io.setAttribute("max-open-files", Integer.toString(this.max_open_files));
		io.setAttribute("meta-file-cache",
				Integer.toString(this.meta_file_cache));
		io.setAttribute("multi-read-timeout",
				Integer.toString(this.multi_read_timeout));
		io.setAttribute("safe-close", Boolean.toString(this.safe_close));
		io.setAttribute("safe-sync", Boolean.toString(this.safe_sync));
		io.setAttribute("system-read-cache",
				Integer.toString(this.system_read_cache));
		io.setAttribute("write-threads", Integer.toString(this.write_threads));
		io.setAttribute("claim-hash-schedule", this.fdisk_schedule);
		io.setAttribute("hash-size", Integer.toString(this.hashSize));
		root.appendChild(io);

		Element perm = xmldoc.createElement("permissions");
		perm.setAttribute("default-file", this.filePermissions);
		perm.setAttribute("default-folder", this.dirPermissions);
		perm.setAttribute("default-group", this.group);
		perm.setAttribute("default-owner", this.owner);
		root.appendChild(perm);
		Element vol = xmldoc.createElement("volume");
		vol.setAttribute("capacity", this.volume_capacity);
		vol.setAttribute("current-size", "0");
		vol.setAttribute("path", this.base_path + File.separator + "files");
		vol.setAttribute("maximum-percentage-full",
				Double.toString(this.max_percent_full));
		vol.setAttribute("closed-gracefully", "true");
		root.appendChild(vol);

		Element cs = xmldoc.createElement("local-chunkstore");
		cs.setAttribute("enabled", Boolean.toString(this.chunk_store_local));
		cs.setAttribute("pre-allocate",
				Boolean.toString(this.chunk_store_pre_allocate));
		cs.setAttribute("allocation-size",
				Long.toString(this.chunk_store_allocation_size));
		cs.setAttribute("chunk-gc-schedule", this.chunk_gc_schedule);
		cs.setAttribute("eviction-age",
				Integer.toString(this.remove_if_older_than));
		cs.setAttribute("gc-class", this.gc_class);
		cs.setAttribute("read-ahead-pages",
				Short.toString(this.chunk_read_ahead_pages));
		cs.setAttribute("chunk-store", this.chunk_store_data_location);
		cs.setAttribute("encrypt", Boolean.toString(this.chunk_store_encrypt));
		cs.setAttribute("encryption-key", this.chunk_store_encryption_key);
		cs.setAttribute("chunk-store-read-cache",
				Integer.toString(this.chunk_store_read_cache));
		cs.setAttribute("chunk-store-dirty-timeout",
				Integer.toString(this.chunk_store_dirty_timeout));
		cs.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		cs.setAttribute("chunkstore-class", this.chunk_store_class);
		Element network = xmldoc.createElement("network");
		network.setAttribute("hostname", this.list_ip);
		network.setAttribute("enable", Boolean.toString(networkEnable));
		network.setAttribute("port", Integer.toString(this.network_port));
		network.setAttribute("use-udp", Boolean.toString(this.use_udp));
		network.setAttribute("upstream-enabled", Boolean.toString(this.upstreamEnabled));
		network.setAttribute("upstream-host", this.upstreamHost);
		network.setAttribute("upstream-host-port", Integer.toString(this.upstreamPort));
		network.setAttribute("upstream-password", this.upstreamPassword);
		cs.appendChild(network);
		Element launchParams = xmldoc.createElement("launch-params");
		launchParams.setAttribute("class-path", Main.classPath);
		launchParams.setAttribute("java-path", Main.javaPath);
		long mem = calcMem(this.chunk_store_allocation_size,this.chunk_size *1024);
		long xmn = calcXmn(this.chunk_store_allocation_size,this.chunk_size *1024);
		launchParams.setAttribute("java-options", Main.javaOptions + " -Xmx"
				+ mem + "m -Xmn" + xmn + "m");
		root.appendChild(launchParams);
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
			aws.setAttribute("aws-access-key", this.awsAccessKey);
			aws.setAttribute("aws-secret-key", this.awsSecretKey);
			aws.setAttribute("aws-bucket-name", this.awsBucketName);
			aws.setAttribute("compress", Boolean.toString(this.awsCompress));
			cs.appendChild(aws);
		} else if (this.gsEnabled) {
			Element aws = xmldoc.createElement("google-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("gs-access-key", this.gsAccessKey);
			aws.setAttribute("gs-secret-key", this.gsSecretKey);
			aws.setAttribute("gs-bucket-name", this.gsBucketName);
			aws.setAttribute("compress", Boolean.toString(this.gsCompress));
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
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
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
				.withDescription("The password used to authenticate to the sdfscli management interface. Thee default password is \"admin\"."
						).hasArg(true).withArgName("password")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-require-auth")
				.withDescription("Require authentication to connect to the sdfscli managment interface"
						).hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-listen-port")
				.withDescription("TCP/IP Listenting port for the sdfscli management interface"
						).hasArg(true).withArgName("tcp port")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("sdfscli-listen-addr")
				.withDescription("IP Listenting address for the sdfscli management interface. This defaults to \"localhost\""
						).hasArg(true).withArgName("ip address or host name")
				.create());
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
				.withLongOpt("io-multi-read-timeout")
				.withDescription(
						"Timeout to try to read from cache before it request data from the chunkstore. \n Defaults to: \n 1000")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-system-read-cache")
				.withDescription(
						"Size, in number of chunks, that read chunks will be cached into memory. \n Defaults to: \n 1000")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-chunk-size")
				.withDescription(
						"The unit size, in kB, of chunks stored. Set this to 4 if you would like to dedup VMDK files inline.\n Defaults to: \n 128")
				.hasArg().withArgName("SIZE in kB").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-max-file-write-buffers")
				.withDescription(
						"The amount of memory to have available for reading and writing per file. Each buffer in the size"
								+ " of io-chunk-size. \n Defaults to: \n 24")
				.hasArg().withArgName("SIZE in MB").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-file-read-cache")
				.withDescription(
						"The number of memory buffers to have available for reading per file. Each buffer in the size"
								+ " of io-chunk-size. \n Defaults to: \n 5")
				.hasArg().withArgName("NUMBER").create());
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
						"The schedule, in cron format, to claim deduped chunks with the Dedup Storage Engine. "
								+ "This should happen more frequently than the chunk-store-gc-schedule. \n Defaults to: \n 0 0 0/1 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
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
								+ "reporting that the disk is full. If the number is negative then it will be infinite. "
								+ " \n e.g. --volume-maximum-full-percentage=100")
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
				.withLongOpt("chunk-read-ahead-pages")
				.withDescription(
						"The number of pages to read ahead when doing a disk read on the chunk store."
								+ " \nDefaults to: \n 128/io-chunk-size or 1 if greater than 128")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-gc-schedule")
				.withDescription(
						"The schedule, in cron format, to check for unclaimed chunks within the Dedup Storage Engine. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 0 0 0/2 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-eviction")
				.withDescription(
						"The duration, in hours, that chunks will be removed from Dedup Storage Engine if unclaimed. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 6")
				.hasArg().withArgName("HOURS").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-size")
				.withDescription(
						"The size in MB,TB,GB of the Dedup Storeage Engine. "
								+ "This . \n Defaults to: \n The size of the Volume")
				.hasArg().withArgName("MB|GB|TB").create());
		options.addOption(OptionBuilder
				.withLongOpt("hash-size")
				.withDescription(
						"This is the size in bytes of the unique hash. In version 1.0 and below this would default to 24 and for newer"
								+ "versions this will default to 16. Set this to 24 if you would like to make the DSE backwards compatible to versions"
								+ "below 1.0.1 ."
								+ "This . \n Defaults to: \n 5MB").hasArg()
				.withArgName("16 or 24 bytes").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-read-cache")
				.withDescription(
						"The size in MB of the Dedup Storeage Engine's read cache. Its useful to set this if you have high number of reads"
								+ " for AWS/Cloud storage "
								+ "This . \n Defaults to: \n 5MB").hasArg()
				.withArgName("Megabytes").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-encrypt")
				.withDescription(
						"Whether or not to Encrypt chunks within the Dedup Storage Engine. The encryption key is generated automatically."
								+ " For AWS this is a good option to enable. The default for this is"
								+ " false").hasArg().withArgName("true|false")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-dirty-timeout")
				.withDescription(
						"The timeout, in milliseconds, for a previous read for the same chunk to finish within the Dedup Storage Engine. "
								+ "For AWS with slow links you may want to set this to a higher number. The default for this is"
								+ " 1000 ms.").hasArg()
				.withArgName("Milliseconds").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Amazon S3 Cloud Storage. aws-secret-key, aws-access-key, and aws-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-secret-key")
				.withDescription(
						"Set to the value of Amazon S3 Cloud Storage secret key. aws-enabled, aws-access-key, and aws-bucket-name will also need to be set. ")
				.hasArg().withArgName("S3 Secret Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-access-key")
				.withDescription(
						"Set to the value of Amazon S3 Cloud Storage access key. aws-enabled, aws-secret-key, and aws-bucket-name will also need to be set. ")
				.hasArg().withArgName("S3 Access Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-bucket-name")
				.withDescription(
						"Set to the value of Amazon S3 Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. aws-enabled, aws-secret-key, and aws-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique S3 Bucket Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-compress")
				.withDescription(
						"Compress AWS chunks before they are sent to the S3 Cloud Storeage bucket. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("gs-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Google Cloud Storage. gs-secret-key, gs-access-key, and gs-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("gs-secret-key")
				.withDescription(
						"Set to the value of Google Cloud Storage secret key. gs-enabled, gs-access-key, and gs-bucket-name will also need to be set. ")
				.hasArg().withArgName("Google Secret Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-access-key")
				.withDescription(
						"Set to the value of the Google Cloud Storage access key. gs-enabled, gs-secret-key, and gs-bucket-name will also need to be set. ")
				.hasArg().withArgName("Google Access Key").create());
		options.addOption(OptionBuilder
				.withLongOpt("gs-bucket-name")
				.withDescription(
						"Set to the value of the Google Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. gs-enabled, gs-secret-key, and gs-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique Google Bucket Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("gs-compress")
				.withDescription(
						"Compress chunks before they are sent to the Google Cloud Storeage bucket. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-enable-udp")
				.withDescription(
						"Enable udp for some communication between Volume and DSE. Defaults to false").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-listen-ip")
				.withDescription(
						"Host name or IPv4 Address to listen on for incoming connections. Defaults to \"0.0.0.0\"")
				.hasArg().withArgName("IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-listen-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-upstream-enabled")
				.withDescription(
						"Enable Upstream Dedup Storage Engine communication").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-upstream-host")
				.withDescription(
						"Host name or IPv4 Address ")
				.hasArg().withArgName("FQDN or IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-upstream-host-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-upstream-password")
				.withDescription(
						"SDFSCLI Password of upstream host. Defaults to \"admin\"")
				.hasArg().withArgName("STRING").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-listen-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-enable-network")
				.withDescription(
						"Enable Network Services for Dedup Storage Enginge to serve outside hosts").create());
		options.addOption(OptionBuilder
				.withLongOpt("enable-replication-master")
				.withDescription(
						"Enable this volume as a replication master").create());
		options.addOption(OptionBuilder
				.withLongOpt("enable-replication-slave")
				.withDescription(
						"Enable this volume as a replication slave").create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-master")
				.withDescription(
						"The Replication master for this slave")
						.hasArg().withArgName("FQDN or IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-master-password")
				.withDescription(
						"The Replication master sdfscli password. Defaults to \"admin\"")
						.hasArg().withArgName("STRING").create());
		return options;
	}

	public static void main(String[] args) {
		try {
			System.out.println("Attempting to create volume ...");
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
		} catch (Exception e) {
			System.err.println("ERROR : Unable to create volume because "
					+ e.toString());
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

	private static long calcMem(long dseSize,int blocksz) {
		double mem = (dseSize / blocksz) * 25;
		mem = (mem / 1024) / 1024;
		double _dmem = mem / 1000;
		_dmem = Math.ceil(_dmem);
		long _mem = ((long) (_dmem * 1000)) + calcXmn(dseSize,blocksz);
		return _mem;
	}

	private static long calcXmn(long dseSize,int blocksz) {
		double mem = (dseSize / blocksz) * 25;
		mem = (mem / 1024) / 1024;
		double _dmem = mem / 400;
		_dmem = Math.ceil(_dmem);
		long _mem = ((long) (_dmem * 100));
		if (_mem > 2000)
			_mem = 2000;
		return _mem;
	}

}
