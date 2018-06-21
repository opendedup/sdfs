package org.opendedup.sdfs;

import java.io.File;





import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

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
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.io.BaseEncoding;

public class VolumeConfigWriter {
	/**
	 * write the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	String volume_name = null;
	String base_path = OSValidator.getProgramBasePath() + File.separator + "volumes" + File.separator + volume_name;
	String dedup_db_store = base_path + File.separator + "ddb";
	String io_log = base_path + File.separator + "io.log";
	boolean safe_close = true;
	boolean vrts_appliance = false;
	boolean safe_sync = true;
	int write_threads = (short) (Runtime.getRuntime().availableProcessors());
	boolean dedup_files = true;
	int chunk_size = 256;
	long max_file_write_buffers = 1;
	int max_open_files = 128;
	int meta_file_cache = 512;
	int write_timeout = Main.writeTimeoutSeconds;
	int read_timeout = Main.readTimeoutSeconds;
	String filePermissions = "0644";
	String dirPermissions = "0755";
	String owner = "0";
	String group = "0";
	boolean simpleS3 = false;
	String volume_capacity = null;
	String clusterDSEPassword = "admin";
	int avgPgSz = 8192;
	boolean mdCompresstion = false;
	double max_percent_full = .95;
	String chunk_store_data_location = null;
	String chunk_store_hashdb_location = null;
	long chunk_store_allocation_size = 0;
	// String chunk_gc_schedule = "0 0 0/4 * * ?";
	// String fdisk_schedule = "0 59 23 * * ?";
	String fdisk_schedule = "0 0 12 ? * SUN";
	String ltrfdisk_schedule = "0 15 10 L * ?";
	String syncfs_schedule = "4 59 23 * * ?";
	private String dExt = null;
	boolean azureEnabled = false;
	boolean tcpKeepAlive = true;
	boolean awsEnabled = false;
	boolean gsEnabled = false;
	String cloudAccessKey = "";
	String cacheSize = "10GB";
	String cloudSecretKey = "";
	String cloudBucketName = "";
	int clusterRSPTimeout = 4000;
	boolean lowMemory = false;
	int maxSegSize = 32;
	int windowSize = 48;
	int cloudThreads = 8;
	private int glacierInDays = 0;
	private int aruzreArchiveInDays =0;
	private String azurestorageTier = null; 
	boolean compress = Main.compress;
	// int chunk_store_read_cache = Main.chunkStorePageCache;
	// int chunk_store_dirty_timeout = Main.chunkStoreDirtyCacheTimeout;
	String chunk_store_encryption_key = PassPhrase.getNext();
	String chunk_store_iv = PassPhrase.getIV();
	boolean chunk_store_encrypt = false;
	String hashType = HashFunctionPool.VARIABLE_MD5;
	String chunk_store_class = "org.opendedup.sdfs.filestore.BatchFileChunkStore";
	String gc_class = "org.opendedup.sdfs.filestore.gc.PFullGC";
	String hash_db_class = Main.hashesDBClass;
	String sdfsCliPassword = "admin";
	String sdfsCliSalt = HashFunctions.getRandomString(6);
	String sdfsCliListenAddr = "localhost";
	boolean sdfsCliSSL = true;
	boolean sdfsCliRequireAuth = false;
	int sdfsCliPort = 6442;
	boolean sdfsCliEnabled = true;
	String bucketLocation = null;
	String list_ip = "::";
	private boolean useDSESize = true;
	private boolean useDSECapacity = true;
	private boolean usePerfMon = false;
	private boolean basicS3Signer = false;
	private String clusterID = "sdfscluster";
	private String clusterConfig = "/etc/sdfs/jgroups.cfg.xml";
	private byte clusterCopies = 2;
	private String perfMonFile = "/var/log/sdfs/perf.json";
	private boolean clusterRackAware = false;
	private boolean ext = true;
	private boolean awsAim = false;
	private boolean genericS3 = false;
	private boolean atmosEnabled = false;
	private boolean backblazeEnabled = false;
	private String backlogSize = "0";
	private String cloudUrl;
	private boolean readAhead = false;
	private boolean usebasicsigner = false;
	private boolean disableDNSBucket = false;
	private boolean simpleMD = false;
	private boolean refreshBlobs = false;
	private boolean disableAutoGC = false;
	private String blockSize = "30 MB";
	private boolean minIOEnabled;
	private boolean aliEnabled;
	private String volumeType = "standard";
	private String userAgentPrefix = null;
	private boolean encryptConfig = false;
	private String glacierClass="standard";
	private long sn = new Random().nextLong();
	

	public VolumeConfigWriter() {
		sn = new Random().nextLong();
		if (sn < 0)
			sn = sn * -1;
	}

	public void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("--help")) {
			printHelp(options);
			System.exit(1);
		}
		if(cmd.hasOption("encrypt-config")) {
			this.encryptConfig = true;
		}
		if (cmd.hasOption("sdfscli-password")) {
			this.sdfsCliPassword = cmd.getOptionValue("sdfscli-password");
		}
		if (cmd.hasOption("sdfscli-require-auth")) {
			this.sdfsCliRequireAuth = true;
		}
		if (cmd.hasOption("sdfscli-listen-port")) {
			this.sdfsCliPort = Integer.parseInt(cmd.getOptionValue("sdfscli-listen-port"));
		}
		if (cmd.hasOption("sdfscli-listen-addr"))
			this.sdfsCliListenAddr = cmd.getOptionValue("sdfscli-listen-addr");
		if (!cmd.hasOption("volume-name")) {
			System.out.println("--volume-name and --volume-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		if (!cmd.hasOption("volume-capacity")) {
			System.out.println("--volume-name and --volume-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		volume_name = cmd.getOptionValue("volume-name");
		if (StringUtils.getSpecialCharacterCount(volume_name) > 0) {
			System.out.println("--volume-name cannot contain any special characters");
			System.exit(-1);
		}
		this.perfMonFile = OSValidator.getProgramBasePath() + File.separator + "logs" + File.separator + "volume-"
				+ volume_name + "-perf.json";
		this.volume_capacity = cmd.getOptionValue("volume-capacity");
		base_path = OSValidator.getProgramBasePath() + "volumes" + File.separator + volume_name;
		if (cmd.hasOption("vrts-appliance")) {
			this.vrts_appliance = true;
			this.base_path = "/config/sdfs/" + volume_name;
			this.safe_sync = false;
		}
		if (cmd.hasOption("base-path")) {
			this.base_path = cmd.getOptionValue("base-path");
		}
		if(cmd.hasOption("backup-volume")) {
			this.mdCompresstion = true;
			this.compress = true;
			this.maxSegSize = 128;
			this.max_open_files = 20;
			this.max_file_write_buffers=80;
			this.chunk_size = 40960;
			this.volumeType ="backup";
			this.fdisk_schedule = this.ltrfdisk_schedule;
			this.disableAutoGC = true;
		}
		this.io_log = this.base_path + File.separator + "ioperf.log";
		this.dedup_db_store = this.base_path + File.separator + "ddb";
		this.chunk_store_data_location = this.base_path + File.separator + "chunkstore" + File.separator + "chunks";
		this.chunk_store_hashdb_location = this.base_path + File.separator + "chunkstore" + File.separator + "hdb-"
				+ this.sn;
		if (cmd.hasOption("dedup-db-store")) {
			this.dedup_db_store = cmd.getOptionValue("dedup-db-store");
		}
		if (cmd.hasOption("io-log")) {
			this.io_log = cmd.getOptionValue("io-log");
		}
		if (cmd.hasOption("io-safe-close")) {
			this.safe_close = Boolean.parseBoolean(cmd.getOptionValue("io-safe-close"));
		}
		
		if (cmd.hasOption("io-max-file-write-buffers")) {
			this.max_file_write_buffers = Integer.parseInt(cmd.getOptionValue("io-max-file-write-buffers"));
		} 
		if (cmd.hasOption("hash-type")) {
			String ht = cmd.getOptionValue("hash-type");
			if (ht.equalsIgnoreCase(HashFunctionPool.TIGER_16) || ht.equalsIgnoreCase(HashFunctionPool.TIGER_24)
					|| ht.equalsIgnoreCase(HashFunctionPool.MURMUR3_16)
					|| ht.equalsIgnoreCase(HashFunctionPool.VARIABLE_MURMUR3))
				this.hashType = ht;
			else {
				System.out.println(
						"Invalid Hash Type. Must be " + HashFunctionPool.TIGER_16 + " " + HashFunctionPool.TIGER_24
								+ " " + HashFunctionPool.MURMUR3_16 + " " + HashFunctionPool.VARIABLE_MURMUR3);
				System.exit(-1);
			}
			if (ht.equalsIgnoreCase(HashFunctionPool.VARIABLE_MURMUR3)) {
				this.chunk_store_class = "org.opendedup.sdfs.filestore.BatchFileChunkStore";
				this.compress = true;
			} else if (cmd.hasOption("chunkstore-class")) {
				this.chunk_store_class = cmd.getOptionValue("chunkstore-class");
			} else {
				this.chunk_size = 4;
			}
		}
		if (cmd.hasOption("chunk-store-encrypt")) {
			this.chunk_store_encrypt = Boolean.parseBoolean(cmd.getOptionValue("chunk-store-encrypt"));
			if (this.chunk_store_encrypt)
				this.chunk_store_class = "org.opendedup.sdfs.filestore.BatchFileChunkStore";
		}
		if (cmd.hasOption("chunk-store-encryption-key")) {
			String key = cmd.getOptionValue("chunk-store-encryption-key");
			if (key.length() < 8) {
				System.err.println("Encryption Key must be greater than 8 characters");
				System.exit(-1);
			} else {
				this.chunk_store_encryption_key = cmd.getOptionValue("chunk-store-encryption-key");
			}
		}
		if (cmd.hasOption("chunk-store-iv")) {
			String iv = cmd.getOptionValue("chunk-store-iv");
			this.chunk_store_iv = iv;
		}

		if (cmd.hasOption("ext")) {
			this.ext = true;
			this.hash_db_class = "org.opendedup.collections.RocksDBMap";
			this.chunk_store_class = "org.opendedup.sdfs.filestore.BatchFileChunkStore";
		} else if (cmd.hasOption("noext")) {
			this.ext = false;
			this.hash_db_class = "org.opendedup.collections.RocksDBMap";
			this.hashType = HashFunctionPool.MURMUR3_16;
		}
		
		if (cmd.hasOption("aws-aim"))
			this.awsAim = true;

		if (cmd.hasOption("io-safe-sync")) {
			this.safe_sync = Boolean.parseBoolean(cmd.getOptionValue("io-safe-sync"));
		}
		if(cmd.hasOption("glacier-in-days")) {
			this.glacierInDays = Integer.parseInt(cmd.getOptionValue("glacier-in-days"));
			this.refreshBlobs = true;
			if(cmd.hasOption("glacier-restore-class")) {
				String cln = cmd.getOptionValue("glacier-restore-class");
				if(cln.equalsIgnoreCase("expedited") || cln.equalsIgnoreCase("standard") || cln.equalsIgnoreCase("bulk")) {
					glacierClass = cln;
				}
			}
		}
		if(cmd.hasOption("azurearchive-in-days")) {
			this.aruzreArchiveInDays = Integer.parseInt(cmd.getOptionValue("azurearchive-in-days"));
			this.azurestorageTier = "archive";
			this.refreshBlobs = true;
		}
		
		if(cmd.hasOption("simple-metadata")) {
			this.simpleMD = true;
		}
		if (cmd.hasOption("io-write-threads")) {
			this.write_threads = Short.parseShort(cmd.getOptionValue("io-write-threads"));
		} else if (this.write_threads < 8) {
			this.write_threads = 8;
		}
		if (cmd.hasOption("io-chunk-size")) {
			this.chunk_size = Integer.parseInt(cmd.getOptionValue("io-chunk-size"));
		}
		if (cmd.hasOption("local-cache-size")) {
			this.cacheSize = cmd.getOptionValue("local-cache-size");
			StringUtils.parseSize(this.cacheSize);
		}

		if (cmd.hasOption("io-max-open-files")) {
			this.max_open_files = Integer.parseInt(cmd.getOptionValue("io-max-open-files"));
		}
		if (cmd.hasOption("io-meta-file-cache")) {
			this.meta_file_cache = Integer.parseInt(cmd.getOptionValue("io-meta-file-cache"));
		}
		if (cmd.hasOption("low-memory")) {
			this.lowMemory = true;
		}
		if (cmd.hasOption("io-claim-chunks-schedule")) {
			this.fdisk_schedule = cmd.getOptionValue("io-claim-chunks-schedule");
		}
		if (cmd.hasOption("tcp-keepalive")) {
			this.tcpKeepAlive = Boolean.parseBoolean(cmd.getOptionValue("tcp-keepalive"));
		}
		if (cmd.hasOption("data-appendix")) {
			this.dExt = cmd.getOptionValue("data-appendix");
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
		
		if (cmd.hasOption("compress-metadata")) {
			this.mdCompresstion = true;
		}
		if (cmd.hasOption("chunk-store-data-location")) {
			this.chunk_store_data_location = cmd.getOptionValue("chunk-store-data-location");
		}
		if (cmd.hasOption("chunk-store-hashdb-location")) {
			this.chunk_store_hashdb_location = cmd.getOptionValue("chunk-store-hashdb-location");
		}
		if (cmd.hasOption("chunk-store-hashdb-class")) {
			this.hash_db_class = cmd.getOptionValue("chunk-store-hashdb-class");
		}

		if (cmd.hasOption("aws-enabled")) {
			this.awsEnabled = Boolean.parseBoolean(cmd.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("minio-enabled")) {
			this.safe_sync = false;
			this.minIOEnabled = true;
			this.simpleMD = true;
		}
		if (cmd.hasOption("ali-enabled")) {
			this.safe_sync = false;
			this.aliEnabled = true;
			this.simpleMD = true;
		}
		if (cmd.hasOption("atmos-enabled")) {

			this.safe_sync = false;
			this.atmosEnabled = true;
			if (!cmd.hasOption("cloud-url")) {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-url, cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
			} else {

			}
		}
		if (cmd.hasOption("backblaze-enabled")) {
			this.backblazeEnabled = true;
			
			
		}
		if (cmd.hasOption("google-enabled")) {
			this.gsEnabled = Boolean.parseBoolean(cmd.getOptionValue("google-enabled"));
		}
		if (cmd.hasOption("aws-bucket-location")) {
			this.bucketLocation = cmd.getOptionValue("aws-bucket-location");
		}
		if (cmd.hasOption("azure-enabled")) {
			this.azureEnabled = Boolean.parseBoolean(cmd.getOptionValue("azure-enabled"));
		}
		if (cmd.hasOption("gc-class")) {
			this.gc_class = cmd.getOptionValue("gc-class");
		}
		if(cmd.hasOption("user-agent-prefix")) {
			this.userAgentPrefix = cmd.getOptionValue("user-agent-prefix");
		}
		if (this.awsEnabled || minIOEnabled || this.aliEnabled) {
			if (awsAim || (cmd.hasOption("cloud-secret-key") && cmd.hasOption("cloud-access-key"))
					&& cmd.hasOption("cloud-bucket-name")) {
				if ((this.minIOEnabled|| this.aliEnabled) && !cmd.hasOption("cloud-url")) {
					System.out.println("Error : Unable to create volume");
					System.out.println(
							"cloud-url, cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				}
				if (!awsAim) {
					this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
					this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				}
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.compress = true;
				this.readAhead = true;
				
				if (cmd.hasOption("simple-s3")) {
					this.simpleS3 = true;
					this.usebasicsigner = true;
				}
				/*
				if (!this.aliEnabled && !minIOEnabled && !awsAim && !cmd.hasOption("cloud-disable-test")
						&& !BatchAwsS3ChunkStore.checkAuth(cloudAccessKey, cloudSecretKey)) {
					System.out.println("Error : Unable to create volume");
					System.out.println("cloud-access-key or cloud-secret-key is incorrect");
					System.exit(-1);
				}
				*/
				
			} else {
				System.out.println("Error : Unable to create volume");
				System.out.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.out.println(cmd.getOptionValue("cloud-access-key"));
				System.out.println(cmd.getOptionValue("cloud-secret-key"));
				System.out.println(cmd.getOptionValue("cloud-bucket-name"));
				System.exit(-1);
			}
		} else if (this.gsEnabled || this.atmosEnabled || this.backblazeEnabled) {
			if (cmd.hasOption("cloud-secret-key") && cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.compress = true;
				this.readAhead = true;
				
			} else {
				System.out.println("Error : Unable to create volume");
				System.out.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
		}

		else if (this.azureEnabled) {
			if (cmd.hasOption("cloud-secret-key") && cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				this.readAhead = true;
				this.compress = true;
				
			} else {
				System.out.println("Error : Unable to create volume");
				System.out.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
		}
		if (cmd.hasOption("chunk-store-io-threads")) {
			this.cloudThreads = Integer.parseInt(cmd.getOptionValue("chunk-store-io-threads"));
		}
		if (cmd.hasOption("chunk-store-compress")) {
			this.compress = Boolean.parseBoolean(cmd.getOptionValue("chunk-store-compress"));
			if (this.compress && !this.awsEnabled && !this.gsEnabled && this.azureEnabled) {
				this.chunk_store_class = "org.opendedup.sdfs.filestore.BatchFileChunkStore";
			}
		}
		if (cmd.hasOption("volume-maximum-full-percentage")) {
			this.max_percent_full = Double.parseDouble(cmd.getOptionValue("volume-maximum-full-percentage")) / 100;
		}
		if (cmd.hasOption("chunk-store-size")) {
			this.chunk_store_allocation_size = StringUtils.parseSize(cmd.getOptionValue("chunk-store-size"));
		} else {
			this.chunk_store_allocation_size = StringUtils.parseSize(this.volume_capacity);
		}
		if (cmd.hasOption("cloud-url")) {
			this.cloudUrl = cmd.getOptionValue("cloud-url");
			this.genericS3 = true;
			this.simpleS3 = true;
			if(!this.aliEnabled)
				this.disableDNSBucket = true;
			if (!this.minIOEnabled) {
				this.usebasicsigner = true;
			}
		}
		if (cmd.hasOption("aws-basic-signer")) {
			this.usebasicsigner = Boolean.parseBoolean(cmd.getOptionValue("aws-basic-signer"));
		}
		if (cmd.hasOption("aws-disable-dns-bucket")) {
			this.disableDNSBucket = Boolean.parseBoolean(cmd.getOptionValue("aws-disable-dns-bucket"));
		}
		if (cmd.hasOption("cluster-dse-password"))
			this.clusterDSEPassword = cmd.getOptionValue("cluster-dse-password");
		if (cmd.hasOption("cluster-id")) {
			this.clusterID = cmd.getOptionValue("cluster-id");
			if (StringUtils.getSpecialCharacterCount(this.clusterID) > 0) {
				System.out.println("--cluster-id cannot contain any special characters");
				System.exit(-1);
			}

		}
		if(cmd.hasOption("cloud-backlog-size")) {
			this.backlogSize = cmd.getOptionValue("cloud-backlog-size");
		}
		if (cmd.hasOption("cluster-config"))
			this.clusterConfig = cmd.getOptionValue("cluster-config");
		if (cmd.hasOption("cluster-block-replicas")) {
			this.clusterCopies = Byte.parseByte(cmd.getOptionValue("cluster-block-replicas"));
			if (this.clusterCopies > 7)
				System.err.println("You can only specify up to 7 replica copies of unique blocks");
		}
		if (cmd.hasOption("cluster-rack-aware"))
			this.clusterRackAware = Boolean.parseBoolean(cmd.getOptionValue("cluster-rack-aware"));
		if (cmd.hasOption("enable-replication-master")) {
			this.sdfsCliRequireAuth = true;
			this.sdfsCliListenAddr = "::";
			this.sdfsCliEnabled = true;
		}
		if(cmd.hasOption("sdfscli-disable-ssl")) {
			this.sdfsCliSSL = false;
		}
		

		File file = new File(OSValidator.getConfigPath() + this.volume_name.trim() + "-volume-cfg.xml");
		if (file.exists()) {
			throw new IOException("Volume [" + this.volume_name + "] already exists");
		}
		if (cmd.hasOption("report-dse-size")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd.getOptionValue("report-dse-size"));
				// this.useDSECapacity = rp;
				this.useDSESize = rp;
			} catch (Throwable e) {
				System.err.println("value for report-dse-size must be true or false");
			}
		}
		if (cmd.hasOption("report-dse-capacity")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd.getOptionValue("report-dse-capacity"));
				this.useDSECapacity = rp;
				// this.useDSESize = rp;
			} catch (Throwable e) {
				System.err.println("value for report-dse-capacity must be true or false");
			}
		}
		if (cmd.hasOption("use-perf-mon")) {
			try {
				Boolean rp = Boolean.parseBoolean(cmd.getOptionValue("use-perf-mon"));
				this.usePerfMon = rp;
			} catch (Throwable e) {
				System.err.println("value for use-perf-mon must be true or false");
			}
		}
	}

	public void writeConfigFile() throws ParserConfigurationException, IOException {
		File dir = new File(OSValidator.getConfigPath());
		if (!dir.exists()) {
			System.out.println("making" + dir.getAbsolutePath());
			dir.mkdirs();
		}
		File file = new File(OSValidator.getConfigPath() + this.volume_name.trim() + "-volume-cfg.xml");
		if(this.encryptConfig) {
			System.out.println("Encrypting Configuration");
			String password = this.sdfsCliPassword;
			String iv = this.chunk_store_iv;
			byte [] ec = EncryptUtils.encryptCBC(this.chunk_store_encryption_key.getBytes(), password, iv);
			this.chunk_store_encryption_key = BaseEncoding.base64Url().encode(ec);
			ec = EncryptUtils.encryptCBC(this.cloudSecretKey.getBytes(), password, iv);
			this.cloudSecretKey = BaseEncoding.base64Url().encode(ec);
		}

		if (vrts_appliance) {
			dir = new File("/config/sdfs/etc/");
			if (!dir.exists()) {
				System.out.println("making" + dir.getAbsolutePath());
				dir.mkdirs();
			}
			File vdir = new File("/opendedupe/volumes/" + this.volume_name.trim());
			if (!vdir.exists()) {
				System.out.println("making mountpoint" + vdir.getAbsolutePath());
				vdir.mkdirs();
			}
			file = new File(dir.getPath() + File.separator + this.volume_name.trim() + "-volume-cfg.xml");

		}
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
		io.setAttribute("chunk-size", Integer.toString(this.chunk_size));

		io.setAttribute("dedup-files", Boolean.toString(this.dedup_files));
		if (OSValidator.isWindows())
			io.setAttribute("max-file-inactive", "0");
		else
			io.setAttribute("max-file-inactive", "900");
		io.setAttribute("max-file-write-buffers", Long.toString(this.max_file_write_buffers));
		io.setAttribute("max-open-files", Integer.toString(this.max_open_files));
		io.setAttribute("meta-file-cache", Integer.toString(this.meta_file_cache));
		io.setAttribute("safe-close", Boolean.toString(this.safe_close));
		io.setAttribute("safe-sync", Boolean.toString(this.safe_sync));
		io.setAttribute("write-threads", Integer.toString(this.write_threads));
		io.setAttribute("claim-hash-schedule", this.fdisk_schedule);
		io.setAttribute("read-ahead", Boolean.toString(this.readAhead));
		io.setAttribute("hash-type", this.hashType);
		io.setAttribute("max-variable-segment-size", Integer.toString(this.maxSegSize));
		io.setAttribute("variable-window-size", Integer.toString(this.windowSize));
		io.setAttribute("volume-type", this.volumeType);
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
		vol.setAttribute("maximum-percentage-full", Double.toString(this.max_percent_full));
		vol.setAttribute("closed-gracefully", "true");
		vol.setAttribute("use-dse-capacity", Boolean.toString(this.useDSECapacity));
		vol.setAttribute("use-dse-size", Boolean.toString(this.useDSESize));
		vol.setAttribute("use-perf-mon", Boolean.toString(this.usePerfMon));
		vol.setAttribute("perf-mon-file", this.perfMonFile);
		vol.setAttribute("cluster-id", this.clusterID);
		vol.setAttribute("cluster-block-copies", Byte.toString(clusterCopies));
		vol.setAttribute("cluster-response-timeout", Integer.toString(chunk_size * 1000));
		vol.setAttribute("cluster-rack-aware", Boolean.toString(clusterRackAware));
		vol.setAttribute("read-timeout-seconds", Integer.toString(this.read_timeout));
		vol.setAttribute("write-timeout-seconds", Integer.toString(this.write_timeout));
		vol.setAttribute("serial-number", Long.toString(sn));
		vol.setAttribute("compress-metadata", Boolean.toString(this.mdCompresstion));
		root.appendChild(vol);
		Element cs = xmldoc.createElement("local-chunkstore");
		cs.setAttribute("low-memory", Boolean.toString(this.lowMemory));
		cs.setAttribute("average-chunk-size", Integer.toString(this.avgPgSz));
		cs.setAttribute("allocation-size", Long.toString(this.chunk_store_allocation_size));
		cs.setAttribute("gc-class", this.gc_class);
		cs.setAttribute("chunk-store", this.chunk_store_data_location);
		cs.setAttribute("fpp", ".001");
		cs.setAttribute("disable-auto-gc", Boolean.toString(this.disableAutoGC));
		cs.setAttribute("encrypt", Boolean.toString(this.chunk_store_encrypt));
		cs.setAttribute("encryption-key", this.chunk_store_encryption_key);
		cs.setAttribute("encryption-iv", this.chunk_store_iv);
		cs.setAttribute("max-repl-batch-sz", Integer.toString(Main.MAX_REPL_BATCH_SZ));
		cs.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		cs.setAttribute("chunkstore-class", this.chunk_store_class);
		cs.setAttribute("hashdb-class", this.hash_db_class);
		cs.setAttribute("cluster-id", this.clusterID);
		cs.setAttribute("cluster-config", this.clusterConfig);
		cs.setAttribute("cluster-dse-password", this.clusterDSEPassword);
		cs.setAttribute("io-threads", Integer.toString(this.cloudThreads));
		cs.setAttribute("compress", Boolean.toString(this.compress));
		
		Element sdfscli = xmldoc.createElement("sdfscli");
		sdfscli.setAttribute("enable-auth", Boolean.toString(this.sdfsCliRequireAuth));
		sdfscli.setAttribute("listen-address", this.sdfsCliListenAddr);
		sdfscli.setAttribute("use-ssl", Boolean.toString(this.sdfsCliSSL));
		try {
			sdfscli.setAttribute("password",
					HashFunctions.getSHAHash(this.sdfsCliPassword.getBytes(), this.sdfsCliSalt.getBytes()));
		} catch (Exception e) {
			System.out.println("unable to create password ");
			e.printStackTrace();
			throw new IOException(e);
		}
		sdfscli.setAttribute("salt", this.sdfsCliSalt);
		sdfscli.setAttribute("port", Integer.toString(this.sdfsCliPort));
		sdfscli.setAttribute("enable", Boolean.toString(this.sdfsCliEnabled));
		

		root.appendChild(sdfscli);
		if (this.atmosEnabled || this.backblazeEnabled) {

			Element aws = xmldoc.createElement("file-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("bucket-name", this.cloudBucketName);
			aws.setAttribute("access-key", this.cloudAccessKey);
			aws.setAttribute("secret-key", this.cloudSecretKey);
			aws.setAttribute("chunkstore-class", "org.opendedup.sdfs.filestore.cloud.BatchJCloudChunkStore");
			Element extended = xmldoc.createElement("extended-config");
			
			if (this.atmosEnabled) {
				extended.setAttribute("service-type", "atmos");
				Element cp = xmldoc.createElement("connection-props");
				cp.setAttribute("jclouds.endpoint", this.cloudUrl);
				extended.appendChild(cp);
			}
			if (this.backblazeEnabled)
				extended.setAttribute("service-type", "b2");
			extended.setAttribute("block-size", this.blockSize);
			if (!this.tcpKeepAlive)
				extended.setAttribute("tcp-keepalive", "false");
			if (this.dExt != null)
				 extended.setAttribute("data-appendix", this.dExt);
			extended.setAttribute("allow-sync", "false");
			extended.setAttribute("upload-thread-sleep-time", "300000");
			extended.setAttribute("sync-files", "true");
			if(this.userAgentPrefix != null)
				extended.setAttribute("user-agent-prefix", this.userAgentPrefix);
			extended.setAttribute("local-cache-size", this.cacheSize);
			extended.setAttribute("map-cache-size", "200");
			extended.setAttribute("io-threads", "16");
			extended.setAttribute("delete-unclaimed", "true");
			extended.setAttribute("sync-check-schedule", syncfs_schedule);
			extended.setAttribute("backlog-size", this.backlogSize);
			cs.appendChild(extended);

			cs.appendChild(aws);
		}

		else if (this.awsEnabled || this.minIOEnabled || this.aliEnabled) {
			Element aws = xmldoc.createElement("aws");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("aws-aim", Boolean.toString(this.awsAim));
			if (!awsAim) {
				aws.setAttribute("aws-access-key", this.cloudAccessKey);
				aws.setAttribute("aws-secret-key", this.cloudSecretKey);
			}
			aws.setAttribute("aws-bucket-name", this.cloudBucketName);
			if (ext) {
				
				if(this.aliEnabled)
					aws.setAttribute("chunkstore-class", "org.opendedup.sdfs.filestore.cloud.BatchAliChunkStore");
				else
					aws.setAttribute("chunkstore-class", "org.opendedup.sdfs.filestore.cloud.BatchAwsS3ChunkStore");
				
				Element extended = xmldoc.createElement("extended-config");
				extended.setAttribute("block-size", this.blockSize);
				if (this.dExt != null)
					 extended.setAttribute("data-appendix", this.dExt);
				if (!this.tcpKeepAlive)
					extended.setAttribute("tcp-keepalive", "false");
				extended.setAttribute("allow-sync", "false");
				extended.setAttribute("upload-thread-sleep-time", "300000");
				extended.setAttribute("sync-files", "true");
				if(this.userAgentPrefix != null)
					extended.setAttribute("user-agent-prefix", this.userAgentPrefix);
				extended.setAttribute("local-cache-size", this.cacheSize);
				extended.setAttribute("map-cache-size", "200");
				extended.setAttribute("io-threads", "16");
				extended.setAttribute("delete-unclaimed", "true");
				extended.setAttribute("refresh-blobs", Boolean.toString(this.refreshBlobs));
				extended.setAttribute("glacier-archive-days", Integer.toString(this.glacierInDays));
				extended.setAttribute("glacier-tier", this.glacierClass);
				extended.setAttribute("simple-metadata", Boolean.toString(this.simpleMD));
				extended.setAttribute("sync-check-schedule", syncfs_schedule);
				extended.setAttribute("use-basic-signer", Boolean.toString(this.usebasicsigner));
				extended.setAttribute("backlog-size", this.backlogSize);
				if (this.genericS3 || this.aliEnabled) {
					Element cp = xmldoc.createElement("connection-props");
					cp.setAttribute("s3-target", this.cloudUrl);
					extended.setAttribute("disableDNSBucket", Boolean.toString(this.disableDNSBucket));

					extended.appendChild(cp);
				}
				if (this.simpleS3)
					extended.setAttribute("simple-s3", "true");
				else
					extended.setAttribute("simple-s3", "false");
				if (this.basicS3Signer)
					extended.setAttribute("use-basic-signer", "true");
				if (this.minIOEnabled) {
					extended.setAttribute("use-v4-signer", "true");
				}
				if (this.bucketLocation != null)
					extended.setAttribute("default-bucket-location", this.bucketLocation);
				cs.appendChild(extended);
			} else if (bucketLocation != null) {
				Element extended = xmldoc.createElement("extended-config");
				extended.setAttribute("default-bucket-location", this.bucketLocation);
				cs.appendChild(extended);
			}
			cs.appendChild(aws);
		} else if (this.gsEnabled) {
			Element aws = xmldoc.createElement("google-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("gs-access-key", this.cloudAccessKey);
			aws.setAttribute("gs-secret-key", this.cloudSecretKey);
			aws.setAttribute("gs-bucket-name", this.cloudBucketName);
			if (ext) {
				

				aws.setAttribute("chunkstore-class", "org.opendedup.sdfs.filestore.cloud.BatchJCloudChunkStore");
				Element extended = xmldoc.createElement("extended-config");
				extended.setAttribute("service-type", "google-cloud-storage");
				extended.setAttribute("block-size", this.blockSize);
				if (this.dExt != null)
					 extended.setAttribute("data-appendix", this.dExt);
				if (!this.tcpKeepAlive)
					extended.setAttribute("tcp-keepalive", "false");
				extended.setAttribute("allow-sync", "false");
				extended.setAttribute("upload-thread-sleep-time", "300000");
				extended.setAttribute("sync-files", "true");
				if(this.userAgentPrefix != null)
					extended.setAttribute("user-agent-prefix", this.userAgentPrefix);
				extended.setAttribute("local-cache-size", this.cacheSize);
				extended.setAttribute("map-cache-size", "100");
				extended.setAttribute("io-threads", "16");
				extended.setAttribute("delete-unclaimed", "true");
				extended.setAttribute("sync-check-schedule", syncfs_schedule);
				extended.setAttribute("backlog-size", this.backlogSize);
				if (this.bucketLocation != null)
					extended.setAttribute("default-bucket-location", this.bucketLocation);
				cs.appendChild(extended);
			} else if (bucketLocation != null) {
				Element extended = xmldoc.createElement("extended-config");
				extended.setAttribute("default-bucket-location", this.bucketLocation);
				cs.appendChild(extended);
			}
			cs.appendChild(aws);
		} else if (this.azureEnabled) {
			Element aws = xmldoc.createElement("azure-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("azure-access-key", this.cloudAccessKey);
			aws.setAttribute("azure-secret-key", this.cloudSecretKey);
			aws.setAttribute("azure-bucket-name", this.cloudBucketName);
			if (ext) {
				aws.setAttribute("chunkstore-class", "org.opendedup.sdfs.filestore.cloud.BatchAzureChunkStore");
				Element extended = xmldoc.createElement("extended-config");
				extended.setAttribute("service-type", "azureblob");
				extended.setAttribute("block-size", this.blockSize);
				extended.setAttribute("allow-sync", "false");
				extended.setAttribute("upload-thread-sleep-time", "300000");
				if(this.userAgentPrefix != null)
					extended.setAttribute("user-agent-prefix", this.userAgentPrefix);
				extended.setAttribute("sync-files", "true");
				extended.setAttribute("local-cache-size", this.cacheSize);
				extended.setAttribute("map-cache-size", "100");
				extended.setAttribute("io-threads", "16");
				extended.setAttribute("delete-unclaimed", "true");
				extended.setAttribute("sync-check-schedule", syncfs_schedule);
				extended.setAttribute("azure-tier-in-days", Integer.toString(aruzreArchiveInDays));
				extended.setAttribute("backlog-size", this.backlogSize);
				if(this.azurestorageTier != null) {
					extended.setAttribute("storage-tier", this.azurestorageTier);
				}
				cs.appendChild(extended);
			}
			cs.appendChild(aws);
		} else if (ext) {
			Element extended = xmldoc.createElement("extended-config");
			extended.setAttribute("block-size", "60 MB");
			if (this.dExt != null)
				 extended.setAttribute("data-appendix", this.dExt);
			if (!this.tcpKeepAlive)
				extended.setAttribute("tcp-keepalive", "false");
			extended.setAttribute("allow-sync", "false");
			extended.setAttribute("upload-thread-sleep-time", "1500");
			extended.setAttribute("sync-files", "false");
			if(this.userAgentPrefix != null)
				extended.setAttribute("user-agent-prefix", this.userAgentPrefix);
			extended.setAttribute("local-cache-size", this.cacheSize);
			extended.setAttribute("map-cache-size", "100");
			extended.setAttribute("io-threads", "16");
			extended.setAttribute("delete-unclaimed", "true");
			cs.appendChild(extended);
		}
		root.appendChild(cs);
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(xmldoc);

			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance().newTransformer();
			xformer.setOutputProperty(OutputKeys.INDENT, "yes");
			xformer.transform(source, result);
			File lf = new File(OSValidator.getConfigPath() + this.volume_name.trim() + "-volume-cfg.xml");
			if (this.vrts_appliance) {
				Path srcP = Paths.get(file.getPath());
				Path dstP = Paths.get(lf.getPath());
				Files.createSymbolicLink(dstP, srcP);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	public void writeGCConfigFile() throws ParserConfigurationException, IOException {
		File dir = new File(OSValidator.getConfigPath());
		if (!dir.exists()) {
			System.out.println("making" + dir.getAbsolutePath());
			dir.mkdirs();
		}
		File file = new File(OSValidator.getConfigPath() + this.clusterID + "-gc-cfg.xml");
		File vlf = new File(OSValidator.getConfigPath() + this.clusterID + "-volume-list.xml");
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
			Transformer xformer = TransformerFactory.newInstance().newTransformer();
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
		options.addOption(
				OptionBuilder.withLongOpt("help").withDescription("Display these options.").hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("vrts-appliance")
				.withDescription("Volume is running on a NetBackup Appliance.").hasArg(true).hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("sdfscli-password")
				.withDescription(
						"The password used to authenticate to the sdfscli management interface. Thee default password is \"admin\".")
				.hasArg(true).withArgName("password").create());
		options.addOption(OptionBuilder.withLongOpt("aws-bucket-location")
				.withDescription("The aws location for this bucket").hasArg(true).withArgName("aws location").create());
		options.addOption(OptionBuilder.withLongOpt("sdfscli-require-auth")
				.withDescription("Require authentication to connect to the sdfscli managment interface").hasArg(false)
				.create());
		options.addOption(OptionBuilder.withLongOpt("sdfscli-disable-ssl")
				.withDescription("disables ssl to management interface").hasArg(false)
				.create());
		options.addOption(OptionBuilder.withLongOpt("sdfscli-listen-port")
				.withDescription("TCP/IP Listenting port for the sdfscli management interface").hasArg(true)
				.withArgName("tcp port").create());
		options.addOption(OptionBuilder.withLongOpt("glacier-in-days")
				.withDescription("Set to move to glacier from s3 after x number of days").hasArg(true)
				.withArgName("number of days e.g. 30").create());
		options.addOption(OptionBuilder.withLongOpt("azurearchive-in-days")
				.withDescription("Set to move to azure archive from hot after x number of days").hasArg(true)
				.withArgName("number of days e.g. 30").create());
		options.addOption(OptionBuilder.withLongOpt("refresh-blobs")
				.withDescription("Updates blobs in s3 to keep them from moving to glacier if clamined by newly written files").hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("sdfscli-listen-addr")
				.withDescription(
						"IP Listenting address for the sdfscli management interface. This defaults to \"localhost\"")
				.hasArg(true).withArgName("ip address or host name").create());
		options.addOption(
				OptionBuilder.withLongOpt("base-path")
						.withDescription("the folder path for all volume data and meta data.\n Defaults to: \n "
								+ OSValidator.getProgramBasePath() + "<volume name>")
						.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("cloud-url")
				.withDescription("The url of the blob server. e.g. http://s3server.localdomain/s3/").hasArg()
				.withArgName("url").create());
		options.addOption(OptionBuilder.withLongOpt("aws-basic-signer")
				.withDescription(
						"use basic s3 signer for the cloud connection. This is set to true by default for all cloud url buckets")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("aws-disable-dns-bucket")
				.withDescription(
						"disable the use of dns bucket names to prepent the cloud url. This is set to true by default when cloud-url is set")
				.hasArg().withArgName("true|false").create());
		options.addOption(
				OptionBuilder.withLongOpt("base-path")
						.withDescription("the folder path for all volume data and meta data.\n Defaults to: \n "
								+ OSValidator.getProgramBasePath() + "<volume name>")
						.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("gc-class")
				.withDescription(
						"The class used for intelligent block garbage collection.\n Defaults to: \n " + Main.gcClass)
				.hasArg().withArgName("CLASS NAME").create());
		options.addOption(OptionBuilder.withLongOpt("compress-metadata")
				.withDescription(
						"Enable compression of metadata at the expense of speed to open and close files. This option should be enabled for backup")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("simple-metadata")
				.withDescription(
						"If set, will create a separate object for metadata used for objects sent to the cloud. Otherwise, metadata will be stored as attributes to the object.")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("dedup-db-store")
				.withDescription(
						"the folder path to location for the dedup file database.\n Defaults to: \n --base-path + "
								+ File.separator + "ddb")
				.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("io-log")
				.withDescription("the file path to location for the io log.\n Defaults to: \n --base-path + "
						+ File.separator + "sdfs.log")
				.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("cloud-disable-test")
				.withDescription("Disables testing authentication for s3").create());
		options.addOption(OptionBuilder.withLongOpt("io-safe-close")
				.withDescription(
						"If true all files will be closed on filesystem close call. Otherwise, files will be closed"
								+ " based on inactivity. Set this to false if you plan on sharing the file system over"
								+ " an nfs share. True takes less RAM than False. \n Defaults to: \n true")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("io-safe-sync")
				.withDescription(
						"If true all files will sync locally on filesystem sync call. Otherwise, by defaule (false), files will sync"
								+ " on close and data will per written to disk based on --max-file-write-buffers.  "
								+ "Setting this to true will ensure that no data loss will occur if the system is turned off abrubtly"
								+ " at the cost of slower speed. \n Defaults to: \n false")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("io-write-threads")
				.withDescription(
						"The number of threads that can be used to process data writted to the file system. \n Defaults to: \n 16")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder.withLongOpt("io-dedup-files")
				.withDescription(
						"True mean that all files will be deduped inline by default. This can be changed on a one off"
								+ "basis by using the command \"setfattr -n user.cmd.dedupAll -v 556:false <path to file on sdfs volume>\"\n Defaults to: \n true")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("io-chunk-size")
				.withDescription(
						"The unit size, in kB, of chunks stored. Set this to 4 if you would like to dedup VMDK files inline.\n Defaults to: \n 4")
				.hasArg().withArgName("SIZE in kB").create());
		options.addOption(OptionBuilder.withLongOpt("io-max-file-write-buffers")
				.withDescription(
						"The amount of memory to have available for reading and writing per file. Each buffer in the size"
								+ " of io-chunk-size. \n Defaults to: \n 24")
				.hasArg().withArgName("SIZE in MB").create());
		options.addOption(OptionBuilder.withLongOpt("io-max-open-files")
				.withDescription("The maximum number of files that can be open at any one time. "
						+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder.withLongOpt("local-cache-size")
				.withDescription(
						"The local read cache size for data uploaded to the cloud. " + "\n Defaults to: \n 10 GB")
				.hasArg().withArgName("Size + [MB,GB,TB]").create());
		options.addOption(OptionBuilder.withLongOpt("io-meta-file-cache")
				.withDescription("The maximum number metadata files to be cached at any one time. "
						+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder.withLongOpt("io-claim-chunks-schedule")
				.withDescription("The schedule, in cron format, to claim deduped chunks with the Volume(s). "
						+ " \n Defaults to: \n 0 59 23 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder.withLongOpt("data-appendix")
				.withDescription(
						"Add an appendix for data files.")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("tcp-keepalive")
				.withDescription(
						"Set tcp-keepalive setting for the connection with S3 storage")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-file")
				.withDescription("Default File Permissions. " + " \n Defaults to: \n 0644").hasArg()
				.withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-folder")
				.withDescription("Default Folder Permissions. " + " \n Defaults to: \n 0755").hasArg()
				.withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-owner")
				.withDescription("Default Owner. " + " \n Defaults to: \n 0").hasArg().withArgName("POSIX PERMISSIONS")
				.create());
		options.addOption(OptionBuilder.withLongOpt("permissions-group")
				.withDescription("Default Group. " + " \n Defaults to: \n 0").hasArg().withArgName("POSIX PERMISSIONS")
				.create());
		options.addOption(OptionBuilder.withLongOpt("volume-capacity")
				.withDescription("Capacity of the volume in [MB|GB|TB]. " + " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("SIZE [MB|GB|TB]").create());
		options.addOption(OptionBuilder.withLongOpt("volume-name")
				.withDescription("The name of the volume. " + " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("STRING").create());
		options.addOption(OptionBuilder.withLongOpt("volume-maximum-full-percentage")
				.withDescription(
						"The maximum percentage of the volume capacity, as set by volume-capacity, before the volume starts"
								+ "reporting that the disk is full. If the number is negative then it will be infinite. This defaults to 95 "
								+ " \n e.g. --volume-maximum-full-percentage=95")
				.hasArg().withArgName("PERCENTAGE").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-data-location")
				.withDescription("The directory where chunks will be stored." + " \nDefaults to: \n --base-path + "
						+ File.separator + "chunkstore" + File.separator + "chunks")
				.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-hashdb-location")
				.withDescription("The directory where hash database for chunk locations will be stored."
						+ " \nDefaults to: \n --base-path + " + File.separator + "chunkstore" + File.separator + "hdb")
				.hasArg().withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("chunkstore-class")
				.withDescription(
						"The class for the specific chunk store to be used. \n Defaults to org.opendedup.sdfs.filestore.FileChunkStore")
				.hasArg().withArgName("Class Name").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-gc-schedule")
				.withDescription(
						"The schedule, in cron format, to check for unclaimed chunks within the Dedup Storage Engine. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 0 0 0/2 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-hashdb-class")
				.withDescription("The class used to store hash values \n Defaults to: \n " + Main.hashesDBClass)
				.hasArg().withArgName("class name").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-size")
				.withDescription("The size in MB,TB,GB of the Dedup Storeage Engine. "
						+ "This . \n Defaults to: \n The size of the Volume")
				.hasArg().withArgName("MB|GB|TB").create());
		options.addOption(OptionBuilder.withLongOpt("hash-type")
				.withDescription(
						"This is the type of hash engine used to calculate a unique hash. The valid options for hash-type are "
								+ HashFunctionPool.TIGER_16 + " " + HashFunctionPool.TIGER_24 + " "
								+ HashFunctionPool.MURMUR3_16 + " " + HashFunctionPool.VARIABLE_MURMUR3
								+ " This Defaults to " + HashFunctionPool.VARIABLE_MURMUR3)
				.hasArg().withArgName(HashFunctionPool.TIGER_16 + "|" + HashFunctionPool.TIGER_24 + "|"
						+ HashFunctionPool.MURMUR3_16 + "|" + HashFunctionPool.VARIABLE_MURMUR3)
				.create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-encrypt")
				.withDescription(
						"Whether or not to Encrypt chunks within the Dedup Storage Engine. The encryption key is generated automatically."
								+ " For AWS this is a good option to enable. The default for this is" + " false")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-encryption-key")
				.withDescription(
						"The encryption key used for encrypting data. If not specified a strong key will be generated automatically. They key must be at least 8 charaters long")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-iv")
				.withDescription(
						"The encryption  initialization vector (IV) used for encrypting data. If not specified a strong key will be generated automatically")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("encrypt-config")
				.withDescription(
						"Encrypt security sensitive encryption parameters with the admin password")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("aws-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Amazon S3 Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("minio-enabled")
				.withDescription(
						"Set to enable this volume to store to Minio Object Storage. cloud-url, cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("ali-enabled")
				.withDescription(
						"Set to enable this volume to store to Alibaba Object Storage (OSS). cloud-url, cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("atmos-enabled")
				.withDescription(
						"Set to enable this volume to store to Atmo Object Storage. cloud-url, cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("backblaze-enabled")
				.withDescription(
						"Set to enable this volume to store to Backblaze Object Storage. cloud-url, cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("cloud-secret-key")
				.withDescription("Set to the value of Cloud Storage secret key.").hasArg()
				.withArgName("Cloud Secret Key").create());
		options.addOption(OptionBuilder.withLongOpt("cloud-access-key")
				.withDescription("Set to the value of Cloud Storage access key.").hasArg()
				.withArgName("Cloud Access Key").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-io-threads")
				.withDescription(
						"Sets the number of io threads to use for io operations to the dse storage provider. This is set to 8 by default but can be changed to more or less based on bandwidth and io.")
				.hasArg().withArgName("integer").create());
		options.addOption(OptionBuilder.withLongOpt("cloud-bucket-name")
				.withDescription(
						"Set to the value of Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. aws-enabled, aws-secret-key, and aws-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique Cloud Bucket Name").create());
		options.addOption(OptionBuilder.withLongOpt("user-agent-prefix")
				.withDescription(
						"Set the user agent prefix for the client when uploading to the cloud.")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("low-memory")
				.withDescription("Sets the volume to mimimize the amount of ram used at the expense of speed")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-compress")
				.withDescription(
						"Compress chunks before they are stored. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("google-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Google Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("backup-volume")
				.withDescription(
						"When set, changed the volume attributes for better deduplication but slower randnom IO.")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("azure-enabled")
				.withDescription(
						"Set to true to enable this volume to store to Microsoft Azure Cloud Storage. cloud-secret-key, cloud-access-key, and cloud-bucket-name will also need to be set. ")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("glacier-restore-class")
				.withDescription(
						"Set the class used to restore glacier data. ")
				.hasArg().withArgName("expedited|standard|bulk").create());
		
		options.addOption(OptionBuilder.withLongOpt("aws-aim")
				.withDescription("Use aim authentication for access to AWS S3").create());
		
		options.addOption(OptionBuilder.withLongOpt("enable-replication-master")
				.withDescription("Enable this volume as a replication master").create());
		options.addOption(OptionBuilder.withLongOpt("simple-s3")
				.withDescription("Uses basic S3 api characteristics for cloud storage backend.").create());
		options.addOption(OptionBuilder.withLongOpt("report-dse-size")
				.withDescription("If set to \"true\" this volume will used as the actual"
						+ " used statistics from the DSE. If this value is set to \"false\" it will"
						+ "report as virtual size of the volume and files. Defaults to \"true\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("report-dse-capacity")
				.withDescription("If set to \"true\" this volume will report capacity the actual"
						+ "capacity statistics from the DSE. If this value is set to \"false\" it will"
						+ "report as virtual size of the volume and files. Defaults to \"true\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("use-perf-mon")
				.withDescription(
						"If set to \"true\" this volume will log io statistics to /etc/sdfs/ directory. Defaults to \"false\"")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("cluster-id")
				.withDescription(
						"The name used to identify the cluster group. This defaults to sdfs-cluster. This name should be the same on all members of this cluster")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("cluster-config")
				.withDescription(
						"The jgroups configuration used to configure this cluster node. This defaults to \"/etc/sdfs/jgroups.cfg.xml\". ")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("cluster-dse-password")
				.withDescription(
						"The jgroups configuration used to configure this cluster node. This defaults to \"/etc/sdfs/jgroups.cfg.xml\". ")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder.withLongOpt("cluster-block-replicas")
				.withDescription(
						"The number copies to distribute to descrete nodes for each unique block. As an example if this value is set to"
								+ "\"3\" the volume will attempt to write any unique block to \"3\" DSE nodes, if available.  This defaults to \"2\". ")
				.hasArg().withArgName("Value [1-7]").create());
		options.addOption(OptionBuilder.withLongOpt("cloud-backlog-size")
				.withDescription(
						"The how much data can live in the spool for backlog. Setting to -1 makes the backlog unlimited. Setting to 0 (default) sets no backlog. Setting to <value> GB TB MB caps the backlog.")
				.hasArg().withArgName("Value 0,-1,[TB GB MB]").create());
		options.addOption(OptionBuilder.withLongOpt("cluster-rack-aware")
				.withDescription(
						"If set to true, the clustered volume will be rack aware and make the best effort to distribute blocks to multiple racks"
								+ " based on the cluster-block-replicas. As an example, if cluster-block replicas is set to \"2\" and cluster-rack-aware is set to \"true\""
								+ " any unique block will be sent to two different racks if present. The mkdse option --cluster-node-rack should be used to distinguish racks per dse node "
								+ " for this cluster.")
				.hasArg().withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("ext").create());
		options.addOption(OptionBuilder.withLongOpt("noext").create());
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
			System.out
					.println("Volume [" + wr.volume_name + "] created with a capacity of [" + wr.volume_capacity + "]");
			System.out.println("check [" + OSValidator.getConfigPath() + wr.volume_name.trim()
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
			System.err.println("ERROR : Unable to create volume because " + e.toString());
			e.printStackTrace();
			System.exit(-1);
		}
		System.exit(0);
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter.printHelp("mkfs.sdfs --volume-name=sdfs --volume-capacity=100GB", options);
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
