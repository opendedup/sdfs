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
import org.apache.commons.cli.UnrecognizedOptionException;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.filestore.S3ChunkStore;
import org.opendedup.util.OSValidator;
import org.opendedup.util.PassPhrase;
import org.opendedup.util.StringUtils;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DSEConfigWriter {
	/**
	 * write the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */

	static long tbc = 1099511627776L;
	static long gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;

	String dse_name = null;
	String base_path = OSValidator.getProgramBasePath() + File.separator
			+ "dse" + File.separator + dse_name;
	boolean chunk_store_local = true;
	String chunk_store_data_location = null;
	String chunk_store_hashdb_location = null;
	boolean chunk_store_pre_allocate = false;
	boolean use_udp = false;
	int network_port = 2222;
	String list_ip = "0.0.0.0";
	long chunk_store_allocation_size = 0;
	Short chunk_read_ahead_pages = 4;
	short chunk_size = 128;
	String chunk_gc_schedule = "0 0 0/4 * * ?";
	int remove_if_older_than = 6;
	boolean awsEnabled = false;
	boolean azureEnabled = false;
	String cloudAccessKey = "";
	String cloudSecretKey = "";
	String cloudBucketName = "";
	int chunk_store_read_cache = Main.chunkStorePageCache;
	int chunk_store_dirty_timeout = Main.chunkStoreDirtyCacheTimeout;
	String chunk_store_encryption_key = PassPhrase.getNext();
	boolean chunk_store_encrypt = false;
	boolean cloudCompress = Main.cloudCompress;
	String hashType = HashFunctionPool.TIGER_16;
	boolean upstreamEnabled = false;
	String upstreamHost = null;
	int upstreamPort = 2222;
	String upstreamPassword = "admin";

	public void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (UnrecognizedOptionException e) {
			System.err.println(e.getMessage());
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("--help")) {
			printHelp(options);
			System.exit(1);
		}
		if (!cmd.hasOption("dse-name") || !cmd.hasOption("dse-capacity")) {
			System.out
					.println("--dse-name and --dse-capacity are required options");
			printHelp(options);
			System.exit(-1);
		}
		dse_name = cmd.getOptionValue("dse-name");
		base_path = OSValidator.getProgramBasePath() + "dse" + File.separator
				+ dse_name;
		if (cmd.hasOption("base-path")) {
			this.base_path = cmd.getOptionValue("base-path");
		}
		this.chunk_store_data_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "chunks";
		this.chunk_store_hashdb_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "hdb";
		if (cmd.hasOption("data-location")) {
			this.chunk_store_data_location = cmd
					.getOptionValue("data-location");
		}
		if (cmd.hasOption("hashdb-location")) {
			this.chunk_store_hashdb_location = cmd
					.getOptionValue("hashdb-location");
		}
		if (cmd.hasOption("pre-allocate")) {
			this.chunk_store_pre_allocate = Boolean.parseBoolean(cmd
					.getOptionValue("pre-allocate"));
		}
		if (cmd.hasOption("read-ahead-pages")) {
			this.chunk_read_ahead_pages = Short.parseShort(cmd
					.getOptionValue("read-ahead-pages"));
		} else {
			if (this.chunk_size < 32) {
				this.chunk_read_ahead_pages = (short) (32 / this.chunk_size);
			} else {
				this.chunk_read_ahead_pages = 1;
			}
		}
		if (cmd.hasOption("aws-enabled")) {
			this.awsEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("azure-enabled")) {
			this.azureEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("read-cache")) {
			this.chunk_store_read_cache = Integer.parseInt(cmd
					.getOptionValue("read-cache"));
		}
		if (cmd.hasOption("encrypt")) {
			this.chunk_store_encrypt = Boolean.parseBoolean(cmd
					.getOptionValue("encrypt"));
		}
		if (cmd.hasOption("dirty-timeout")) {
			this.chunk_store_dirty_timeout = Integer.parseInt(cmd
					.getOptionValue("dirty-timeout"));
		}
		if (this.awsEnabled) {
			if (cmd.hasOption("cloud-secret-key")
					&& cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
				if(!S3ChunkStore.checkAuth(cloudAccessKey, cloudSecretKey)) {
					System.out.println("Error : Unable to create volume");
					System.out
							.println("cloud-access-key or cloud-secret-key is incorrect");
					System.exit(-1);
				}
				if(!S3ChunkStore.checkBucketUnique(cloudAccessKey, cloudSecretKey, cloudBucketName)) {
					System.out.println("Error : Unable to create volume");
					System.out
							.println("cloud-bucket-name is not unique");
					System.exit(-1);
				}
					
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
			if (cmd.hasOption("cloud-compress"))
				this.cloudCompress = Boolean.parseBoolean(cmd
						.getOptionValue("cloud-compress"));
		} 
		
		else if (this.azureEnabled) {
			if (cmd.hasOption("cloud-secret-key")
					&& cmd.hasOption("cloud-access-key")
					&& cmd.hasOption("cloud-bucket-name")) {
				this.cloudAccessKey = cmd.getOptionValue("cloud-access-key");
				this.cloudSecretKey = cmd.getOptionValue("cloud-secret-key");
				this.cloudBucketName = cmd.getOptionValue("cloud-bucket-name");
				if (!cmd.hasOption("io-chunk-size"))
					this.chunk_size = 128;
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
			if (cmd.hasOption("cloud-compress"))
				this.cloudCompress = Boolean.parseBoolean(cmd
						.getOptionValue("cloud-compress"));
		}
		if (cmd.hasOption("gc-schedule")) {
			this.chunk_gc_schedule = cmd.getOptionValue("gc-schedule");
		}
		if (cmd.hasOption("eviction")) {
			this.remove_if_older_than = Integer.parseInt(cmd
					.getOptionValue("eviction"));
		}
		if (cmd.hasOption("dse-capacity")) {

			long sz = StringUtils.parseSize(cmd.getOptionValue("dse-capacity"));
			this.chunk_store_allocation_size = sz;
		}
		if (cmd.hasOption("page-size")) {
			this.chunk_size = Short.parseShort(cmd.getOptionValue("page-size"));
		}
		if (cmd.hasOption("enable-udp")) {
			this.use_udp = Boolean.parseBoolean(cmd
					.getOptionValue("enable-udp"));
		}
		if (cmd.hasOption("listen-ip")) {
			this.list_ip = cmd.getOptionValue("listen-ip");
		}
		if (cmd.hasOption("hash-type")) {
			String ht = cmd.getOptionValue("hash-type");
			if (ht.equalsIgnoreCase(HashFunctionPool.TIGER_16)
					|| ht.equalsIgnoreCase(HashFunctionPool.TIGER_24)
					|| ht.equalsIgnoreCase(HashFunctionPool.MURMUR3_16))
				this.hashType = ht;
			else {
				System.out.println("Invalid Hash Type. Must be "
						+ HashFunctionPool.TIGER_16 + " "
						+ HashFunctionPool.TIGER_24 + " "
						+ HashFunctionPool.MURMUR3_16);
				System.exit(-1);
			}
		}
		if (cmd.hasOption("listen-port")) {
			this.network_port = Integer.parseInt(cmd
					.getOptionValue("listen-port"));
		}
		if(cmd.hasOption("upstream-enabled")) {
			if(!cmd.hasOption("upstream-host")) {
				throw new Exception("upstream-host must be specified");
			} else {
				this.upstreamHost = cmd.getOptionValue("upstream-host");
				if(cmd.hasOption("upstream-host-port"))
					this.upstreamPort = Integer.parseInt(cmd.getOptionValue("upstream-host-port"));
			}
		}
		if(cmd.hasOption("upstream-password"))
			this.upstreamPassword = cmd.getOptionValue("upstream-password");
		File file = new File(OSValidator.getConfigPath() + this.dse_name.trim()
				+ "-dse-cfg.xml");
		if (file.exists()) {
			throw new IOException("DSE Configuration [" + this.dse_name
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
		File file = new File(OSValidator.getConfigPath() + this.dse_name.trim()
				+ "-dse-cfg.xml");
		// Create XML DOM document (Memory consuming).
		Document xmldoc = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		xmldoc = impl.createDocument(null, "chunk-server", null);
		// Root element.
		Element root = xmldoc.getDocumentElement();
		root.setAttribute("version", Main.version);
		Element network = xmldoc.createElement("network");
		network.setAttribute("hostname", this.list_ip);
		network.setAttribute("port", Integer.toString(this.network_port));
		network.setAttribute("use-udp", Boolean.toString(this.use_udp));
		network.setAttribute("upstream-enabled", Boolean.toString(this.upstreamEnabled));
		network.setAttribute("upstream-host", this.upstreamHost);
		network.setAttribute("upstream-host-port", Integer.toString(this.upstreamPort));
		network.setAttribute("upstream-password", this.upstreamPassword);
		root.appendChild(network);
		Element loc = xmldoc.createElement("locations");
		loc.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		loc.setAttribute("chunk-store", this.chunk_store_data_location);
		root.appendChild(loc);
		Element cs = xmldoc.createElement("chunk-store");

		cs.setAttribute("page-size", Integer.toString(this.chunk_size * 1024));
		cs.setAttribute("pre-allocate",
				Boolean.toString(this.chunk_store_pre_allocate));
		cs.setAttribute("allocation-size",
				Long.toString(this.chunk_store_allocation_size));
		cs.setAttribute("max-repl-batch-sz", Integer.toString(Main.MAX_REPL_BATCH_SZ));
		cs.setAttribute("chunk-gc-schedule", this.chunk_gc_schedule);
		cs.setAttribute("eviction-age",
				Integer.toString(this.remove_if_older_than));
		cs.setAttribute("read-ahead-pages",
				Short.toString(this.chunk_read_ahead_pages));
		cs.setAttribute("encrypt", Boolean.toString(this.chunk_store_encrypt));
		cs.setAttribute("encryption-key", this.chunk_store_encryption_key);
		cs.setAttribute("chunk-store-read-cache",
				Integer.toString(this.chunk_store_read_cache));
		cs.setAttribute("chunk-store-dirty-timeout",
				Integer.toString(this.chunk_store_dirty_timeout));
		cs.setAttribute("hash-type", this.hashType);
		if (this.awsEnabled) {
			Element aws = xmldoc.createElement("aws");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("aws-access-key", this.cloudAccessKey);
			aws.setAttribute("aws-secret-key", this.cloudSecretKey);
			aws.setAttribute("aws-bucket-name", this.cloudBucketName);
			aws.setAttribute("compress", Boolean.toString(this.cloudCompress));
			cs.appendChild(aws);
		} else if (this.azureEnabled) {
			Element aws = xmldoc.createElement("azure-store");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("azure-access-key", this.cloudAccessKey);
			aws.setAttribute("azure-secret-key", this.cloudSecretKey);
			aws.setAttribute("azure-bucket-name", this.cloudBucketName);
			aws.setAttribute("compress", Boolean.toString(this.cloudCompress));
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
				.withLongOpt("base-path")
				.withDescription(
						"the folder path for all volume data and meta data.\n Defaults to: \n "
								+ OSValidator.getProgramBasePath()
								+ "<volume name>").hasArg().withArgName("PATH")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-name")
				.withDescription(
						"The name of the Dedup Storage Engine. "
								+ " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("STRING").create());
		options.addOption(OptionBuilder
				.withLongOpt("data-location")
				.withDescription(
						"The directory where chunks will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "chunks").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("page-size")
				.withDescription(
						"The unit size, in kB, of chunks stored. This must match the chunk size for the volumes being stored.\n Defaults to: \n 128")
				.hasArg().withArgName("SIZE in kB").create());
		options.addOption(OptionBuilder
				.withLongOpt("hashdb-location")
				.withDescription(
						"The directory where hash database for chunk locations will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "hdb").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("pre-allocate")
				.withDescription(
						"Pre-allocate the chunk store if true."
								+ " \nDefaults to: \n false").hasArg()
				.withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("read-ahead-pages")
				.withDescription(
						"The number of pages to read ahead when doing a disk read on the chunk store."
								+ " \nDefaults to: \n 128/io-chunk-size or 1 if greater than 128")
				.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder
				.withLongOpt("gc-schedule")
				.withDescription(
						"The schedule, in cron format, to check for unclaimed chunks within the Dedup Storage Engine. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 0 0 0/2 * * ?")
				.hasArg().withArgName("CRON Schedule").create());
		options.addOption(OptionBuilder
				.withLongOpt("eviction")
				.withDescription(
						"The duration, in hours, that chunks will be removed from Dedup Storage Engine if unclaimed. "
								+ "This should happen less frequently than the io-claim-chunks-schedule. \n Defaults to: \n 6")
				.hasArg().withArgName("HOURS").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-capacity")
				.withDescription(
						"The size in bytes of the Dedup Storeage Engine. "
								+ "This . \n Defaults to: \n The size of the Volume")
				.hasArg().withArgName("BYTES").create());
		options.addOption(OptionBuilder
				.withLongOpt("read-cache")
				.withDescription(
						"The size in MB of the Dedup Storeage Engine's read cache. Its useful to set this if you have high number of reads"
								+ " for AWS/Cloud storage "
								+ "This . \n Defaults to: \n 5MB").hasArg()
				.withArgName("Megabytes").create());
		options.addOption(OptionBuilder
				.withLongOpt("hash-type")
				.withDescription(
						"This is the type of hash engine used to calculate a unique hash. The valid options for hash-type are "
								+ HashFunctionPool.TIGER_16
								+ " "
								+ HashFunctionPool.TIGER_24
								+ " "
								+ HashFunctionPool.MURMUR3_16
								+ " This Defaults to " + HashFunctionPool.TIGER_16).hasArg()
				.withArgName(HashFunctionPool.TIGER_16
						+ "|"
						+ HashFunctionPool.TIGER_24
						+ "|"
						+ HashFunctionPool.MURMUR3_16).create());
		options.addOption(OptionBuilder
				.withLongOpt("encrypt")
				.withDescription(
						"Whether or not to Encrypt chunks within the Dedup Storage Engine. The encryption key is generated automatically."
								+ " For AWS this is a good option to enable. The default for this is"
								+ " false").hasArg().withArgName("true|false")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("dirty-timeout")
				.withDescription(
						"The timeout, in milliseconds, for a previous read for the same chunk to finish within the Dedup Storage Engine. "
								+ "For AWS with slow links you may want to set this to a higher number. The default for this is"
								+ " 1000 ms.").hasArg()
				.withArgName("Milliseconds").create());
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
				.withLongOpt("cloud-bucket-name")
				.withDescription(
						"Set to the value of Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. aws-enabled, aws-secret-key, and aws-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique Cloud Bucket Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("cloud-compress")
				.withDescription(
						"Compress chunks before they are sent to the Cloud Storeage bucket. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
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
				.withLongOpt("enable-udp")
				.withDescription(
						"Enable udp for some communication between Volume and DSE. Defaults to false").create());
		options.addOption(OptionBuilder
				.withLongOpt("listen-ip")
				.withDescription(
						"Host name or IPv4 Address to listen on for incoming connections. Defaults to \"0.0.0.0\"")
				.hasArg().withArgName("IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("listen-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("upstream-enabled")
				.withDescription(
						"Enable Upstream Dedup Storage Engine communication").create());
		options.addOption(OptionBuilder
				.withLongOpt("upstream-host")
				.withDescription(
						"Host name or IPv4 Address ")
				.hasArg().withArgName("FQDN or IPv4 Address").create());
		options.addOption(OptionBuilder
				.withLongOpt("upstream-password")
				.withDescription(
						"SDFSCLI Password of upstream host")
				.hasArg().withArgName("STRING").create());
		options.addOption(OptionBuilder
				.withLongOpt("upstream-host-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		options.addOption(OptionBuilder
				.withLongOpt("listen-port")
				.withDescription(
						"TCP and UDP Port to listen on for incoming connections. Defaults to 2222")
				.hasArg().withArgName("IP Port").create());
		return options;
	}

	public static void main(String[] args) {
		try {
			System.out
					.println("Attempting to create Dedup Storage Engine Config ...");
			File f = new File(OSValidator.getConfigPath());
			if (!f.exists())
				f.mkdirs();
			DSEConfigWriter wr = new DSEConfigWriter();
			wr.parseCmdLine(args);
			wr.writeConfigFile();
			System.out.println("dse [" + wr.dse_name
					+ "] created with a capacity of ["
					+ wr.chunk_store_allocation_size + "]");
			System.out
					.println("check ["
							+ OSValidator.getConfigPath()
							+ wr.dse_name.trim()
							+ "-dse-cfg.xml] for configuration details if you need to change anything");
		} catch (Exception e) {
			System.err.println("ERROR : Unable to create volume because "
					+ e.toString());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter.printHelp("mkfs.sdfs --dse-name=sdfs --dse-capacity=100GB",
				options);
	}

}
