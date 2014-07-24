package org.opendedup.sdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

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
import org.opendedup.hashing.HashFunctions;
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
	int network_port = 2222;
	String list_ip = "0.0.0.0";
	long chunk_store_allocation_size = 0;
	short chunk_size = 4;
	boolean awsEnabled = false;
	boolean azureEnabled = false;
	String cloudAccessKey = "";
	String cloudSecretKey = "";
	String cloudBucketName = "";
	String chunk_store_encryption_key = PassPhrase.getNext();
	String chunk_store_iv = PassPhrase.getIV();
	boolean chunk_store_encrypt = false;
	boolean compress = Main.compress;
	String hashType = HashFunctionPool.MURMUR3_16;
	private String clusterID = "sdfs-cluster";
	private byte clusterMemberID = 1;
	private String clusterConfig = "/etc/sdfs/jgroups.cfg.xml";
	String chunk_store_class = "org.opendedup.sdfs.filestore.FileChunkStore";
	String hash_db_class = Main.hashesDBClass;
	String sdfsCliPassword = "admin";
	String sdfsCliSalt = HashFunctions.getRandomString(6);
	String clusterRack = "rack1";
	String clusterNodeLocation = "pdx";
	int cloudThreads = 0;

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
		if (!cmd.hasOption("dse-capacity") || !cmd.hasOption("cluster-node-id")) {
			System.out.println("--dse-capacity and --cluster-node-id");
			printHelp(options);
			System.exit(-1);
		}
		if (cmd.hasOption("dse-name"))
			dse_name = cmd.getOptionValue("dse-name");
		else
			dse_name = InetAddress.getLocalHost().getHostName();
		if (!cmd.hasOption("listen-ip"))
			this.list_ip = InetAddress.getLocalHost().getHostAddress();
		else
			this.list_ip = cmd.getOptionValue("listen-ip");
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
		if (cmd.hasOption("aws-enabled")) {
			this.awsEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}
		if (cmd.hasOption("azure-enabled")) {
			this.azureEnabled = Boolean.parseBoolean(cmd
					.getOptionValue("aws-enabled"));
		}

		if (cmd.hasOption("encrypt")) {
			this.chunk_store_encrypt = Boolean.parseBoolean(cmd
					.getOptionValue("encrypt"));
		}
		if (cmd.hasOption("encryption-key")) {
			String key = cmd.getOptionValue("encryption-key");
			if (key.length() < 8) {
				System.err
						.println("Encryption Key must be greater than 8 characters");
				System.exit(-1);
			} else {
				this.chunk_store_encryption_key = cmd
						.getOptionValue("encryption-key");
			}
		}
		if (cmd.hasOption("encryption-iv")) {
			String iv =  cmd.getOptionValue("encryption-iv");
			this.chunk_store_iv = iv;
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
					this.chunk_size = 4;
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
					this.chunk_size = 4;
			} else {
				System.out.println("Error : Unable to create volume");
				System.out
						.println("cloud-access-key, cloud-secret-key, and cloud-bucket-name are required.");
				System.exit(-1);
			}
		}

		if (cmd.hasOption("dse-capacity")) {
			long sz = StringUtils.parseSize(cmd.getOptionValue("dse-capacity"));
			this.chunk_store_allocation_size = sz;
		}
		if (cmd.hasOption("page-size")) {
			this.chunk_size = Short.parseShort(cmd.getOptionValue("page-size"));
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

		if (cmd.hasOption("compress")) {
			this.compress = Boolean
					.parseBoolean(cmd.getOptionValue("compress"));
			this.compress = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-compress"));
			if (this.compress && !this.awsEnabled && this.azureEnabled) {
				this.chunk_store_class = "org.opendedup.sdfs.filestore.VariableFileChunkStore";
			}
		}
		if (cmd.hasOption("listen-port")) {
			this.network_port = Integer.parseInt(cmd
					.getOptionValue("listen-port"));
		}
		if (cmd.hasOption("cluster-dse-password"))
			this.sdfsCliPassword = cmd.getOptionValue("cluster-dse-password");
		if (cmd.hasOption("cluster-name"))
			this.clusterID = cmd.getOptionValue("cluster-name");
		if (cmd.hasOption("cluster-node-id"))
			this.clusterMemberID = Byte.parseByte(cmd
					.getOptionValue("cluster-node-id"));
		if (cmd.hasOption("cluster-config-path"))
			this.clusterConfig = cmd.getOptionValue("cluster-config-path");
		if (cmd.hasOption("cluster-node-location"))
			this.clusterNodeLocation = cmd
					.getOptionValue("cluster-node-location");
		if (cmd.hasOption("cluster-node-rack"))
			this.clusterRack = cmd.getOptionValue("cluster-node-rack");
		if (cmd.hasOption("io-threads")) {
			this.cloudThreads = Integer.parseInt(cmd
					.getOptionValue("io-threads"));
		}

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
		network.setAttribute("use-ssl", "false");
		root.appendChild(network);
		Element loc = xmldoc.createElement("locations");
		loc.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		loc.setAttribute("chunk-store", this.chunk_store_data_location);
		root.appendChild(loc);
		Element cs = xmldoc.createElement("chunk-store");

		cs.setAttribute("page-size", Integer.toString(this.chunk_size * 1024));
		cs.setAttribute("enabled", Boolean.toString(this.chunk_store_local));
		cs.setAttribute("allocation-size",
				Long.toString(this.chunk_store_allocation_size));
		cs.setAttribute("chunk-store", this.chunk_store_data_location);
		cs.setAttribute("encrypt", Boolean.toString(this.chunk_store_encrypt));
		cs.setAttribute("encryption-key", this.chunk_store_encryption_key);
		cs.setAttribute("encryption-iv", this.chunk_store_iv);
		cs.setAttribute("max-repl-batch-sz",
				Integer.toString(Main.MAX_REPL_BATCH_SZ));
		cs.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
		cs.setAttribute("chunkstore-class", this.chunk_store_class);
		cs.setAttribute("hashdb-class", this.hash_db_class);
		cs.setAttribute("hash-type", this.hashType);
		cs.setAttribute("cluster-id", this.clusterID);

		cs.setAttribute("cluster-member-id", Byte.toString(clusterMemberID));
		cs.setAttribute("cluster-config", this.clusterConfig);
		cs.setAttribute("cluster-node-rack", this.clusterRack);
		cs.setAttribute("cluster-node-location", this.clusterNodeLocation);
		cs.setAttribute("io-threads", Integer.toString(this.cloudThreads));
		try {
			cs.setAttribute("dse-password", HashFunctions.getSHAHash(
					this.sdfsCliPassword.getBytes(),
					this.sdfsCliSalt.getBytes()));
		} catch (Exception e) {
			System.out.println("unable to create password ");
			e.printStackTrace();
			throw new IOException(e);
		}
		cs.setAttribute("dse-password-salt", this.sdfsCliSalt);
		cs.setAttribute("compress", Boolean.toString(this.compress));
		if (this.awsEnabled) {
			Element aws = xmldoc.createElement("aws");
			aws.setAttribute("enabled", "true");
			aws.setAttribute("aws-access-key", this.cloudAccessKey);
			aws.setAttribute("aws-secret-key", this.cloudSecretKey);
			aws.setAttribute("aws-bucket-name", this.cloudBucketName);
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
						"The unit size, in KB, of chunks stored. This must match the chunk size for the volumes being stored.\n Defaults to: \n 4")
				.hasArg().withArgName("SIZE in KB").create());
		options.addOption(OptionBuilder
				.withLongOpt("hashdb-location")
				.withDescription(
						"The directory where hash database for chunk locations will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "hdb").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-capacity")
				.withDescription(
						"The size in bytes of the Dedup Storeage Engine. "
								+ "This . \n Defaults to: \n The size of the Volume")
				.hasArg().withArgName("BYTES").create());
		options.addOption(OptionBuilder
				.withLongOpt("hash-type")
				.withDescription(
						"This is the type of hash engine used to calculate a unique hash. The valid options for hash-type are "
								+ HashFunctionPool.TIGER_16
								+ " "
								+ HashFunctionPool.TIGER_24
								+ " "
								+ HashFunctionPool.MURMUR3_16
								+ " This Defaults to "
								+ HashFunctionPool.TIGER_16)
				.hasArg()
				.withArgName(
						HashFunctionPool.TIGER_16 + "|"
								+ HashFunctionPool.TIGER_24 + "|"
								+ HashFunctionPool.MURMUR3_16).create());
		options.addOption(OptionBuilder
				.withLongOpt("encrypt")
				.withDescription(
						"Whether or not to Encrypt chunks within the Dedup Storage Engine. The encryption key is generated automatically."
								+ " For AWS this is a good option to enable. The default for this is"
								+ " false").hasArg().withArgName("true|false")
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("encryption-key")
				.withDescription(
						"The encryption key used for encrypting data. If not specified a strong key will be generated automatically. They key must be at least 8 charaters long")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("encryption-iv")
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
				.withLongOpt("cloud-bucket-name")
				.withDescription(
						"Set to the value of Cloud Storage bucket name. This will need to be unique and a could be set the the access key if all else fails. aws-enabled, aws-secret-key, and aws-secret-key will also need to be set. ")
				.hasArg().withArgName("Unique Cloud Bucket Name").create());
		options.addOption(OptionBuilder
				.withLongOpt("compress")
				.withDescription(
						"Compress chunks before they are sent to the Storeage. By default this is set to true. Set it to  false for volumes that hold data that does not compress well, such as pictures and  movies")
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
				.withLongOpt("listen-ip")
				.withDescription(
						"Host name or IPv4 Address to listen on for incoming connections. This is a required option.")
				.hasArg().withArgName("IPv4 Address").create());
		/*
		 * options.addOption(OptionBuilder .withLongOpt("listen-port")
		 * .withDescription(
		 * "TCP Port to listen on for incoming connections. Defaults to 2222")
		 * .hasArg().withArgName("IP Port").create());
		 */
		options.addOption(OptionBuilder
				.withLongOpt("dse-password")
				.withDescription(
						"The password used to remotely connect to the TCP server on this system. This does not authenticate cluster connections just fetches and writes.")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-id")
				.withDescription(
						"The name used to identify the cluster group. This defaults to sdfs-cluster. This name should be the same on all members of this cluster")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-node-id")
				.withDescription(
						"The unique id [1-200] used to identify this node within the cluster group. This defaults to 1 but should be incremented for each new DSE member of the cluster."
								+ " As an example, if this is the second DSE within the cluster, the id should be \"2\"")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-config")
				.withDescription(
						"The jgroups configuration used to configure this cluster node. This defaults to \"/etc/sdfs/jgroups.cfg.xml\". ")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-node-location")
				.withDescription(
						"The location where this cluster node is located.")
				.hasArg().withArgName("String").create());

		options.addOption(OptionBuilder
				.withLongOpt("cluster-node-rack")
				.withDescription(
						"The rack where this cluster node is located.This is used to make sure that redundant blocks are not all copied to the name rack. To make the cluster rack aware, "
								+ "also set the --cluster-rack-aware=true.")
				.hasArg().withArgName("String").create());
		options.addOption(OptionBuilder
				.withLongOpt("io-threads")
				.withDescription(
						"Sets the number of io threads to use for io operations to the dse storage provider. This is set to 8 by default but can be changed to more or less based on bandwidth and io.")
				.hasArg().withArgName("integer").create());
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
		formatter
				.printHelp(
						"mkdse --dse-name=sdfs --dse-capacity=100GB --listen-ip=192.168.0.10",
						options);
	}

}
