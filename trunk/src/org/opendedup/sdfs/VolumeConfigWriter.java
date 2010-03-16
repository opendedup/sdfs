package org.opendedup.sdfs;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.w3c.dom.*; //JAXP 1.1
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;
import javax.xml.transform.dom.*;

public class VolumeConfigWriter {
	/**
	 * write the client side config file
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	String volume_name = null;
	String base_path = "/opt/sdfs/" + volume_name;
	String meta_file_store = base_path + File.separator + "mdb";
	String dedup_db_store = base_path + File.separator + "ddb";
	String io_log = base_path + File.separator + "io.log";
	boolean safe_close = true;
	boolean safe_sync = false;
	short write_threads = 16;
	boolean dedup_files = true;
	int multi_read_timeout = 1000;
	int system_read_cache = 1000;
	short chunk_size = 128;
	int max_file_write_buffers = 50;
	int file_read_cache = 5;
	int max_open_files = 1024;
	int meta_file_cache = 1024;
	String filePermissions = "0644";
	String dirPermissions = "0755";
	String owner = "0";
	String group = "0";
	String volume_capacity = null;
	boolean chunk_store_local = true;
	String chunk_store_data_location = null;
	String chunk_store_meta_location = null;
	String chunk_store_hashdb_location = null;
	boolean chunk_store_pre_allocate = false;
	Short chunk_read_ahead_pages = 4;

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
		base_path = "/opt/sdfs/" + volume_name;
		if (cmd.hasOption("base-path")) {
			this.base_path = cmd.getOptionValue("base-path");
		}
		this.meta_file_store = this.base_path + File.separator + "mdb";
		this.io_log = this.base_path + File.separator + "io.log";
		this.dedup_db_store = this.base_path + File.separator + "ddb";
		this.chunk_store_data_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "chunks";
		this.chunk_store_meta_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "metadata";
		this.chunk_store_hashdb_location = this.base_path + File.separator
				+ "chunkstore" + File.separator + "hdb";

		if (cmd.hasOption("meta-db-store")) {
			this.meta_file_store = cmd.getOptionValue("base-path");
		}
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
		if (cmd.hasOption("chunk-store-metadata-location")) {
			this.chunk_store_meta_location = cmd
					.getOptionValue("chunk-store-metadata-location");
		}
		if (cmd.hasOption("chunk-store-hashdb-location")) {
			this.chunk_store_hashdb_location = cmd
					.getOptionValue("chunk-store-hashdb-location");
		}
		if (cmd.hasOption("chunk-store-pre-allocate")) {
			this.chunk_store_pre_allocate = Boolean.parseBoolean(cmd
					.getOptionValue("chunk-store-pre-allocate"));
		}
		if (cmd.hasOption("chunk-read-ahead-pages")) {
			this.chunk_read_ahead_pages = Short.parseShort(cmd
					.getOptionValue("chunk-read-ahead-pages"));
		}
		if (cmd.hasOption("chunk-store-local")) {
			this.chunk_store_local = Boolean.parseBoolean((cmd
					.getOptionValue("chunk-store-local")));
		}

		File file = new File("/etc/sdfs/" + this.volume_name.trim()
				+ "-volume-cfg.xml");
		if (file.exists()) {
			throw new IOException("Volume [" + this.volume_name
					+ "] already exists");
		}

	}

	public void writeConfigFile() throws ParserConfigurationException,
			IOException {
		File file = new File("/etc/sdfs/" + this.volume_name.trim()
				+ "-volume-cfg.xml");
		// Create XML DOM document (Memory consuming).
		Document xmldoc = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		xmldoc = impl.createDocument(null, "subsystem-config", null);
		// Root element.
		Element root = xmldoc.getDocumentElement();
		Element locations = xmldoc.createElement("locations");
		locations.setAttribute("dedup-db-store", this.dedup_db_store);
		locations.setAttribute("io-log", this.io_log);
		locations.setAttribute("meta-db-store", this.meta_file_store);
		root.appendChild(locations);
		Element io = xmldoc.createElement("io");
		io.setAttribute("chunk-size", Short.toString(this.chunk_size));
		io.setAttribute("dedup-files", Boolean.toString(this.dedup_files));
		io.setAttribute("file-read-cache", Integer
				.toString(this.file_read_cache));
		io.setAttribute("max-file-inactive", "900");
		io.setAttribute("max-file-write-buffers", Integer
				.toString(this.max_file_write_buffers));
		io
				.setAttribute("max-open-files", Integer
						.toString(this.max_open_files));
		io.setAttribute("meta-file-cache", Integer
				.toString(this.meta_file_cache));
		io.setAttribute("multi-read-timeout", Integer
				.toString(this.multi_read_timeout));
		io.setAttribute("safe-close", Boolean.toString(this.safe_close));
		io.setAttribute("safe-sync", Boolean.toString(this.safe_sync));
		io.setAttribute("system-read-cache", Integer
				.toString(this.system_read_cache));
		io.setAttribute("write-threads", Short.toString(this.write_threads));
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
		root.appendChild(vol);

		Element cs = xmldoc.createElement("local-chunkstore");
		cs.setAttribute("enabled", Boolean.toString(this.chunk_store_local));
		cs.setAttribute("pre-allocate", Boolean
				.toString(this.chunk_store_pre_allocate));
		cs.setAttribute("read-ahead-pages", Short
				.toString(this.chunk_read_ahead_pages));
		cs.setAttribute("chunk-store", this.chunk_store_data_location);
		cs.setAttribute("chunk-store-metadata", this.chunk_store_meta_location);
		cs.setAttribute("hash-db-store", this.chunk_store_hashdb_location);
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
		options.addOption(OptionBuilder.withLongOpt("help").withDescription(
				"Display these options.").hasArg(false).create());
		options
				.addOption(OptionBuilder
						.withLongOpt("base-path")
						.withDescription(
								"the folder path for all volume data and meta data.\n Defaults to: \n /opt/sdfs/<volume name>")
						.hasArg().withArgName("PATH").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("meta-db-store")
						.withDescription(
								"the folder path to for the meta file database.\n Defaults to: \n --base-path + "
										+ File.separator + "mdb").hasArg()
						.withArgName("PATH").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("dedup-db-store")
						.withDescription(
								"the folder path to location for the dedup file database.\n Defaults to: \n --base-path + "
										+ File.separator + "ddb").hasArg()
						.withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("io-log").withDescription(
				"the file path to location for the io log.\n Defaults to: \n --base-path + "
						+ File.separator + "io.log").hasArg().withArgName(
				"PATH").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-safe-close")
						.withDescription(
								"If true all files will be closed on filesystem close call. Otherwise, files will be closed"
										+ " based on inactivity. Set this to false if you plan on sharing the file system over"
										+ " an nfs share. True takes less RAM than False. \n Defaults to: \n true")
						.hasArg().withArgName("true|false").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-safe-sync")
						.withDescription(
								"If true all files will sync locally on filesystem sync call. Otherwise, by defaule (false), files will sync"
										+ " on close and data will per written to disk based on --max-file-write-buffers.  "
										+ "Setting this to true will ensure that no data loss will occur if the system is turned off abrubtly"
										+ " at the cost of slower speed. \n Defaults to: \n false")
						.hasArg().withArgName("true|false").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-write-threads")
						.withDescription(
								"The number of threads that can be used to process data writted to the file system. \n Defaults to: \n 16")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-dedup-files")
						.withDescription(
								"True mean that all files will be deduped inline by default. This can be changed on a one off"
										+ "basis by using the command \"setfattr -n user.cmd.dedupAll -v 556:false <path to file on sdfs volume>\"\n Defaults to: \n true")
						.hasArg().withArgName("true|false").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-multi-read-timeout")
						.withDescription(
								"Timeout to try to read from cache before it request data from the chunkstore. \n Defaults to: \n 1000")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-system-read-cache")
						.withDescription(
								"Size, in number of chunks, that read chunks will be cached into memory. \n Defaults to: \n 1000")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-chunk-size")
						.withDescription(
								"The unit size, in kB, of chunks stored. Set this to 4 if you would like to dedup VMDK files inline.\n Defaults to: \n 128")
						.hasArg().withArgName("SIZE in kB").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-max-file-write-buffers")
						.withDescription(
								"The number of memory buffers to have available for reading and writing per file. Each buffer in the size"
										+ " of io-chunk-size. \n Defaults to: \n 50")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-file-read-cache")
						.withDescription(
								"The number of memory buffers to have available for reading per file. Each buffer in the size"
										+ " of io-chunk-size. \n Defaults to: \n 5")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-max-open-files")
						.withDescription(
								"The maximum number of files that can be open at any one time. "
										+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
						.hasArg().withArgName("NUMBER").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("io-meta-file-cache")
						.withDescription(
								"The maximum number metadata files to be cached at any one time. "
										+ "If the number of files is exceeded the least recently used will be closed. \n Defaults to: \n 1024")
						.hasArg().withArgName("NUMBER").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-file")
				.withDescription(
						"Default File Permissions. "
								+ " \n Defaults to: \n 0644").hasArg()
				.withArgName("POSIX PERMISSIONS").create());
		options.addOption(OptionBuilder.withLongOpt("permissions-folder")
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
		options.addOption(OptionBuilder.withLongOpt("volume-capacity")
				.withDescription(
						"Capacity of the volume in [MB|GB|TB]. "
								+ " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("SIZE [MB|GB|TB]").create());
		options.addOption(OptionBuilder.withLongOpt("volume-name")
				.withDescription(
						"The name of the volume. "
								+ " \n THIS IS A REQUIRED OPTION").hasArg()
				.withArgName("STRING").create());
		options
				.addOption(OptionBuilder
						.withLongOpt("chunk-store-local")
						.withDescription(
								"enables or disables local chunk store. The chunk store can be "
										+ "local(true or remote(false) provided you supply the routing config file "
										+ "and there is a storageHub listening on the remote server(s) when you "
										+ "mount the SDFS volume."
										+ " \nDefaults to: \n true").hasArg()
						.withArgName("true|flase").create());
		options.addOption(OptionBuilder
				.withLongOpt("chunk-store-data-location").withDescription(
						"The directory where chunks will be stored."
								+ " \nDefaults to: \n --base-path + "
								+ File.separator + "chunkstore"
								+ File.separator + "chunks").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt(
				"chunk-store-metadata-location").withDescription(
				"The directory where extended data about chunks will be stored."
						+ " \nDefaults to: \n --base-path + " + File.separator
						+ "chunkstore" + File.separator + "metadata").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt(
				"chunk-store-hashdb-location").withDescription(
				"The directory where hash database for chunk locations will be stored."
						+ " \nDefaults to: \n --base-path + " + File.separator
						+ "chunkstore" + File.separator + "hdb").hasArg()
				.withArgName("PATH").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-store-pre-allocate")
				.withDescription(
						"Pre-allocate the chunk store if true."
								+ " \nDefaults to: \n false").hasArg()
				.withArgName("true|false").create());
		options.addOption(OptionBuilder.withLongOpt("chunk-read-ahead-pages")
				.withDescription(
						"The number of pages to read ahead when doing a disk read on the chunk store."
								+ " \nDefaults to: \n 8").hasArg().withArgName(
						"NUMBER").create());
		return options;
	}

	public static void main(String[] args) {
		try {
			System.out.println("Attempting to create volume ...");
			File f = new File("/etc/sdfs");
			if (!f.exists())
				f.mkdirs();
			VolumeConfigWriter wr = new VolumeConfigWriter();
			wr.parseCmdLine(args);
			wr.writeConfigFile();
			System.out.println("Volume [" + wr.volume_name
					+ "] created with a capacity of [" + wr.volume_capacity
					+ "]");
			System.out
					.println("check [/etc/sdfs/"
							+ wr.volume_name.trim()
							+ "-volume-cfg.xml] for configuration details if you need to change anything");
		} catch (Exception e) {
			System.err.println("ERROR : Unable to create volume because "
					+ e.toString());
			System.exit(-1);
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(125);
		formatter
				.printHelp(
						"mkfs.sdfs --volume-name=sdfs --volume-capacity=100GB",
						options);
	}

}
