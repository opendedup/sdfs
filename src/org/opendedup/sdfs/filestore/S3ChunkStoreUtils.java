package org.opendedup.sdfs.filestore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class S3ChunkStoreUtils {
	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Display these options.").hasArg(false)
				.create());
		options.addOption(OptionBuilder.withLongOpt("delete-bucket")
				.withDescription("Delete a  non-empty S3 bucket").hasArg()
				.withArgName("PATH").create());
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
		return options;
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter.printHelp("S3utils [options...]", options);
	}

	public static void parseCmdLine(String[] args) throws Exception {

		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("--help") || args.length == 0) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("delete-bucket")) {
			System.out.println("000000000000000000000");
			S3ChunkStore.deleteBucket(cmd.getOptionValue("delete-bucket"),
					cmd.getOptionValue("aws-access-key"),
					cmd.getOptionValue("aws-secret-key"));
		}
	}

	public static void main(String[] args) throws Exception {

		parseCmdLine(args);
	}

}
