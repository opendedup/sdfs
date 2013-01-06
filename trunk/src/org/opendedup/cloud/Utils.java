package org.opendedup.cloud;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import org.opendedup.logging.SDFSLogger;

public class Utils {
	public static void deleteBucketAWS(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out.print("Deleting Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new AWSCredentials(awsAccessKey,
					awsSecretKey);
			S3Service bs3Service = new RestS3Service(bawsCredentials);
			S3Object[] obj = bs3Service.listObjects(bucketName);
			int n = 0;
			for (int i = 0; i < obj.length; i++) {
				bs3Service.deleteObject(bucketName, obj[i].getKey());
				if (n == 100) {
					System.out.print(".");
					n = 0;
				}
				n++;
			}
			bs3Service.deleteBucket(bucketName);
			System.out.println("done");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (ServiceException e) {
			SDFSLogger.getLog()
					.warn("Unable to delete bucket " + bucketName, e);
		}
	}

	public static void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("help") || args.length == 0) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("aws-delete-bucket")) {
			if (cmd.hasOption("aws-secret-key")
					&& cmd.hasOption("aws-access-key")
					&& cmd.hasOption("aws-bucket-name")) {
				deleteBucketAWS(cmd.getOptionValue("aws-bucket-name"),
						cmd.getOptionValue("aws-access-key"),
						cmd.getOptionValue("aws-secret-key"));
				System.exit(0);

			} else {
				System.out
						.println("delete bucket request failed. --aws-secret-key, --aws-access-key, and --aws-bucket-name options are required");
				System.exit(-1);
			}
		}

	}

	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Display these options.").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("aws-delete-bucket")
				.withDescription(
						"Deletes and S3 bucket. --aws-secret-key, --aws-access-key, and --aws-bucket-name options are required")
				.hasArg(false).create());
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
		formatter.printHelp("sdfs.cmd <options>", options);
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.ERROR);
		try {
			parseCmdLine(args);
		} catch (org.apache.commons.cli.UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelp(buildOptions());
		} catch (Exception e) {
			System.out
					.println("Error : It does not appear the SDFS volume is mounted or listening on tcp port 6642");
		}

	}

}
