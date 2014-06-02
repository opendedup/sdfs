package org.opendedup.cloud;

import java.util.ArrayList;

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
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;

public class Utils {
	public static void deleteBucketAWS(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out.print("Deleting Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new AWSCredentials(awsAccessKey,
					awsSecretKey);
			S3Service bs3Service = new RestS3Service(bawsCredentials);
			S3Object[] obj = bs3Service.listObjects(bucketName);
			while (obj.length > 0) {
				int n = 0;
				ArrayList<String> al = new ArrayList<String>();
				for (int i = 0; i < obj.length; i++) {
					al.add(obj[i].getKey());

					if (n == 100) {
						String[] ar = new String[al.size()];
						ar = al.toArray(ar);
						bs3Service.deleteMultipleObjects(bucketName, ar);
						al = new ArrayList<String>();
						System.out.print(".");
						n = 0;
					}
					n++;
				}
				String[] ar = new String[al.size()];
				ar = al.toArray(ar);
				bs3Service.deleteMultipleObjects(bucketName, (String[]) ar);
				al = new ArrayList<String>();
				obj = bs3Service.listObjects(bucketName);
			}
			bs3Service.deleteBucket(bucketName);
			System.out.println("done");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (ServiceException e) {
			e.printStackTrace();
		}
	}

	public static void listBucketAWS(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out
					.println("Listing Objects in Bucket [" + bucketName + "]");
			AWSCredentials bawsCredentials = new AWSCredentials(awsAccessKey,
					awsSecretKey);
			S3Service bs3Service = new RestS3Service(bawsCredentials);
			StorageObjectsChunk ck = bs3Service.listObjectsChunked(bucketName,
					null, null, 100, null);
			StorageObject[] obj = ck.getObjects();
			String lastKey = null;
			while (obj.length > 0) {

				for (int i = 0; i < obj.length; i++) {
					lastKey = obj[i].getKey();

					System.out.println(lastKey);
				}
				ck = bs3Service.listObjectsChunked(bucketName, null, null, 100,
						lastKey);
				obj = ck.getObjects();
			}

		} catch (ServiceException e) {
			e.printStackTrace();
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
		if (cmd.hasOption("aws-list-bucket-obj")) {
			if (cmd.hasOption("aws-secret-key")
					&& cmd.hasOption("aws-access-key")
					&& cmd.hasOption("aws-bucket-name")) {
				listBucketAWS(cmd.getOptionValue("aws-bucket-name"),
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
				.withLongOpt("aws-list-bucket-obj")
				.withDescription(
						"Lists objects in the bucket. --aws-secret-key, --aws-access-key, and --aws-bucket-name options are required")
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
			e.printStackTrace();
		}

	}

}
