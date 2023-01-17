package fuse.SDFS;

import java.io.File;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.OSValidator;

import fuse.FuseMount;

public class MountSDFS implements Daemon, Runnable {
	private static final Log log = LogFactory.getLog(SDFSFileSystem.class);
	private static String[] sFal = null;
	private static SDFSService sdfsService;
	private static String mountOptions;
	protected static ShutdownHook shutdownHook = null;
	private static String password = null;
	private static boolean nm;

	public static Options buildOptions() {
		Options options = new Options();
		options.addOption("o", true,
				"fuse mount options.\nWill default to: \ndirect_io,big_writes,allow_other,fsname=SDFS");
		options.addOption("r", false, "Restores files from cloud storage if the backend cloud store supports it");
		options.addOption("p", true, "port to use for sdfs cli");
		options.addOption("l", false, "Compact Volume on Disk");
		options.addOption("c", false, "Runs Consistency Check");
		options.addOption("e", true, "password to decrypt config");
		options.addOption("j", true, "environmental variable to decrypt config");
		options.addOption("n", false, "disable drive mount");
		options.addOption("m", true, "mount point for SDFS file system \n e.g. /media/dedup");
		options.addOption("v", true, "sdfs volume to mount \ne.g. dedup");
		options.addOption("f", true, "sdfs volume configuration file to mount \ne.g. /etc/sdfs/dedup-volume-cfg.xml");
		options.addOption("h", false, "displays available options");
		options.addOption("s", false, "If set ssl will not be used sdfscli traffic.");
		options.addOption("w", false, "Sync With All Files in Cloud.");
		options.addOption("q", false, "Use Console Logging.");
		options.addOption("b", true,
				"Folder basepath for sdfs to be used in linux os, same as sdfs-base-path in mkfs.sdfs. \n e.g. /opt/test");
		options.addOption("t", true, "Temporary directory for sdfs to be used in linux os. \n e.g. /tmp");
		options.addOption("a", false, "Runs Consistency Check Periodically");
		options.addOption("u", false, "Disables TLS and forces localhost");
		options.addOption("d", false, "Enables TLS and forces 0.0.0.0");
		return options;
	}

	public static void main(String[] args) throws ParseException {
		setup(args);
		try {
			if (!OSValidator.isWindows()) {
				FuseMount.mount(sFal, new SDFSFileSystem(Main.volume.getPath(), Main.volumeMountPoint), log);
				System.exit(0);
			} else {
				System.out.println("");
				System.out.println("volumemounted");
				System.out.println("");
				while (!SDFSService.isStopped()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {

					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("mount.sdfs", options);
	}

	private static void checkJavaVersion() {
		Properties sProp = java.lang.System.getProperties();
		String sVersion = sProp.getProperty("java.version");
		sVersion = sVersion.substring(0, 3);
		Float f = Float.valueOf(sVersion);
		if (f.floatValue() < (float) 1.8) {
			System.out.println("Java version must be 1.8 or newer");
			System.out.println("To get Java 8 go to https://jdk7.dev.java.net/");
			System.exit(-1);
		}
	}

	private static void setup(String[] args) throws ParseException {
		checkJavaVersion();
		int port = -1;
		String volumeConfigFile = null;
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		ArrayList<String> fal = new ArrayList<String>();
		ArrayList<String> volumes = new ArrayList<String>();
		fal.add("-f");
		if (cmd.hasOption("h")) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("n")) {
			nm = true;
		}
		if (cmd.hasOption("b")) {
			Main.sdfsBasePath = cmd.getOptionValue("b").trim();
		}
		if (cmd.hasOption("c")) {
			Main.runConsistancyCheck = true;
		}
		if (cmd.hasOption("a")) {
			Main.runConsistancyCheckPeriodically = true;
		}
		if (cmd.hasOption("q")) {
			SDFSLogger.useConsoleLogger();
		}

		if (cmd.hasOption("d")) {
			fal.add("-d");
		}
		if (cmd.hasOption("e")) {
			password = cmd.getOptionValue("e");
		}
		if (cmd.hasOption("j")) {
			String jv = cmd.getOptionValue("j");
			password = System.getenv(jv);
		}

		if (cmd.hasOption("p")) {
			port = Integer.parseInt(cmd.getOptionValue("p"));
		}
		if (!cmd.hasOption("m")) {
			fal.add(args[1]);
			Main.volumeMountPoint = args[1];
		} else {
			fal.add(cmd.getOptionValue("m"));
			Main.volumeMountPoint = cmd.getOptionValue("m");
		}

		String volname = "SDFS";
		if (cmd.hasOption("l")) {
			Main.runCompact = true;
			Main.forceCompact = true;
		}
		if (cmd.hasOption("r")) {
			Main.syncDL = true;
			Main.runConsistancyCheck = true;
		}
		if (cmd.hasOption("w")) {
			Main.syncDL = true;
			Main.syncDLAll = true;
		}
		if (cmd.hasOption("v")) {
			File f = new File("/etc/sdfs/" + cmd.getOptionValue("v").trim() + "-volume-cfg.xml");
			if (OSValidator.isWindows() || OSValidator.isUnix()) {
				f = new File(OSValidator.getConfigPath() + cmd.getOptionValue("v").trim() + "-volume-cfg.xml");
			}
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath() + " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else if (cmd.hasOption("f")) {
			File f = new File(cmd.getOptionValue("f").trim());
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath() + " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else {
			File f = new File("/etc/sdfs/" + args[0].trim() + "-volume-cfg.xml");
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath() + " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		}

		if (volumeConfigFile == null) {
			System.out.println("error : volume or path to volume configuration file not defined");
			printHelp(options);
			System.exit(-1);
		}
		if (OSValidator.isUnix()) {
			if (Main.sdfsBasePath.equals("")) {
				Main.logPath = "/var/log/sdfs/" + volname + ".log";
			} else {
				Main.logPath = OSValidator.getProgramBasePath() + "/var/log/sdfs/" + volname + ".log";
			}
		}
		if(cmd.hasOption("u")) {
			Main.usePortRedirector = true;
		}
		if(cmd.hasOption("d")) {
			Main.useDedicatedPort = true;
		}
		//Main.logPath = "/var/log/sdfs/" + volname + ".log";
		if (OSValidator.isWindows()) {
			File cf = new File(volumeConfigFile);
			String fn = cf.getName().substring(0, cf.getName().lastIndexOf(".")) + ".log";
			Main.logPath = OSValidator.getProgramBasePath() + File.separator + "logs" + File.separator + fn;
			File lf = new File(Main.logPath);
			lf.getParentFile().mkdirs();
		}
		sdfsService = new SDFSService(volumeConfigFile, volumes);
		try {
			sdfsService.start(port, password, cmd.hasOption("s"));
		} catch (Throwable e1) {
			e1.printStackTrace();
			System.out.println("Exiting because " + e1.toString());
			if(e1.toString().contains("No port Available in range Specified"))
			{
				System.exit(-2);
			}
			else
				System.exit(-1);
		}
		shutdownHook = new ShutdownHook(sdfsService, cmd.getOptionValue("m"));
		mountOptions = cmd.getOptionValue("m");
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		if (!OSValidator.isWindows()) {
			if (!nm) {
				if (cmd.hasOption("o")) {
					fal.add("-o");
					fal.add("modules=iconv,from_code=UTF-8,to_code=UTF-8,direct_io,allow_other,nonempty,big_writes,allow_other,fsname=sdfs:"
							+ volumeConfigFile + ":" + Main.sdfsCliPort + "," + cmd.getOptionValue("o"));
				} else {
					fal.add("-o");
					fal.add("modules=iconv,from_code=UTF-8,to_code=UTF-8,direct_io,allow_other,nonempty,big_writes,allow_other,fsname=sdfs:"
							+ volumeConfigFile + ":" + Main.sdfsCliPort);
				}
				sFal = new String[fal.size()];
				fal.toArray(sFal);
				for (int i = 0; i < sFal.length; i++) {
					SDFSLogger.getLog().info("Mount Option : " + sFal[i]);
				}
			}
		}
	}

	@Override
	public void destroy() {
		sdfsService = null;
		mountOptions = null;
	}

	@Override
	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
		setup(arg0.getArguments());
	}

	@Override
	public void start() throws Exception {
		if (!nm) {
			MountSDFS sd = new MountSDFS();
			Thread th = new Thread(sd);
			th.start();
		} else {
			System.out.println("SDFS Volume Service Started");
		}
	}

	@Override
	public void stop() throws Exception {
		SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
		SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
		sdfsService.stop();
		SDFSLogger.getLog().info("All Data Flushed");
		try {
			Process p = Runtime.getRuntime().exec("umount " + mountOptions);
			p.waitFor();
		} catch (Exception e) {

		}
		SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
	}

	@Override
	public void run() {
		try {
			FuseMount.mount(sFal, new SDFSFileSystem(Main.volume.getPath(), Main.volumeMountPoint), log);
			if (shutdownHook != null)
				shutdownHook.shutdown();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
