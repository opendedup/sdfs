package org.opendedup.sdfs;

import java.io.File;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.OSValidator;
import org.w3c.dom.Element;

/**
 *
 * @author Sam Silverberg Global constants used for SDFS classes.
 *
 */
public class Main {
	static {
		if (OSValidator.isWindows()) {
			Main.chunkStore = System.getenv("programfiles") + File.separator
					+ "sdfs" + File.separator;
		}

	}

	public static int hlVersion = 0;
	public static boolean checkArchiveOnOpen = false;
	public static boolean checkArchiveOnRead = true;
	public static int hashSeed=6442;
	public static double fpp = .01;
	public static boolean ignoreDSEHTSize = true;
	public static long GLOBAL_CACHE_SIZE=512*1024L*1024L;
	public static int readAheadThreads = 16;
	public static boolean refCount = true;
	public static boolean useLegacy = false;
	public static String authJarFilePath="";
	public static String authClassInfo="";
	public static String prodConfigFilePath = "";
	public static boolean s3ApiCompatible = false;
	public static int immutabilityPeriod=10;//by default set to 10 days
	public static boolean partialTransition = false;
	public static boolean encryptBucket = false;
	public static boolean rehydrateBlobs = false;

	public static int parallelDBCount = 4;
	public static int writeTimeoutSeconds = -1; // 1 hour timeout
	public static int readTimeoutSeconds = -1; // 1 hour timeout
	// public static VolumeConfigWriterThread wth = null;
	public static boolean runConsistancyCheck = false;
	public static boolean blockDev = false;

	public static boolean firstRun = true;
	public static boolean disableGC = false;
	public static boolean logToConsole = false;
	public static boolean REFRESH_BLOBS=false;
	public static int MAX_TBLS=0;
	public static int REPLICATION_THREADS=8;

	public static boolean COMPRESS_METADATA = false;
	public static boolean syncDL = false;
	public static boolean syncDLAll = false;

	public static StandAloneGCScheduler pFullSched = null;

	public static String logPath = "/var/log/sdfs/sdfs.log";
	public static String logSize="10MB";
	public static int logFiles=10;
	public static byte MAPVERSION = 0;
	public static int MAX_OPEN_SST_FILES=-1;
	public static String eSdfsPassword =null;
	public static String sdfsPassword = "";
	public static boolean readAheadMap = true;
	public static String sdfsPasswordSalt = "";
	public static String eSdfsPasswordSalt = null;
	public static boolean allowExternalSymlinks = true;

	public static boolean sdfsCliSSL = true;
	public static boolean sdfsCliRequireAuth = false;
	public static boolean sdfsCliRequireMutualTLSAuth = false;
	public static int sdfsCliPort = 6442;
	public static boolean sdfsCliEnabled = true;
	public static String sdfsCliListenAddr = "localhost";
	public static boolean runCompact = false;
	public static boolean INLINE_REF_INSERT = false;
	public static byte [] decKey = null;
	public static boolean forceCompact = false;
	public static int MAX_REPL_BATCH_SZ = 128;
	public static String extendCapacity = "";
	public static String sdfsBasePath = "";
	public static String volumeConfigFile = "";

	public static SDFSEvent mountEvent = null;

	public static boolean standAloneDSE = false;

	/**
	 * DSE Host port for front end file systems
	 */
	public static int DSERemotePort = 2222;

	/**
	 * DSE Host use SSL for front end file systems
	 */
	public static boolean DSERemoteUseSSL = true;

	/**
	 * The Version of SDFS this is
	 */
	public static String version = "master";

	public static boolean readAhead = false;

	/**
	 * The location where the actual blocks of deduplicated data will be
	 * located. This is used for the chunk stores.
	 */
	public static String chunkStore = "";

	public static boolean usePortRedirector;

	public static boolean useDedicatedPort;

	/**
	 * Future implementation for pluggable chunkstores
	 */
	public static Element chunkStoreConfig = null;

	/**
	 * Future implementation of pluggable cs
	 */
	public static String chunkStoreClass = "org.opendedup.sdfs.filestore.NullChunkStore";

	// public static String hashesDBClass =
	// "com.opendedup.collections.FileBasedCSMap";
	public static String hashesDBClass = "org.opendedup.collections.RocksDBMap";
	/**
	 * Future implementation of pluggable garbageCollector
	 */
	public static String gcClass = "org.opendedup.sdfs.filestore.gc.PFullGC";

	/**
	 * Secret Key to Encrypt chunks in DSE.
	 */
	public static String chunkStoreEncryptionKey = null;
	public static String eChunkStoreEncryptionKey = null;
	public static String chunkStoreEncryptionIV = null;
	/**
	 * whether encryption should be enabled for the DSE
	 */
	public static boolean chunkStoreEncryptionEnabled = false;
	public static long DSEID = 0;
	/**
	 * The location where database of deduped hashes will be stores and written
	 * to. This is used for the chunk store.
	 */
	public static String hashDBStore = null;
	/**
	 * The location where dedup file maps will be stored. Dedup file maps are
	 * database files and the virtual representation of a file on disk. This is
	 * used on the client.
	*/
	public static String dedupDBStore = null;
	public static String lookupfilterStore = null;
	public static String dedupDBTrashStore = null;
	public static boolean DDB_TRASH_ENABLED = false;
	public static boolean sdfsSyncEnabled = false;
	public static boolean runConsistancyCheckPeriodically = false;
	public static String sdfsVolName = "";
	public static String retrievalTier = "";
	public static long CLEANUP_THREAD_INTERVAL=30*1000;
	public static boolean PRINT_CACHESTATS=true;
	/**
	 * The location where the model of the virtual file structure will be held.
	 * The virtual file structure maps what will be presented as the filesystem
	 * is being mapped. This is used on the client.
	 */
	// public static String metaDBStore = "/opt/dedup/jdb";
	/**
	 * The location where the IO stats SDFSLogger.getLog() file will be held.
	 * The IO SDFSLogger.getLog() file is used to record IO stats at specific
	 * intervals. This is used on the client and chunk store.
	 */
	public static String ioLogFile = null;
	/**
	 * The location where debug and system SDFSLogger.getLog()s are kept. This
	 * is used on the client and chunk store.
	 */
	public static String logLocation = null;
	/**
	 * The chunk size used for deduplication of incoming data. This is used on
	 * the client.
	 */
	public static int CHUNK_LENGTH = 16 * 1024;
	public static int MIN_CHUNK_LENGTH = (4*1024)-1;


	/**
	 * The maximum number of writable chunks @see
	 * com.annesam.sdfs.io.WritableCacheBuffer to keep in memory for a specific
	 * file. As a file is written too the write buffer will fill up to the
	 * maxWriteBuffers size and the purged from the buffer and written to the
	 * chunk store based on LRU queuing. This parameter is used per file. This
	 * is used on the client.
	 */
	public static int maxWriteBuffers = 100;

	/**
	 * Write threads @see com.annesam.util.ThreadPool are used to process data
	 * from dedup file write buffers in a multi threaded fashion. When data is
	 * purged from the write buffer it is added to the thread pool queue and
	 * then processed by an available thread. The number of initial available
	 * threads is set by writeThreads. The number here should be set to at least
	 * the number of cpu cores used by the client. This is used on the client.
	 */

	public static int writeThreads = (short) (Runtime.getRuntime()
			.availableProcessors() * 3);
	/**
	 * The representation of a blank hash of the default chunk size. This is
	 * used on the client.
	 */

	public static byte[] blankHash = new byte[CHUNK_LENGTH];
	public static String internalMountPath = "/media";
	public static String vmdkMountPath = "/media/vmmount";
	public static String scriptsDir = "/opt/dedup/scripts/";
	public static String vmdkMountOptions = "rw";
	public static int defaultOffset = 2048;

	public static String permissionsFile = null;

	public static String PasswordPattern = "(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[@#$%^&+=])(?=\\S+$).{8,}";

	/**
	 * The maximum number of dedup files that can be open at any one time. This
	 * can be changed based on the amount of memory on a specfic system. Files
	 * will be closed automatically once the maxOpenFiles has been hit based on
	 * LRU. This is used on the client.
	 */
	public static int maxOpenFiles = 100;
	/**
	 * Default file posix permissions as is represented on the client when the
	 * filesystem is mounted. This is used on the client.
	 */
	public static int defaultFilePermissions = 0644;
	/**
	 * Default folder posix permissions as is represented on the client when the
	 * filesystem is mounted. This is used on the client.
	 */
	public static int defaultDirPermissions = 0755;
	/**
	 * Default posix owner as is represented on the client when the filesystem
	 * is mounted. This is used on the client.
	 */
	public static int defaultOwner = 0;
	/**
	 * Default posix group as is represented on the client when the filesystem
	 * is mounted. This is used on the client.
	 */
	public static int defaultGroup = 0;


	/**
	 * The Volume object. This is used on the client.
	 */
	public static Volume volume;

	/**
	 *
	 */
	public static double gcPFIncrement = .05;

	/**
	 * The Volume mount point. This is used on the client.
	 */
	public static String volumeMountPoint;

	/**
	 * The Threshold for removing unreferenced data from the hashtable
	 */
	public static long HT_RM_THRESH=15 * 60 * 1000;


	/**
	 * Determines whether dedup file map will be closed when the filesystem
	 * requests that the represented file is. This should be set to false if NFS
	 * is used because the opening and closing of dedup file maps is resource
	 * intensive. The default Linux NFS implementation closes and opens files on
	 * every open and read command. If this option is set to false files will be
	 * closed based on inactivity with maxInactiveFileTime @see
	 * maxInactiveFileTime or the maxOpenFiles parameter @see maxOpenFiles.
	 */
	public static boolean safeClose = true;
	/**
	 * Determines if, when the filesystem request a sync, if the deduped file
	 * will actually sync the underlying data. If set to true, this could
	 * severely impact performance and deduplication rates. It should be set to
	 * false unless needed. This is used on the client.
	 */
	public static boolean safeSync = true;
	/**
	 * The maximum about of time that a file is inactive before it is close.
	 * Inactivity is determined by the time the file was last accessed. @see
	 * com.annesam.sdfs.filestore.OpenFileMonitor . This is used on the client.
	 */
	public static int maxInactiveFileTime = 15 * 60 * 1000;
	/**
	 * Specifies whether the Dedup Storage Engine will store data to AWS S3 or
	 * not. This is set on the chunk store.
	 */
	public static boolean cloudChunkStore = false;
	/**
	 *
	 */
	public static String cloudBucket = null;
	/**
	 * The awsAccessKey. This is used on the client.
	 */
	public static String cloudAccessKey = null;
	/**
	 * The awsSecretKey. This is used on the client.
	 */
	public static String cloudSecretKey = null;
	public static String eCloudSecretKey = null;

	/**
	 * use Aim for AWS S3 authentication
	 */
	public static boolean useAim;

	/** Azure login info **/

	public static boolean AZUREChunkStore = false;
	public static String AZURE_ACCOUNT_NAME = "MyAccountName";
	public static String AZURE_ACCOUNT_KEY = "MyAccountKey";
	public static String AZURE_BLOB_HOST_NAME = null;

	/**
	 * Compress AWS data using speedy
	 */
	public static boolean compress = false;

	/**
	 * PreAllocates the size of the Dedup Storage Engine
	 */
	public static long chunkStoreAllocationSize = 536870912000L;

	/**
	 * Dedup Files by default
	 */
	public static boolean dedupFiles = false;

	/**
	 * The page size used for the Dedup Storage Engine. This should be the same
	 * as the Chunk Length used on the client side.
	 */
	public static int chunkStorePageSize = 4096;



	public static boolean disableAutoGC = false;

	/**
	 * hash type can be tiger or murmur
	 */
	public static String hashType = HashFunctionPool.MURMUR3_16;

	public static int dseIOThreads = 24;

	/**
	 * FDisk Schedule in cron format
	 *
	 * @see org.opendedup.sdfs.FDISKJob
	 */
	public static String fDkiskSchedule = "0 59 23 * * ?";

	public static boolean closedGracefully = true;
	public static boolean rebuildHashTable = false;

	public static boolean enableLookupFilter = false;
	public static String sdfsUserName = "admin";
	public static long maxAge = -1;
}
