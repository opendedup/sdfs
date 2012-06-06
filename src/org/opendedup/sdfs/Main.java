package org.opendedup.sdfs;

import java.io.File;

import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.io.VolumeConfigWriterThread;
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
	public static VolumeConfigWriterThread wth = null;
	
	public static boolean firstRun = true;

	public static boolean logToConsole = false;

	public static String logPath = "/var/log/sdfs/sdfs.log";
	
	public static String sdfsCliUserName = "admin";
	
	public static String sdfsCliPassword = "";
	
	public static String sdfsCliSalt = "";
	
	public static boolean sdfsCliRequireAuth = false;
	public static int sdfsCliPort = 6442;
	public static boolean sdfsCliEnabled  = true;
	public static String sdfsCliListenAddr = "localhost";
	
	
	/**
	 * Upstream DSE Host for cache misses and replication
	 */
	public static String upStreamDSEHostName = null;
	
	/**
	 * Upstream DSE Host Port for cache misses and replication
	 */
	public static int upStreamDSEPort = 2222;
	
	/**
	 * Upstream Secondary DSE Host for cache
	 */
	public static boolean upStreamDSEHostEnabled = false;
	/**
	 * Maximum number op upstream hops
	 */
	public static short maxUpStreamDSEHops = 3;
	/**
	 * Maximum number op upstream hops
	 */
	public static String upStreamPassword = "admin";
	
	
	/**
	 * Class path when launching sdfs
	 */
	public static String classPath = "/usr/share/sdfs/lib/truezip-samples-7.3.2-jar-with-dependencies.jar:/usr/share/sdfs/lib/commons-collections-3.2.1.jar:/usr/share/sdfs/lib/sdfs.jar:/usr/share/sdfs/lib/jacksum.jar:/usr/share/sdfs/lib/slf4j-log4j12-1.5.10.jar:/usr/share/sdfs/lib/slf4j-api-1.5.10.jar:/usr/share/sdfs/lib/simple-4.1.21.jar:/usr/share/sdfs/lib/commons-io-1.4.jar:/usr/share/sdfs/lib/clhm-release-1.0-lru.jar:/usr/share/sdfs/lib/trove-3.0.0a3.jar:/usr/share/sdfs/lib/quartz-1.8.3.jar:/usr/share/sdfs/lib/log4j-1.2.15.jar:/usr/share/sdfs/lib/bcprov-jdk16-143.jar:/usr/share/sdfs/lib/commons-codec-1.3.jar:/usr/share/sdfs/lib/commons-httpclient-3.1.jar:/usr/share/sdfs/lib/commons-logging-1.1.1.jar:/usr/share/sdfs/lib/java-xmlbuilder-1.jar:/usr/share/sdfs/lib/jets3t-0.7.4.jar:/usr/share/sdfs/lib/commons-cli-1.2.jar";

	public static String javaOptions = "-Djava.library.path=/usr/share/sdfs/bin/ -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog -Dfuse.logging.level=INFO -server -XX:+UseG1GC";

	public static String javaPath = "/usr/share/sdfs/jre1.7.0/bin/java";

	/**
	 * The Version of SDFS this is
	 */
	public static String version = "1.1.5";

	/**
	 * The location where the actual blocks of deduplicated data will be
	 * located. This is used for the chunk stores.
	 */
	public static String chunkStore = "";

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
	public static String hashesDBClass = "org.opendedup.collections.CSByteArrayLongMap";
	/**
	 * Future implementation of pluggable garbageCollector
	 */
	public static String gcClass = "org.opendedup.sdfs.filestore.gc.PFullGC";

	/**
	 * Secret Key to Encrypt chunks in DSE.
	 */
	public static String chunkStoreEncryptionKey = "Password";
	/**
	 * whether encryption should be enabled for the DSE
	 */
	public static boolean chunkStoreEncryptionEnabled = false;

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
	public static int CHUNK_LENGTH = 4 * 1024;
	/**
	 * the default db user name. This is only used if the H2 database is being
	 * used instead of TC. This is used on the client and chunk store.
	 */
	public static String dbuserName = "sa";
	/**
	 * the default db user password. This is only used if the H2 database is
	 * being used instead of TC. This is used on the client and chunk store.
	 */
	public static String dbpassword = "sa";
	/**
	 * The version of the communication protocol being used for client <-> chunk
	 * store network communication.
	 */
	public static String PROTOCOL_VERSION = "1.1";
	/**
	 * The ping time used to keep client to chunk store network pipes open. This
	 * is used on the client.
	 */
	public static int PING_TIME = 15 * 1000;
	/**
	 * The number of chunks to keep in memory on the client for reading. The
	 * chunks are cached in an LRU hash table. This is used on the client.
	 */
	public static int systemReadCacheSize = 5000;
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

	public static int writeThreads = 8;
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
	 * The timeout from a udp client request before the request fails over to a
	 * TCP request. This is used on the client.
	 */
	public static int UDPClientTimeOut = 2000;
	/**
	 * Specifies if the client and chunk store can use UDP for communication.
	 * This is used on the client and the chunk store.
	 */
	public static boolean useUDP = false;
	/**
	 * The port the chunk store uses to listen of TCP and UDP connections. This
	 * is used on the chunk store.
	 */
	public static int serverPort = 2222;
	/**
	 * The host name or IP that the chunk store network port will listen on.
	 * This is used on the chunk store.
	 */
	public static String serverHostName = "0.0.0.0";
	/**
	 * The maximum number of results that a specific query will return if H2 is
	 * being used. This is used on the chunk store and the client.
	 */
	public static int maxReturnResults = 3000;
	/**
	 * The Volume object. This is used on the client.
	 */
	public static Volume volume;
	/**
	 * The Volume mount point. This is used on the client.
	 */
	public static String volumeMountPoint;
	
	/**
	 * Enable the DSE Network Server
	 */
	public static boolean enableNetworkDSEServer = false;

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
	public static boolean AWSChunkStore = false;
	/**
	 * 
	 */
	public static String awsBucket = null;
	/**
	 * The awsAccessKey. This is used on the client.
	 */
	public static String awsAccessKey = null;
	/**
	 * The awsSecretKey. This is used on the client.
	 */
	public static String awsSecretKey = null;
	/**
	 * Compress AWS data using zlib
	 */
	public static boolean awsCompress = true;
	/**
	 * The time out on the client to wait for a read or write command to finish
	 * for the same hash. This is used to limit the communication between the
	 * client and the chunk store when specific sections of the same chunk are
	 * requested through separate read threads. It is epecially useful when AWS
	 * is used to store chunks since it multiple simultainous requests for the
	 * same chunk will cost in IO throughput and money. This is used on the
	 * client.
	 */
	public static int multiReadTimeout = 1000;

	/**
	 * Pre-Allocates space for the TC datables on the chunk store. This is
	 * specified per hash store and not for all hashes held. Typically this
	 * number should be (maximum number of hashes held)/256 . Setting this too
	 * high will impact initial storage needed to preallocate space for the TC
	 * databases. Setting this number too low will impact performance. The total
	 * number of hashes needed can be computed by (expected total
	 * capacity)/(average chunk size). This is set on the chunk store.
	 */
	public static int entriesPerDB = 30000000;

	/**
	 * Determines whether the chunk store will be pre-allocated or not.
	 */

	public static boolean preAllocateChunkStore = true;

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

	/**
	 * The number of pages to read ahead during a normal read. This will usually
	 * speed up reads quite a bit.
	 */
	public static int chunkStoreReadAheadPages = 4;

	/**
	 * The size (MB) of pages (HashChunks) to cache for reading.
	 */
	public static int chunkStorePageCache = 5;

	/**
	 * The time in milliseconds for a page cache to timeout while waiting for a
	 * chunck to be read.
	 */
	public static int chunkStoreDirtyCacheTimeout = 1000;

	/**
	 * If the Dedup Storage Engine is remote or local
	 */
	public static boolean chunkStoreLocal = false;

	/**
	 * If the Dedup Storage Engine is remote or local
	 */
	public static boolean enableNetworkChunkStore = false;

	/**
	 * the length of the hash. Will be either 16 or 24 depending on md/tiger128
	 * or tiger192
	 */
	public static short hashLength = 16;
	/**
	 * FDisk Schedule in cron format
	 * 
	 * @see org.opendedup.sdfs.FDISKJob
	 */
	public static String fDkiskSchedule = "0 0 0/1 * * ?";
	/**
	 * Remove chunks schedule
	 * 
	 * @see org.opendedup.sdfs.RemoveChunksJob
	 */
	public static String gcChunksSchedule = "0 0 0/2 * * ?";

	/**
	 * Age, if older than, that data will be evicted from the Dedup Storage
	 * Engine
	 */
	public static int evictionAge = 3;
	/**
	 * Compressed Index
	 */
	public static boolean compressedIndex = false;

	public static boolean closedGracefully = true;

}
