package org.opendedup.sdfs.cluster.cmds;

/**
 * 
 * @author Sam Silverberg These are the commands that are sent by the client to
 *         the chunk store. The command is sent as the first byte in a command
 *         request. A typical client request is as follows :
 * 
 *         |command type (1b)|length of hash (2b)|md5 or sha hash (lenghth of
 *         hash)| command specific data (variable length)|
 * 
 */

public class NetworkCMDS {
	/** Fetch a chunk of data from the chunk store */
	public static final byte FETCH_CMD = 0;
	/** See if a hash already exists in the chunk store */
	public static final byte HASH_EXISTS_CMD = 1;
	/** write a chunk to the chunk store **/
	public static final byte WRITE_HASH_CMD = 2;
	/** Close the client thread used for this TCP connection */
	public static final byte QUIT_CMD = 3;
	/** Claim that the client is still using the hash in question */
	// public static final byte CLAIM_HASH = 4;
	/**
	 * Fetch a chunk and request that it is compressed before transmitting to
	 * the client. The data will be compressed by the chunk store before it is
	 * sent to the client.
	 */
	public static final byte FETCH_COMPRESSED_CMD = 5;
	/**
	 * Write a compressed chunk to the chunk server. The data will be compressed
	 * by the client before it is sent.
	 */
	public static final byte WRITE_COMPRESSED_CMD = 6;
	/** Keep alive ping command. Not used in this implementation */
	public static final byte PING_CMD = 9;
	public static final byte STORE_MAX_SIZE_CMD = 10;
	public static final byte STORE_SIZE_CMD = 11;
	public static final byte STORE_PAGE_SIZE = 12;
	public static final byte BULK_FETCH_CMD = 13;
	public static final byte UPDATE_DSE = 14;
	public static final byte RUN_FDISK=15;
	public static final byte RUN_CLAIM = 16;
	public static final byte RUN_REMOVE = 17;
	public static final byte LIST_VOLUMES = 18;
	public static final byte RM_VOLUME = 19;
	public static final byte ADD_VOLUME = 20;
	public static final byte BATCH_HASH_EXISTS_CMD = 21;
	public static final byte FIND_GC_MASTER_CMD = 22;
	public static final byte STOP_GC_MASTER_CMD = 23;
	public static final byte FIND_VOLUME_OWNER = 24;
	public static final byte GET_VOLUME_INFO = 25;
}
