package org.opendedup.sdfs.io;

/**
 * Seek file position types.
 * 
 * <p>
 * Defines constants used by the SeekFile SMB request to specify where the seek
 * position is relative to.
 * 
 * @author gkspencer
 */
public class SeekType {

	// Seek file types

	public static final int StartOfFile = 0;
	public static final int CurrentPos = 1;
	public static final int EndOfFile = 2;
}