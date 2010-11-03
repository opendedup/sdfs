/*
** Authored by Timothy Gerard Endres
** <mailto:time@gjt.org>  <http://www.trustice.com>
** 
** This work has been placed into the public domain.
** You may use this work in any way and for any purpose you wish.
**
** THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY OF ANY KIND,
** NOT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY. THE AUTHOR
** OF THIS SOFTWARE, ASSUMES _NO_ RESPONSIBILITY FOR ANY
** CONSEQUENCE RESULTING FROM THE USE, MODIFICATION, OR
** REDISTRIBUTION OF THIS SOFTWARE. 
** 
*/

package com.ice.tar;

import java.io.*;
import javax.activation.*;


/**
 * The TarInputStream reads a UNIX tar archive as an InputStream.
 * methods are provided to position at each successive entry in
 * the archive, and the read each entry as a normal input stream
 * using read().
 *
 * Kerry Menzel <kmenzel@cfl.rr.com> Contributed the code to support
 * file sizes greater than 2GB (longs versus ints).
 *
 *
 * @version $Revision: 1.9 $
 * @author Timothy Gerard Endres, <time@gjt.org>
 * @see TarBuffer
 * @see TarHeader
 * @see TarEntry
 */


public
class		TarInputStream
extends		FilterInputStream
	{
	protected boolean			debug;
	protected boolean			hasHitEOF;

	protected long				entrySize;
	protected long				entryOffset;

	protected byte[]			oneBuf;
	protected byte[]			readBuf;

	protected TarBuffer			buffer;

	protected TarEntry			currEntry;

	protected EntryFactory		eFactory;


	public
	TarInputStream( InputStream is )
		{
		this( is, TarBuffer.DEFAULT_BLKSIZE, TarBuffer.DEFAULT_RCDSIZE );
		}

	public
	TarInputStream( InputStream is, int blockSize )
		{
		this( is, blockSize, TarBuffer.DEFAULT_RCDSIZE );
		}

	public
	TarInputStream( InputStream is, int blockSize, int recordSize )
		{
		super( is );

		this.buffer = new TarBuffer( is, blockSize, recordSize );

		this.readBuf = null;
		this.oneBuf = new byte[1];
		this.debug = false;
		this.hasHitEOF = false;
		this.eFactory = null;
		}

	/**
	 * Sets the debugging flag.
	 *
	 * @param debugF True to turn on debugging.
	 */
	public void
	setDebug( boolean debugF )
		{
		this.debug = debugF;
		}

	/**
	 * Sets the debugging flag.
	 *
	 * @param debugF True to turn on debugging.
	 */
	public void
	setEntryFactory( EntryFactory factory )
		{
		this.eFactory = factory;
		}

	/**
	 * Sets the debugging flag in this stream's TarBuffer.
	 *
	 * @param debugF True to turn on debugging.
	 */
	public void
	setBufferDebug( boolean debug )
		{
		this.buffer.setDebug( debug );
		}

	/**
	 * Closes this stream. Calls the TarBuffer's close() method.
	 */
	public void
	close()
		throws IOException
		{
		this.buffer.close();
		}

	/**
	 * Get the record size being used by this stream's TarBuffer.
	 *
	 * @return The TarBuffer record size.
	 */
	public int
	getRecordSize()
		{
		return this.buffer.getRecordSize();
		}

	/**
	 * Get the available data that can be read from the current
	 * entry in the archive. This does not indicate how much data
	 * is left in the entire archive, only in the current entry.
	 * This value is determined from the entry's size header field
	 * and the amount of data already read from the current entry.
	 * 
	 *
	 * @return The number of available bytes for the current entry.
	 */
	public int
	available()
		throws IOException
		{
		return (int)(this.entrySize - this.entryOffset);
		}

	/**
	 * Skip bytes in the input buffer. This skips bytes in the
	 * current entry's data, not the entire archive, and will
	 * stop at the end of the current entry's data if the number
	 * to skip extends beyond that point.
	 *
	 * @param numToSkip The number of bytes to skip.
	 * @return The actual number of bytes skipped.
	 */
	public long
	skip( long numToSkip )
		throws IOException
		{
		// REVIEW
		// This is horribly inefficient, but it ensures that we
		// properly skip over bytes via the TarBuffer...
		//

		byte[] skipBuf = new byte[ 8 * 1024 ];
        long num = numToSkip;
		for ( ; num > 0 ; )
			{
			int numRead =
				this.read( skipBuf, 0,
					( num > skipBuf.length ? skipBuf.length : (int) num ) );

			if ( numRead == -1 )
				break;

			num -= numRead;
			}

		return ( numToSkip - num );
		}

	/**
	 * Since we do not support marking just yet, we return false.
	 *
	 * @return False.
	 */
	public boolean
	markSupported()
		{
		return false;
		}

	/**
	 * Since we do not support marking just yet, we do nothing.
	 *
	 * @param markLimit The limit to mark.
	 */
	public void
	mark( int markLimit )
		{
		}

	/**
	 * Since we do not support marking just yet, we do nothing.
	 */
	public void
	reset()
		{
		}

	/**
	 * Get the number of bytes into the current TarEntry.
	 * This method returns the number of bytes that have been read
	 * from the current TarEntry's data.
	 *
	 * @returns The current entry offset.
	 */

	public long
	getEntryPosition()
		{
		return this.entryOffset;
		}

	/**
	 * Get the number of bytes into the stream we are currently at.
	 * This method accounts for the blocking stream that tar uses,
	 * so it represents the actual position in input stream, as
	 * opposed to the place where the tar archive parsing is.
	 *
	 * @returns The current file pointer.
	 */

	public long
	getStreamPosition()
		{
		return ( buffer.getBlockSize() * buffer.getCurrentBlockNum() )
					+ buffer.getCurrentRecordNum();
		}

	/**
	 * Get the next entry in this tar archive. This will skip
	 * over any remaining data in the current entry, if there
	 * is one, and place the input stream at the header of the
	 * next entry, and read the header and instantiate a new
	 * TarEntry from the header bytes and return that entry.
	 * If there are no more entries in the archive, null will
	 * be returned to indicate that the end of the archive has
	 * been reached.
	 *
	 * @return The next TarEntry in the archive, or null.
	 */
	public TarEntry
	getNextEntry()
		throws IOException
		{
		if ( this.hasHitEOF )
			return null;

		if ( this.currEntry != null )
			{
			long numToSkip = (this.entrySize - this.entryOffset);

			if ( this.debug )
			System.err.println
				( "TarInputStream: SKIP currENTRY '"
				+ this.currEntry.getName() + "' SZ "
				+ this.entrySize + " OFF " + this.entryOffset
				+ "  skipping " + numToSkip + " bytes" );

			if ( numToSkip > 0 )
				{
				this.skip( numToSkip );
				}

			this.readBuf = null;
			}

		byte[] headerBuf = this.buffer.readRecord();

		if ( headerBuf == null )
			{
			if ( this.debug )
				{
				System.err.println( "READ NULL RECORD" );
				}

			this.hasHitEOF = true;
			}
		else if ( this.buffer.isEOFRecord( headerBuf ) )
			{
			if ( this.debug )
				{
				System.err.println( "READ EOF RECORD" );
				}

			this.hasHitEOF = true;
			}

		if ( this.hasHitEOF )
			{
			this.currEntry = null;
			}
		else
			{
			try {
				if ( this.eFactory == null )
					{
					this.currEntry = new TarEntry( headerBuf );
					}
				else
					{
					this.currEntry =
						this.eFactory.createEntry( headerBuf );
					}

				if ( this.debug )
				System.err.println
					( "TarInputStream: SET CURRENTRY '"
						+ this.currEntry.getName()
						+ "' size = " + this.currEntry.getSize() );

				this.entryOffset = 0;
				this.entrySize = this.currEntry.getSize();
				}
			catch ( InvalidHeaderException ex )
				{
				this.entrySize = 0;
				this.entryOffset = 0;
				this.currEntry = null;
				throw new InvalidHeaderException
					( "bad header in block "
						+ this.buffer.getCurrentBlockNum()
						+ " record "
						+ this.buffer.getCurrentRecordNum()
						+ ", " + ex.getMessage() );
				}
			}

		return this.currEntry;
		}

	/**
	 * Reads a byte from the current tar archive entry.
	 *
	 * This method simply calls read( byte[], int, int ).
	 *
	 * @return The byte read, or -1 at EOF.
	 */
	public int
	read()
		throws IOException
		{
		int num = this.read( this.oneBuf, 0, 1 );
		if ( num == -1 )
			return num;
		else
			return (int) this.oneBuf[0];
		}

	/**
	 * Reads bytes from the current tar archive entry.
	 *
	 * This method simply calls read( byte[], int, int ).
	 *
	 * @param buf The buffer into which to place bytes read.
	 * @return The number of bytes read, or -1 at EOF.
	 */
	public int
	read( byte[] buf )
		throws IOException
		{
		return this.read( buf, 0, buf.length );
		}

	/**
	 * Reads bytes from the current tar archive entry.
	 *
	 * This method is aware of the boundaries of the current
	 * entry in the archive and will deal with them as if they
	 * were this stream's start and EOF.
	 *
	 * @param buf The buffer into which to place bytes read.
	 * @param offset The offset at which to place bytes read.
	 * @param numToRead The number of bytes to read.
	 * @return The number of bytes read, or -1 at EOF.
	 */
	public int
	read( byte[] buf, int offset, int numToRead )
		throws IOException
		{
		int totalRead = 0;

		if ( this.entryOffset >= this.entrySize )
			return -1;

		if ( (numToRead + this.entryOffset) > this.entrySize )
			{
			numToRead = (int) (this.entrySize - this.entryOffset);
			}

		if ( this.readBuf != null )
			{
			int sz = ( numToRead > this.readBuf.length )
						? this.readBuf.length : numToRead;

			System.arraycopy( this.readBuf, 0, buf, offset, sz );

			if ( sz >= this.readBuf.length )
				{
				this.readBuf = null;
				}
			else
				{
				int newLen = this.readBuf.length - sz;
				byte[] newBuf = new byte[ newLen ];
				System.arraycopy( this.readBuf, sz, newBuf, 0, newLen );
				this.readBuf = newBuf;
				}

			totalRead += sz;
			numToRead -= sz;
			offset += sz;
			}

		for ( ; numToRead > 0 ; )
			{
			byte[] rec = this.buffer.readRecord();
			if ( rec == null )
				{
				// Unexpected EOF!
				throw new IOException
					( "unexpected EOF with " + numToRead + " bytes unread" );
				}

			int sz = numToRead;
			int recLen = rec.length;

			if ( recLen > sz )
				{
				System.arraycopy( rec, 0, buf, offset, sz );
				this.readBuf = new byte[ recLen - sz ];
				System.arraycopy( rec, sz, this.readBuf, 0, recLen - sz );
				}
			else
				{
				sz = recLen;
				System.arraycopy( rec, 0, buf, offset, recLen );
				}

			totalRead += sz;
			numToRead -= sz;
			offset += sz;
			}

		this.entryOffset += totalRead;

		return totalRead;
		}

	/**
	 * Copies the contents of the current tar archive entry directly into
	 * an output stream.
	 *
	 * @param out The OutputStream into which to write the entry's data.
	 */
	public void
	copyEntryContents( OutputStream out )
		throws IOException
		{
		byte[] buf = new byte[ 32 * 1024 ];

		for ( ; ; )
			{
			int numRead = this.read( buf, 0, buf.length );
			if ( numRead == -1 )
				break;
			out.write( buf, 0, numRead );
			}
		}

	/**
	 * This interface is provided, with the method setEntryFactory(), to allow
	 * the programmer to have their own TarEntry subclass instantiated for the
	 * entries return from getNextEntry().
	 */

	public
	interface	EntryFactory
		{
		public TarEntry
			createEntry( String name );

		public TarEntry
			createEntry( File path )
				throws InvalidHeaderException;

		public TarEntry
			createEntry( byte[] headerBuf )
				throws InvalidHeaderException;
		}

	public
	class		EntryAdapter
	implements	EntryFactory
		{
		public TarEntry
		createEntry( String name )
			{
			return new TarEntry( name );
			}

		public TarEntry
		createEntry( File path )
			throws InvalidHeaderException
			{
			return new TarEntry( path );
			}

		public TarEntry
		createEntry( byte[] headerBuf )
			throws InvalidHeaderException
			{
			return new TarEntry( headerBuf );
			}
		}

	}


