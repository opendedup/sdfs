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
import java.util.zip.GZIPInputStream;

/**
 * Special class designed to parse a Tar archive VERY FAST.
 * This class is not a general Tar archive solution because
 * it does not accomodate TarBuffer, or blocking. It does not
 * allow you to read the entries either. This would not be
 * difficult to add in a subclass.
 *
 * The real purpose of this class is that there are folks out
 * there who wish to parse an ENORMOUS tar archive, and maybe
 * only want to know the filenames, or they wish to locate the
 * offset of a particular entry so that can process that entry
 * with special code.
 *
 * @author Timothy Gerard Endres, <time@gjt.org>
 *
 */

public
class		FastTarStream
	{
	private boolean			debug = false;
	private boolean			hasHitEOF = false;
	private TarEntry		currEntry = null;
	private InputStream		inStream = null;
	private int				recordSize = TarBuffer.DEFAULT_RCDSIZE;


	public
	FastTarStream( InputStream in )
		{
		this( in, TarBuffer.DEFAULT_RCDSIZE );
		}

	public
	FastTarStream( InputStream in, int recordSize )
		{
		this.inStream = in;
		this.hasHitEOF = false;
		this.currEntry = null;
		this.recordSize = recordSize;
		}

	public void
	setDebug( boolean debug )
		{
		this.debug = debug;
		}

	public TarEntry
	getNextEntry()
		throws IOException
		{
		if ( this.hasHitEOF )
			return null;
		
		/**
 		 * Here I have tried skipping the entry size, I have tried
		 * skipping entrysize - header size,
         * entrysize + header, and all seem to skip to some bizzarelocation!
         */
		if ( this.currEntry != null && this.currEntry.getSize() > 0 )
			{
			// Need to round out the number of records to be read to skip entry...
			int numRecords =
				( (int)this.currEntry.getSize() + (this.recordSize - 1) )
					/ this.recordSize;

			if ( numRecords > 0 )
				{
                this.inStream.skip( numRecords * this.recordSize );
            	}
        	}

		byte[] headerBuf = new byte[ this.recordSize ];

		// NOTE Apparently (with GZIPInputStream maybe?) we are able to
		//      read less then record size bytes in any given read(). So,
		//      we have to be persistent.

		int bufIndex = 0;
		for ( int bytesNeeded = this.recordSize ; bytesNeeded > 0 ; )
			{
			int numRead = this.inStream.read( headerBuf, bufIndex, bytesNeeded );

			if ( numRead == -1 )
				{
				this.hasHitEOF = true;
				break;
				}

			bufIndex += numRead;
			bytesNeeded -= numRead;
			}

		// Check for "EOF block" of all zeros
		if ( ! this.hasHitEOF )
			{
			this.hasHitEOF = true;
			for ( int i = 0 ; i < headerBuf.length ; ++i )
				{
				if ( headerBuf[i] != 0 )
					{
					this.hasHitEOF = false;
					break;
					}
				}
			}

		if ( this.hasHitEOF )
			{
			this.currEntry = null;
			}
		else
			{
			try {
				this.currEntry = new TarEntry( headerBuf );

				if ( this.debug )
					{
					byte[] by = new byte[ headerBuf.length ];
					for(int i = 0; i < headerBuf.length; i++)
						{
						by[i] = ( headerBuf[i] == 0? 20: headerBuf[i] );
						}
					String s = new String( by );
					System.out.println( "\n" + s );
					}

				if ( ! ( headerBuf[257] == 'u' &&headerBuf[258] == 's'
						&& headerBuf[259] == 't' &&headerBuf[260] == 'a'
						&& headerBuf[261] == 'r' ) )
					{
					throw new InvalidHeaderException
						( "header magic is not'ustar', but '"
							+ headerBuf[257] +headerBuf[258] + headerBuf[259]
							+ headerBuf[260] +headerBuf[261] + "', or (dec) "
							+((int)headerBuf[257]) + ", "
							+((int)headerBuf[258]) + ", "
							+((int)headerBuf[259]) + ", "
							+((int)headerBuf[260]) + ", "
							+((int)headerBuf[261]) );
					}
				}
			catch ( InvalidHeaderException ex )
				{
				this.currEntry = null;
				throw ex;
				}
			}

		return this.currEntry;
		}

	public static void
	main( String[] args )
		{
		boolean debug = false;
		InputStream in = null;

		String fileName = args[0];

		try {
			int idx = 0;
			if ( args.length > 0 )
				{
				if ( args[idx].equals( "-d" ) )
					{
					debug = true;
					idx++;
					}

				if ( args[idx].endsWith( ".gz" )
						|| args[idx].endsWith( ".tgz" ) )
					{
					in = new GZIPInputStream( new FileInputStream( args[idx] ) );
					}
				else
					{
					in = new FileInputStream( args[idx] );
					}
				}
			else
				{
				in = System.in;
				}

			FastTarStream fts = new FastTarStream( in );
			fts.setDebug( debug );

			int nameWidth = 56;
			int sizeWidth = 9;
			int userWidth = 8;
			StringBuffer padBuf = new StringBuffer(128);
			for ( ; ; )
				{
				TarEntry entry = fts.getNextEntry();
				if ( entry == null )
					break;

				if ( entry.isDirectory() )
					{
					// TYPE
					System.out.print( "D " );

					// NAME
					padBuf.setLength(0);
					padBuf.append( entry.getName() );
					padBuf.setLength( padBuf.length() - 1 ); // drop '/'
					if ( padBuf.length() > nameWidth )
						padBuf.setLength( nameWidth );
					for ( ; padBuf.length() < nameWidth ; )
						padBuf.append( '_' );

					padBuf.append( '_' );
					System.out.print( padBuf.toString() );

					// SIZE
					padBuf.setLength(0);
					for ( ; padBuf.length() < sizeWidth ; )
						padBuf.insert( 0, '_' );

					padBuf.append( ' ' );
					System.out.print( padBuf.toString() );

					// USER
					padBuf.setLength(0);
					padBuf.append( entry.getUserName() );
					if ( padBuf.length() > userWidth )
						padBuf.setLength( userWidth );
					for ( ; padBuf.length() < userWidth ; )
						padBuf.append( ' ' );

					System.out.print( padBuf.toString() );
					}
				else
					{
					// TYPE
					System.out.print( "F " );

					// NAME
					padBuf.setLength(0);
					padBuf.append( entry.getName() );
					if ( padBuf.length() > nameWidth )
						padBuf.setLength( nameWidth );
					for ( ; padBuf.length() < nameWidth ; )
						padBuf.append( ' ' );

					padBuf.append( ' ' );
					System.out.print( padBuf.toString() );

					// SIZE
					padBuf.setLength(0);
					padBuf.append( entry.getSize() );
					if ( padBuf.length() > sizeWidth )
						padBuf.setLength( sizeWidth );
					for ( ; padBuf.length() < sizeWidth ; )
						padBuf.insert( 0, ' ' );

					padBuf.append( ' ' );
					System.out.print( padBuf.toString() );

					// USER
					padBuf.setLength(0);
					padBuf.append( entry.getUserName() );
					if ( padBuf.length() > userWidth )
						padBuf.setLength( userWidth );
					for ( ; padBuf.length() < userWidth ; )
						padBuf.append( ' ' );
					}

				System.out.println( "" );
				}
			}
		catch ( IOException ex )
			{
			ex.printStackTrace( System.err );
			}
		}

	}

