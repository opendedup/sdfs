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

/**
 * This class encapsulates the Tar Entry Header used in Tar Archives.
 * The class also holds a number of tar constants, used mostly in headers.
 *
 * @author Timothy Gerard Endres, <time@gjt.org>
 */

public
class		TarHeader
extends		Object
implements	Cloneable
	{
	/**
	 * The length of the name field in a header buffer.
	 */
	public static final int		NAMELEN = 100;
	/**
	 * The offset of the name field in a header buffer.
	 */
	public static final int		NAMEOFFSET = 0;
	/**
	 * The length of the name prefix field in a header buffer.
	 */
	public static final int		PREFIXLEN = 155;
	/**
	 * The offset of the name prefix field in a header buffer.
	 */
	public static final int		PREFIXOFFSET = 345;
	/**
	 * The length of the mode field in a header buffer.
	 */
	public static final int		MODELEN = 8;
	/**
	 * The length of the user id field in a header buffer.
	 */
	public static final int		UIDLEN = 8;
	/**
	 * The length of the group id field in a header buffer.
	 */
	public static final int		GIDLEN = 8;
	/**
	 * The length of the checksum field in a header buffer.
	 */
	public static final int		CHKSUMLEN = 8;
	/**
	 * The length of the size field in a header buffer.
	 */
	public static final int		SIZELEN = 12;
	/**
	 * The length of the magic field in a header buffer.
	 */
	public static final int		MAGICLEN = 8;
	/**
	 * The length of the modification time field in a header buffer.
	 */
	public static final int		MODTIMELEN = 12;
	/**
	 * The length of the user name field in a header buffer.
	 */
	public static final int		UNAMELEN = 32;
	/**
	 * The length of the group name field in a header buffer.
	 */
	public static final int		GNAMELEN = 32;
	/**
	 * The length of the devices field in a header buffer.
	 */
	public static final int		DEVLEN = 8;

	/**
	 * LF_ constants represent the "link flag" of an entry, or more commonly,
	 * the "entry type". This is the "old way" of indicating a normal file.
	 */
	public static final byte	LF_OLDNORM	= 0;
	/**
	 * Normal file type.
	 */
	public static final byte	LF_NORMAL	= (byte) '0';
	/**
	 * Link file type.
	 */
	public static final byte	LF_LINK		= (byte) '1';
	/**
	 * Symbolic link file type.
	 */
	public static final byte	LF_SYMLINK	= (byte) '2';
	/**
	 * Character device file type.
	 */
	public static final byte	LF_CHR		= (byte) '3';
	/**
	 * Block device file type.
	 */
	public static final byte	LF_BLK		= (byte) '4';
	/**
	 * Directory file type.
	 */
	public static final byte	LF_DIR		= (byte) '5';
	/**
	 * FIFO (pipe) file type.
	 */
	public static final byte	LF_FIFO		= (byte) '6';
	/**
	 * Contiguous file type.
	 */
	public static final byte	LF_CONTIG	= (byte) '7';

	/**
	 * The magic tag representing a POSIX tar archive.
	 */
	public static final String	TMAGIC		= "ustar";

	/**
	 * The magic tag representing a GNU tar archive.
	 */
	public static final String	GNU_TMAGIC	= "ustar  ";

	/**
	 * The entry's name.
	 */
	public StringBuffer		name;
	/**
	 * The entry's permission mode.
	 */
	public int				mode;
	/**
	 * The entry's user id.
	 */
	public int				userId;
	/**
	 * The entry's group id.
	 */
	public int				groupId;
	/**
	 * The entry's size.
	 */
	public long				size;
	/**
	 * The entry's modification time.
	 */
	public long				modTime;
	/**
	 * The entry's checksum.
	 */
	public int				checkSum;
	/**
	 * The entry's link flag.
	 */
	public byte				linkFlag;
	/**
	 * The entry's link name.
	 */
	public StringBuffer		linkName;
	/**
	 * The entry's magic tag.
	 */
	public StringBuffer		magic;
	/**
	 * The entry's user name.
	 */
	public StringBuffer		userName;
	/**
	 * The entry's group name.
	 */
	public StringBuffer		groupName;
	/**
	 * The entry's major device number.
	 */
	public int				devMajor;
	/**
	 * The entry's minor device number.
	 */
	public int				devMinor;


	public
	TarHeader()
		{
		this.magic = new StringBuffer( TarHeader.TMAGIC );

		this.name = new StringBuffer();
		this.linkName = new StringBuffer();

		String user =
			System.getProperty( "user.name", "" );

		if ( user.length() > 31 )
			user = user.substring( 0, 31 );

		this.userId = 0;
		this.groupId = 0;
		this.userName = new StringBuffer( user );
		this.groupName = new StringBuffer( "" );
		}

	/**
	 * TarHeaders can be cloned.
	 */
	public Object
	clone()
		{
		TarHeader hdr = null;

		try {
			hdr = (TarHeader) super.clone();

			hdr.name =
				(this.name == null ) ? null
					: new StringBuffer( this.name.toString() );
			hdr.mode = this.mode;
			hdr.userId = this.userId;
			hdr.groupId = this.groupId;
			hdr.size = this.size;
			hdr.modTime = this.modTime;
			hdr.checkSum = this.checkSum;
			hdr.linkFlag = this.linkFlag;
			hdr.linkName =
				(this.linkName == null ) ? null
					: new StringBuffer( this.linkName.toString() );
			hdr.magic =
				(this.magic == null ) ? null
					: new StringBuffer( this.magic.toString() );
			hdr.userName =
				(this.userName == null ) ? null
					: new StringBuffer( this.userName.toString() );
			hdr.groupName =
				(this.groupName == null ) ? null
					: new StringBuffer( this.groupName.toString() );
			hdr.devMajor = this.devMajor;
			hdr.devMinor = this.devMinor;
			}
		catch ( CloneNotSupportedException ex )
			{
			ex.printStackTrace( System.err );
			}

		return hdr;
		}

	/**
	 * Get the name of this entry.
	 *
	 * @return Teh entry's name.
	 */
	public String
	getName()
		{
		return this.name.toString();
		}

	/**
	 * Parse an octal string from a header buffer. This is used for the
	 * file permission mode value.
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The long value of the octal string.
	 */
	public static long
	parseOctal( byte[] header, int offset, int length )
		throws InvalidHeaderException
		{
		long result = 0;
		boolean stillPadding = true;

		int end = offset + length;
		for ( int i = offset ; i < end ; ++i )
			{
			if ( header[i] == 0 )
				break;

			if ( header[i] == (byte) ' ' || header[i] == '0' )
				{
				if ( stillPadding )
					continue;

				if ( header[i] == (byte) ' ' )
					break;
				}
			
			stillPadding = false;

			result =
				(result << 3)
					+ (header[i] - '0');
			}

		return result;
		}

	/**
	 * Parse a file name from a header buffer. This is different from
	 * parseName() in that is recognizes 'ustar' names and will handle
	 * adding on the "prefix" field to the name.
	 *
	 * Contributed by Dmitri Tikhonov <dxt2431@yahoo.com>
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The header's entry name.
	 */
	public static StringBuffer
	parseFileName( byte[] header )
		{
		StringBuffer result = new StringBuffer( 256 );

		// If header[345] is not equal to zero, then it is the "prefix"
		// that 'ustar' defines. It must be prepended to the "normal"
		// name field. We are responsible for the separating '/'.
		//
		if ( header[345] != 0 )
			{
			for ( int i = 345 ; i < 500 && header[i] != 0 ; ++i )
				{
				result.append( (char)header[i] );
				}

			result.append( "/" );
			}

		for ( int i = 0 ; i < 100 && header[i] != 0 ; ++i )
			{
			result.append( (char)header[i] );
			}

		return result;
		}

	/**
	 * Parse an entry name from a header buffer.
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The header's entry name.
	 */
	public static StringBuffer
	parseName( byte[] header, int offset, int length )
		throws InvalidHeaderException
		{
		StringBuffer result = new StringBuffer( length );

		int end = offset + length;
		for ( int i = offset ; i < end ; ++i )
			{
			if ( header[i] == 0 )
				break;
			result.append( (char)header[i] );
			}

		return result;
		}

	/**
	 * This method, like getNameBytes(), is intended to place a name
	 * into a TarHeader's buffer. However, this method is sophisticated
	 * enough to recognize long names (name.length() > NAMELEN). In these
	 * cases, the method will break the name into a prefix and suffix and
	 * place the name in the header in 'ustar' format. It is up to the
	 * TarEntry to manage the "entry header format". This method assumes
	 * the name is valid for the type of archive being generated.
	 *
	 * @param outbuf The buffer containing the entry header to modify.
	 * @param newName The new name to place into the header buffer.
	 * @return The current offset in the tar header (always TarHeader.NAMELEN).
	 * @throws InvalidHeaderException If the name will not fit in the header.
	 */
	public static int
	getFileNameBytes( String newName, byte[] outbuf )
		throws InvalidHeaderException
		{
		if ( newName.length() > 100 )
			{
			// Locate a pathname "break" prior to the maximum name length...
			int index = newName.indexOf( "/", newName.length() - 100 );
			if ( index == -1 )
				throw new InvalidHeaderException
					( "file name is greater than 100 characters, " + newName );

			// Get the "suffix subpath" of the name.
			String name = newName.substring( index + 1 );

			// Get the "prefix subpath", or "prefix", of the name.
			String prefix = newName.substring( 0, index );
			if ( prefix.length() > TarHeader.PREFIXLEN )
				throw new InvalidHeaderException
					( "file prefix is greater than 155 characters" );

			TarHeader.getNameBytes
				( new StringBuffer( name ), outbuf,
					TarHeader.NAMEOFFSET, TarHeader.NAMELEN );

			TarHeader.getNameBytes
				( new StringBuffer( prefix ), outbuf,
					TarHeader.PREFIXOFFSET, TarHeader.PREFIXLEN );
			}
		else
			{
			TarHeader.getNameBytes
				( new StringBuffer( newName ), outbuf,
					TarHeader.NAMEOFFSET, TarHeader.NAMELEN );
			}

		// The offset, regardless of the format, is now the end of the
		// original name field.
		//
		return TarHeader.NAMELEN;
		}

	/**
	 * Move the bytes from the name StringBuffer into the header's buffer.
	 *
	 * @param header The header buffer into which to copy the name.
	 * @param offset The offset into the buffer at which to store.
	 * @param length The number of header bytes to store.
	 * @return The new offset (offset + length).
	 */
	public static int
	getNameBytes( StringBuffer name, byte[] buf, int offset, int length )
		{
		int i;

		for ( i = 0 ; i < length && i < name.length() ; ++i )
			{
			buf[ offset + i ] = (byte) name.charAt( i );
			}

		for ( ; i < length ; ++i )
			{
			buf[ offset + i ] = 0;
			}

		return offset + length;
		}

	/**
	 * Parse an octal integer from a header buffer.
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The integer value of the octal bytes.
	 */
	public static int
	getOctalBytes( long value, byte[] buf, int offset, int length )
		{
		byte[] result = new byte[ length ];

		int idx = length - 1;

		buf[ offset + idx ] = 0;
		--idx;
		buf[ offset + idx ] = (byte) ' ';
		--idx;

		if ( value == 0 )
			{
			buf[ offset + idx ] = (byte) '0';
			--idx;
			}
		else
			{
			for ( long val = value ; idx >= 0 && val > 0 ; --idx )
				{
				buf[ offset + idx ] = (byte)
					( (byte) '0' + (byte) (val & 7) );
				val = val >> 3;
				}
			}

		for ( ; idx >= 0 ; --idx )
			{
			buf[ offset + idx ] = (byte) ' ';
			}

		return offset + length;
		}

	/**
	 * Parse an octal long integer from a header buffer.
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The long value of the octal bytes.
	 */
	public static int
	getLongOctalBytes( long value, byte[] buf, int offset, int length )
		{
		byte[] temp = new byte[ length + 1 ];
		TarHeader.getOctalBytes( value, temp, 0, length + 1 );
		System.arraycopy( temp, 0, buf, offset, length );
		return offset + length;
		}

	/**
	 * Parse the checksum octal integer from a header buffer.
	 *
	 * @param header The header buffer from which to parse.
	 * @param offset The offset into the buffer from which to parse.
	 * @param length The number of header bytes to parse.
	 * @return The integer value of the entry's checksum.
	 */
	public static int
	getCheckSumOctalBytes( long value, byte[] buf, int offset, int length )
		{
		TarHeader.getOctalBytes( value, buf, offset, length );
		buf[ offset + length - 1 ] = (byte) ' ';
		buf[ offset + length - 2 ] = 0;
		return offset + length;
		}

	}
 
