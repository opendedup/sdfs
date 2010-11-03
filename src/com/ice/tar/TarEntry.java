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
import java.util.Date;


/**
 *
 * This class represents an entry in a Tar archive. It consists
 * of the entry's header, as well as the entry's File. Entries
 * can be instantiated in one of three ways, depending on how
 * they are to be used.
 * <p>
 * TarEntries that are created from the header bytes read from
 * an archive are instantiated with the TarEntry( byte[] )
 * constructor. These entries will be used when extracting from
 * or listing the contents of an archive. These entries have their
 * header filled in using the header bytes. They also set the File
 * to null, since they reference an archive entry not a file.
 * <p>
 * TarEntries that are created from Files that are to be written
 * into an archive are instantiated with the TarEntry( File )
 * constructor. These entries have their header filled in using
 * the File's information. They also keep a reference to the File
 * for convenience when writing entries.
 * <p>
 * Finally, TarEntries can be constructed from nothing but a name.
 * This allows the programmer to construct the entry by hand, for
 * instance when only an InputStream is available for writing to
 * the archive, and the header information is constructed from
 * other information. In this case the header fields are set to
 * defaults and the File is set to null.
 *
 * <pre>
 *
 * Original Unix Tar Header:
 *
 * Field  Field     Field
 * Width  Name      Meaning
 * -----  --------- ---------------------------
 *   100  name      name of file
 *     8  mode      file mode
 *     8  uid       owner user ID
 *     8  gid       owner group ID
 *    12  size      length of file in bytes
 *    12  mtime     modify time of file
 *     8  chksum    checksum for header
 *     1  link      indicator for links
 *   100  linkname  name of linked file
 *
 * </pre>
 *
 * <pre>
 *
 * POSIX "ustar" Style Tar Header:
 *
 * Field  Field     Field
 * Width  Name      Meaning
 * -----  --------- ---------------------------
 *   100  name      name of file
 *     8  mode      file mode
 *     8  uid       owner user ID
 *     8  gid       owner group ID
 *    12  size      length of file in bytes
 *    12  mtime     modify time of file
 *     8  chksum    checksum for header
 *     1  typeflag  type of file
 *   100  linkname  name of linked file
 *     6  magic     USTAR indicator
 *     2  version   USTAR version
 *    32  uname     owner user name
 *    32  gname     owner group name
 *     8  devmajor  device major number
 *     8  devminor  device minor number
 *   155  prefix    prefix for file name
 *
 * struct posix_header
 *   {                     byte offset
 *   char name[100];            0
 *   char mode[8];            100
 *   char uid[8];             108
 *   char gid[8];             116
 *   char size[12];           124
 *   char mtime[12];          136
 *   char chksum[8];          148
 *   char typeflag;           156
 *   char linkname[100];      157
 *   char magic[6];           257
 *   char version[2];         263
 *   char uname[32];          265
 *   char gname[32];          297
 *   char devmajor[8];        329
 *   char devminor[8];        337
 *   char prefix[155];        345
 *   };                       500
 *
 * </pre>
 *
 * Note that while the class does recognize GNU formatted headers,
 * it does not perform proper processing of GNU archives. I hope
 * to add the GNU support someday.
 *
 * Directory "size" fix contributed by:
 * Bert Becker <becker@informatik.hu-berlin.de>
 *
 * @see TarHeader
 * @author Timothy Gerard Endres, <time@gjt.org>
 */

public
class		TarEntry
extends		Object
implements	Cloneable
	{
	/**
	 * If this entry represents a File, this references it.
	 */
	protected File				file;

	/**
	 * This is the entry's header information.
	 */
	protected TarHeader			header;

	/**
	 * Set to true if this is a "old-unix" format entry.
	 */
	protected boolean			unixFormat;

	/**
	 * Set to true if this is a 'ustar' format entry.
	 */
	protected boolean			ustarFormat;

	/**
	 * Set to true if this is a GNU 'ustar' format entry.
	 */
	protected boolean			gnuFormat;


	/**
	 * The default constructor is protected for use only by subclasses.
	 */
	protected
	TarEntry()
		{
		}

	/**
	 * Construct an entry with only a name. This allows the programmer
	 * to construct the entry's header "by hand". File is set to null.
	 */
	public
	TarEntry( String name )
		{
		this.initialize();
		this.nameTarHeader( this.header, name );
		}

	/**
	 * Construct an entry for a file. File is set to file, and the
	 * header is constructed from information from the file.
	 *
	 * @param file The file that the entry represents.
	 */
	public
	TarEntry( File file )
		throws InvalidHeaderException
		{
		this.initialize();
		this.getFileTarHeader( this.header, file );
		}

	/**
	 * Construct an entry from an archive's header bytes. File is set
	 * to null.
	 *
	 * @param headerBuf The header bytes from a tar archive entry.
	 */
	public
	TarEntry( byte[] headerBuf )
		throws InvalidHeaderException
		{
		this.initialize();
		this.parseTarHeader( this.header, headerBuf );
		}

	/**
	 * Initialization code common to all constructors.
	 */
	private void
	initialize()
		{
		this.file = null;
		this.header = new TarHeader();

		this.gnuFormat = false;
		this.ustarFormat = true; // REVIEW What we prefer to use...
		this.unixFormat = false;
		}

	/**
	 * Clone the entry.
	 */
	public Object
	clone()
		{
		TarEntry entry = null;

		try {
			entry = (TarEntry) super.clone();

			if ( this.header != null )
				{
				entry.header = (TarHeader) this.header.clone();
				}

			if ( this.file != null )
				{
				entry.file = new File( this.file.getAbsolutePath() );
				}
			}
		catch ( CloneNotSupportedException ex )
			{
			ex.printStackTrace( System.err );
			}

		return entry;
		}

	/**
	 * Returns true if this entry's header is in "ustar" format.
	 *
	 * @return True if the entry's header is in "ustar" format.
	 */
	public boolean
	isUSTarFormat()
		{
		return this.ustarFormat;
		}

	/**
	 * Sets this entry's header format to "ustar".
	 */
	public void
	setUSTarFormat()
		{
		this.ustarFormat = true;
		this.gnuFormat = false;
		this.unixFormat = false;
		}

	/**
	 * Returns true if this entry's header is in the GNU 'ustar' format.
	 *
	 * @return True if the entry's header is in the GNU 'ustar' format.
	 */
	public boolean
	isGNUTarFormat()
		{
		return this.gnuFormat;
		}

	/**
	 * Sets this entry's header format to GNU "ustar".
	 */
	public void
	setGNUTarFormat()
		{
		this.gnuFormat = true;
		this.ustarFormat = false;
		this.unixFormat = false;
		}

	/**
	 * Returns true if this entry's header is in the old "unix-tar" format.
	 *
	 * @return True if the entry's header is in the old "unix-tar" format.
	 */
	public boolean
	isUnixTarFormat()
		{
		return this.unixFormat;
		}

	/**
	 * Sets this entry's header format to "unix-style".
	 */
	public void
	setUnixTarFormat()
		{
		this.unixFormat = true;
		this.ustarFormat = false;
		this.gnuFormat = false;
		}

	/**
	 * Determine if the two entries are equal. Equality is determined
	 * by the header names being equal.
	 *
	 * @return it Entry to be checked for equality.
	 * @return True if the entries are equal.
	 */
	public boolean
	equals( TarEntry it )
		{
		return
			this.header.name.toString().equals
				( it.header.name.toString() );
		}

	/**
	 * Determine if the given entry is a descendant of this entry.
	 * Descendancy is determined by the name of the descendant
	 * starting with this entry's name.
	 *
	 * @param desc Entry to be checked as a descendent of this.
	 * @return True if entry is a descendant of this.
	 */
	public boolean
	isDescendent( TarEntry desc )
		{
		return
			desc.header.name.toString().startsWith
				( this.header.name.toString() );
		}

	/**
	 * Get this entry's header.
	 *
	 * @return This entry's TarHeader.
	 */
	public TarHeader
	getHeader()
		{
		return this.header;
		}

	/**
	 * Get this entry's name.
	 *
	 * @return This entry's name.
	 */
	public String
	getName()
		{
		return this.header.name.toString();
		}

	/**
	 * Set this entry's name.
	 *
	 * @param name This entry's new name.
	 */
	public void
	setName( String name )
		{
		this.header.name =
			new StringBuffer( name );
		}

	/**
	 * Get this entry's user id.
	 *
	 * @return This entry's user id.
	 */
	public int
	getUserId()
		{
		return this.header.userId;
		}

	/**
	 * Set this entry's user id.
	 *
	 * @param userId This entry's new user id.
	 */
	public void
	setUserId( int userId )
		{
		this.header.userId = userId;
		}

	/**
	 * Get this entry's group id.
	 *
	 * @return This entry's group id.
	 */
	public int
	getGroupId()
		{
		return this.header.groupId;
		}

	/**
	 * Set this entry's group id.
	 *
	 * @param groupId This entry's new group id.
	 */
	public void
	setGroupId( int groupId )
		{
		this.header.groupId = groupId;
		}

	/**
	 * Get this entry's user name.
	 *
	 * @return This entry's user name.
	 */
	public String
	getUserName()
		{
		return this.header.userName.toString();
		}

	/**
	 * Set this entry's user name.
	 *
	 * @param userName This entry's new user name.
	 */
	public void
	setUserName( String userName )
		{
		this.header.userName =
			new StringBuffer( userName );
		}

	/**
	 * Get this entry's group name.
	 *
	 * @return This entry's group name.
	 */
	public String
	getGroupName()
		{
		return this.header.groupName.toString();
		}

	/**
	 * Set this entry's group name.
	 *
	 * @param groupName This entry's new group name.
	 */
	public void
	setGroupName( String groupName )
		{
		this.header.groupName =
			new StringBuffer( groupName );
		}

	/**
	 * Convenience method to set this entry's group and user ids.
	 *
	 * @param userId This entry's new user id.
	 * @param groupId This entry's new group id.
	 */
	public void
	setIds( int userId, int groupId )
		{
		this.setUserId( userId );
		this.setGroupId( groupId );
		}

	/**
	 * Convenience method to set this entry's group and user names.
	 *
	 * @param userName This entry's new user name.
	 * @param groupName This entry's new group name.
	 */
	public void
	setNames( String userName, String groupName )
		{
		this.setUserName( userName );
		this.setGroupName( groupName );
		}

	/**
	 * Set this entry's modification time. The parameter passed
	 * to this method is in "Java time".
	 *
	 * @param time This entry's new modification time.
	 */
	public void
	setModTime( long time )
		{
		this.header.modTime = time / 1000;
		}

	/**
	 * Set this entry's modification time.
	 *
	 * @param time This entry's new modification time.
	 */
	public void
	setModTime( Date time )
		{
		this.header.modTime = time.getTime() / 1000;
		}

	/**
	 * Set this entry's modification time.
	 *
	 * @param time This entry's new modification time.
	 */
	public Date
	getModTime()
		{
		return new Date( this.header.modTime * 1000 );
		}

	/**
	 * Get this entry's file.
	 *
	 * @return This entry's file.
	 */
	public File
	getFile()
		{
		return this.file;
		}

	/**
	 * Get this entry's file size.
	 *
	 * @return This entry's file size.
	 */
	public long
	getSize()
		{
		return this.header.size;
		}

	/**
	 * Set this entry's file size.
	 *
	 * @param size This entry's new file size.
	 */
	public void
	setSize( long size )
		{
		this.header.size = size;
		}

	/**
	 * Return whether or not this entry represents a directory.
	 *
	 * @return True if this entry is a directory.
	 */
	public boolean
	isDirectory()
		{
		if ( this.file != null )
			return this.file.isDirectory();

		if ( this.header != null )
			{
			if ( this.header.linkFlag == TarHeader.LF_DIR )
				return true;

			if ( this.header.name.toString().endsWith( "/" ) )
				return true;
			}

		return false;
		}

	/**
	 * Fill in a TarHeader with information from a File.
	 *
	 * @param hdr The TarHeader to fill in.
	 * @param file The file from which to get the header information.
	 */
	public void
	getFileTarHeader( TarHeader hdr, File file )
		throws InvalidHeaderException
		{
		this.file = file;

		String name = file.getPath();
		String osname = System.getProperty( "os.name" );
		if ( osname != null )
			{
			// Strip off drive letters!
			// REVIEW Would a better check be "(File.separator == '\')"?

			// String Win32Prefix = "Windows";
			// String prefix = osname.substring( 0, Win32Prefix.length() );
			// if ( prefix.equalsIgnoreCase( Win32Prefix ) )

			// if ( File.separatorChar == '\\' )

			// Windows OS check was contributed by
			// Patrick Beard <beard@netscape.com>
			String Win32Prefix = "windows";
			if ( osname.toLowerCase().startsWith( Win32Prefix ) )
				{
				if ( name.length() > 2 )
					{
					char ch1 = name.charAt(0);
					char ch2 = name.charAt(1);
					if ( ch2 == ':'
						&& ( (ch1 >= 'a' && ch1 <= 'z')
							|| (ch1 >= 'A' && ch1 <= 'Z') ) )
						{
						name = name.substring( 2 );
						}
					}
				}
			}

		name = name.replace( File.separatorChar, '/' );

		// No absolute pathnames
		// Windows (and Posix?) paths can start with "\\NetworkDrive\",
		// so we loop on starting /'s.
		
		for ( ; name.startsWith( "/" ) ; )
			name = name.substring( 1 );

 		hdr.linkName = new StringBuffer( "" );

		hdr.name = new StringBuffer( name );

		if ( file.isDirectory() )
			{
			hdr.size = 0;
			hdr.mode = 040755;
			hdr.linkFlag = TarHeader.LF_DIR;
			if ( hdr.name.charAt( hdr.name.length() - 1 ) != '/' )
				hdr.name.append( "/" );
			}
		else
			{
			hdr.size = file.length();
			hdr.mode = 0100644;
			hdr.linkFlag = TarHeader.LF_NORMAL;
			}

		// UNDONE When File lets us get the userName, use it!

		hdr.modTime = file.lastModified() / 1000;
		hdr.checkSum = 0;
		hdr.devMajor = 0;
		hdr.devMinor = 0;
		}

	/**
	 * If this entry represents a file, and the file is a directory, return
	 * an array of TarEntries for this entry's children.
	 *
	 * @return An array of TarEntry's for this entry's children.
	 */
	public TarEntry[]
	getDirectoryEntries()
		throws InvalidHeaderException
		{
		if ( this.file == null
				|| ! this.file.isDirectory() )
			{
			return new TarEntry[0];
			}

		String[] list = this.file.list();

		TarEntry[] result = new TarEntry[ list.length ];

		for ( int i = 0 ; i < list.length ; ++i )
			{
			result[i] =
				new TarEntry
					( new File( this.file, list[i] ) );
			}

		return result;
		}

	/**
	 * Compute the checksum of a tar entry header.
	 *
	 * @param buf The tar entry's header buffer.
	 * @return The computed checksum.
	 */
	public long
	computeCheckSum( byte[] buf )
		{
		long sum = 0;

		for ( int i = 0 ; i < buf.length ; ++i )
			{
			sum += 255 & buf[ i ];
			}

		return sum;
		}

	/**
	 * Write an entry's header information to a header buffer.
	 * This method can throw an InvalidHeaderException
	 *
	 * @param outbuf The tar entry header buffer to fill in.
	 * @throws InvalidHeaderException If the name will not fit in the header.
	 */
	public void
	writeEntryHeader( byte[] outbuf )
		throws InvalidHeaderException
		{
		int offset = 0;

		if ( this.isUnixTarFormat() )
			{
			if ( this.header.name.length() > 100 )
				throw new InvalidHeaderException
					( "file path is greater than 100 characters, "
						+ this.header.name );
			}

		offset = TarHeader.getFileNameBytes( this.header.name.toString(), outbuf );

		offset = TarHeader.getOctalBytes
			( this.header.mode, outbuf, offset, TarHeader.MODELEN );

		offset = TarHeader.getOctalBytes
			( this.header.userId, outbuf, offset, TarHeader.UIDLEN );

		offset = TarHeader.getOctalBytes
			( this.header.groupId, outbuf, offset, TarHeader.GIDLEN );

		long size = this.header.size;

		offset = TarHeader.getLongOctalBytes
			( size, outbuf, offset, TarHeader.SIZELEN );

		offset = TarHeader.getLongOctalBytes
			( this.header.modTime, outbuf, offset, TarHeader.MODTIMELEN );

		int csOffset = offset;
		for ( int c = 0 ; c < TarHeader.CHKSUMLEN ; ++c )
			outbuf[ offset++ ] = (byte) ' ';

		outbuf[ offset++ ] = this.header.linkFlag;

		offset = TarHeader.getNameBytes
			( this.header.linkName, outbuf, offset, TarHeader.NAMELEN );

		if ( this.unixFormat )
			{
			for ( int i = 0 ; i < TarHeader.MAGICLEN ; ++i )
				outbuf[ offset++ ] = 0;
			}
		else
			{
			offset = TarHeader.getNameBytes
				( this.header.magic, outbuf, offset, TarHeader.MAGICLEN );
			}

		offset = TarHeader.getNameBytes
			( this.header.userName, outbuf, offset, TarHeader.UNAMELEN );

		offset = TarHeader.getNameBytes
			( this.header.groupName, outbuf, offset, TarHeader.GNAMELEN );

		offset = TarHeader.getOctalBytes
			( this.header.devMajor, outbuf, offset, TarHeader.DEVLEN );

		offset = TarHeader.getOctalBytes
			( this.header.devMinor, outbuf, offset, TarHeader.DEVLEN );

		for ( ; offset < outbuf.length ; )
			outbuf[ offset++ ] = 0;

		long checkSum = this.computeCheckSum( outbuf );

		TarHeader.getCheckSumOctalBytes
			( checkSum, outbuf, csOffset, TarHeader.CHKSUMLEN );
		}

	/**
	 * Parse an entry's TarHeader information from a header buffer.
	 *
	 * Old unix-style code contributed by David Mehringer <dmehring@astro.uiuc.edu>.
	 *
	 * @param hdr The TarHeader to fill in from the buffer information.
	 * @param header The tar entry header buffer to get information from.
	 */
	public void
	parseTarHeader( TarHeader hdr, byte[] headerBuf )
		throws InvalidHeaderException
		{
		int offset = 0;

		//
		// NOTE Recognize archive header format.
		//
		if (       headerBuf[257] == 0
				&& headerBuf[258] == 0
				&& headerBuf[259] == 0
				&& headerBuf[260] == 0
				&& headerBuf[261] == 0 )
			{
			this.unixFormat = true;
			this.ustarFormat = false;
			this.gnuFormat = false;
			}
		else if (  headerBuf[257] == 'u'
				&& headerBuf[258] == 's'
				&& headerBuf[259] == 't'
				&& headerBuf[260] == 'a'
				&& headerBuf[261] == 'r'
				&& headerBuf[262] == 0 )
			{
			this.ustarFormat = true;
			this.gnuFormat = false;
			this.unixFormat = false;
			}
		else if (  headerBuf[257] == 'u'
				&& headerBuf[258] == 's'
				&& headerBuf[259] == 't'
				&& headerBuf[260] == 'a'
				&& headerBuf[261] == 'r'
				&& headerBuf[262] != 0
				&& headerBuf[263] != 0 )
			{
			// REVIEW
			this.gnuFormat = true;
			this.unixFormat = false;
			this.ustarFormat = false;
			}
		else
			{
			StringBuffer buf = new StringBuffer( 128 );

			buf.append( "header magic is not 'ustar' or unix-style zeros, it is '" );
			buf.append( headerBuf[257] );
			buf.append( headerBuf[258] );
			buf.append( headerBuf[259] );
			buf.append( headerBuf[260] );
			buf.append( headerBuf[261] );
			buf.append( headerBuf[262] );
			buf.append( headerBuf[263] );
			buf.append( "', or (dec) " );
			buf.append( (int)headerBuf[257] );
			buf.append( ", " );
			buf.append( (int)headerBuf[258] );
			buf.append( ", " );
			buf.append( (int)headerBuf[259] );
			buf.append( ", " );
			buf.append( (int)headerBuf[260] );
			buf.append( ", " );
			buf.append( (int)headerBuf[261] );
			buf.append( ", " );
			buf.append( (int)headerBuf[262] );
			buf.append( ", " );
			buf.append( (int)headerBuf[263] );

			throw new InvalidHeaderException( buf.toString() );
			}

		hdr.name = TarHeader.parseFileName( headerBuf );

		offset = TarHeader.NAMELEN;

		hdr.mode = (int)
			TarHeader.parseOctal( headerBuf, offset, TarHeader.MODELEN );

		offset += TarHeader.MODELEN;

		hdr.userId = (int)
			TarHeader.parseOctal( headerBuf, offset, TarHeader.UIDLEN );

		offset += TarHeader.UIDLEN;

		hdr.groupId = (int)
			TarHeader.parseOctal( headerBuf, offset, TarHeader.GIDLEN );

		offset += TarHeader.GIDLEN;

		hdr.size =
			TarHeader.parseOctal( headerBuf, offset, TarHeader.SIZELEN );

		offset += TarHeader.SIZELEN;

		hdr.modTime =
			TarHeader.parseOctal( headerBuf, offset, TarHeader.MODTIMELEN );

		offset += TarHeader.MODTIMELEN;

		hdr.checkSum = (int)
			TarHeader.parseOctal( headerBuf, offset, TarHeader.CHKSUMLEN );

		offset += TarHeader.CHKSUMLEN;

		hdr.linkFlag = headerBuf[ offset++ ];

		hdr.linkName =
			TarHeader.parseName( headerBuf, offset, TarHeader.NAMELEN );

		offset += TarHeader.NAMELEN;

		if ( this.ustarFormat )
			{
			hdr.magic =
				TarHeader.parseName( headerBuf, offset, TarHeader.MAGICLEN );

			offset += TarHeader.MAGICLEN;

			hdr.userName =
				TarHeader.parseName( headerBuf, offset, TarHeader.UNAMELEN );

			offset += TarHeader.UNAMELEN;

			hdr.groupName =
				TarHeader.parseName( headerBuf, offset, TarHeader.GNAMELEN );

			offset += TarHeader.GNAMELEN;

			hdr.devMajor = (int)
				TarHeader.parseOctal( headerBuf, offset, TarHeader.DEVLEN );

			offset += TarHeader.DEVLEN;

			hdr.devMinor = (int)
				TarHeader.parseOctal( headerBuf, offset, TarHeader.DEVLEN );
			}
		else
			{
			hdr.devMajor = 0;
			hdr.devMinor = 0;
			hdr.magic = new StringBuffer( "" );
			hdr.userName = new StringBuffer( "" );
			hdr.groupName = new StringBuffer( "" );
			}
		}

	/**
	 * Fill in a TarHeader given only the entry's name.
	 *
	 * @param hdr The TarHeader to fill in.
	 * @param name The tar entry name.
	 */
	public void
	nameTarHeader( TarHeader hdr, String name )
		{
		boolean isDir = name.endsWith( "/" );

		this.gnuFormat = false;
		this.ustarFormat = true;
		this.unixFormat = false;

		hdr.checkSum = 0;
		hdr.devMajor = 0;
		hdr.devMinor = 0;

		hdr.name = new StringBuffer( name );
		hdr.mode = isDir ? 040755 : 0100644;
		hdr.userId = 0;
		hdr.groupId = 0;
		hdr.size = 0;
		hdr.checkSum = 0;

		hdr.modTime =
			(new java.util.Date()).getTime() / 1000;

		hdr.linkFlag =
			isDir ? TarHeader.LF_DIR : TarHeader.LF_NORMAL;

		hdr.linkName = new StringBuffer( "" );
		hdr.userName = new StringBuffer( "" );
		hdr.groupName = new StringBuffer( "" );

		hdr.devMajor = 0;
		hdr.devMinor = 0;
		}

	public String
	toString()
		{
		StringBuffer result = new StringBuffer( 128 );
		return result.
			append( "[TarEntry name=" ).
			append( this.getName() ).
			append( ", isDir=" ).
			append( this.isDirectory() ).
			append( ", size=" ).
			append( this.getSize() ).
			append( ", userId=" ).
			append( this.getUserId() ).
			append( ", user=" ).
			append( this.getUserName() ).
			append( ", groupId=" ).
			append( this.getGroupId() ).
			append( ", group=" ).
			append( this.getGroupName() ).
			append( "]" ).
			toString();
		}

	}

