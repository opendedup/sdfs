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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.activation.FileTypeMap;
import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;


/**
 * The TarArchive class implements the concept of a
 * tar archive. A tar archive is a series of entries, each of
 * which represents a file system object. Each entry in
 * the archive consists of a header record. Directory entries
 * consist only of the header record, and are followed by entries
 * for the directory's contents. File entries consist of a
 * header record followed by the number of records needed to
 * contain the file's contents. All entries are written on
 * record boundaries. Records are 512 bytes long.
 *
 * TarArchives are instantiated in either read or write mode,
 * based upon whether they are instantiated with an InputStream
 * or an OutputStream. Once instantiated TarArchives read/write
 * mode can not be changed.
 *
 * There is currently no support for random access to tar archives.
 * However, it seems that subclassing TarArchive, and using the
 * TarBuffer.getCurrentRecordNum() and TarBuffer.getCurrentBlockNum()
 * methods, this would be rather trvial.
 *
 * @version $Revision: 1.15 $
 * @author Timothy Gerard Endres, <time@gjt.org>
 * @see TarBuffer
 * @see TarHeader
 * @see TarEntry
 */


public class
TarArchive extends Object
	{
	protected boolean			verbose;
	protected boolean			debug;
	protected boolean			keepOldFiles;
	protected boolean			asciiTranslate;

	protected int				userId;
	protected String			userName;
	protected int				groupId;
	protected String			groupName;

	protected String			rootPath;
	protected String			tempPath;
	protected String			pathPrefix;

	protected int				recordSize;
	protected byte[]			recordBuf;

	protected TarInputStream	tarIn;
	protected TarOutputStream	tarOut;

	protected TarTransFileTyper		transTyper;
	protected TarProgressDisplay	progressDisplay;


	/**
	 * The InputStream based constructors create a TarArchive for the
	 * purposes of e'x'tracting or lis't'ing a tar archive. Thus, use
	 * these constructors when you wish to extract files from or list
	 * the contents of an existing tar archive.
	 */

	public
	TarArchive( InputStream inStream )
		{
		this( inStream, TarBuffer.DEFAULT_BLKSIZE );
		}

	public
	TarArchive( InputStream inStream, int blockSize )
		{
		this( inStream, blockSize, TarBuffer.DEFAULT_RCDSIZE );
		}

	public
	TarArchive( InputStream inStream, int blockSize, int recordSize )
		{
		this.tarIn = new TarInputStream( inStream, blockSize, recordSize );
		this.initialize( recordSize );
		}

	/**
	 * The OutputStream based constructors create a TarArchive for the
	 * purposes of 'c'reating a tar archive. Thus, use these constructors
	 * when you wish to create a new tar archive and write files into it.
	 */

	public
	TarArchive( OutputStream outStream )
		{
		this( outStream, TarBuffer.DEFAULT_BLKSIZE );
		}

	public
	TarArchive( OutputStream outStream, int blockSize )
		{
		this( outStream, blockSize, TarBuffer.DEFAULT_RCDSIZE );
		}

	public
	TarArchive( OutputStream outStream, int blockSize, int recordSize )
		{
		this.tarOut = new TarOutputStream( outStream, blockSize, recordSize );
		this.initialize( recordSize );
		}

	/**
	 * Common constructor initialization code.
	 */

	private void
	initialize( int recordSize )
		{
		this.rootPath = null;
		this.pathPrefix = null;
		this.tempPath = System.getProperty( "user.dir" );

		this.userId = 0;
		this.userName = "";
		this.groupId = 0;
		this.groupName = "";

		this.debug = false;
		this.verbose = false;
		this.keepOldFiles = false;
		this.progressDisplay = null;

		this.recordBuf =
			new byte[ this.getRecordSize() ];
		}

	/**
	 * Set the debugging flag.
	 *
	 * @param debugF The new debug setting.
	 */

	public void
	setDebug( boolean debugF )
		{
		this.debug = debugF;
		if ( this.tarIn != null )
			this.tarIn.setDebug( debugF );
		else if ( this.tarOut != null )
			this.tarOut.setDebug( debugF );
		}

	/**
	 * Returns the verbosity setting.
	 *
	 * @return The current verbosity setting.
	 */

	public boolean
	isVerbose()
		{
		return this.verbose;
		}

	/**
	 * Set the verbosity flag.
	 *
	 * @param verbose The new verbosity setting.
	 */

	public void
	setVerbose( boolean verbose )
		{
		this.verbose = verbose;
		}

	/**
	 * Set the current progress display interface. This allows the
	 * programmer to use a custom class to display the progress of
	 * the archive's processing.
	 *
	 * @param display The new progress display interface.
	 * @see TarProgressDisplay
	 */

	public void
	setTarProgressDisplay( TarProgressDisplay display )
		{
		this.progressDisplay = display;
		}

	/**
	 * Set the flag that determines whether existing files are
	 * kept, or overwritten during extraction.
	 *
	 * @param keepOldFiles If true, do not overwrite existing files.
	 */

	public void
	setKeepOldFiles( boolean keepOldFiles )
		{
		this.keepOldFiles = keepOldFiles;
		}
	
	/**
	 * Set the ascii file translation flag. If ascii file translatio
	 * is true, then the MIME file type will be consulted to determine
	 * if the file is of type 'text/*'. If the MIME type is not found,
	 * then the TransFileTyper is consulted if it is not null. If
	 * either of these two checks indicates the file is an ascii text
	 * file, it will be translated. The translation converts the local
	 * operating system's concept of line ends into the UNIX line end,
	 * '\n', which is the defacto standard for a TAR archive. This makes
	 * text files compatible with UNIX, and since most tar implementations
	 * for other platforms, compatible with most other platforms.
	 *
	 * @param asciiTranslate If true, translate ascii text files.
	 */

	public void
	setAsciiTranslation( boolean asciiTranslate )
		{
		this.asciiTranslate = asciiTranslate;
		}

	/**
	 * Set the object that will determine if a file is of type
	 * ascii text for translation purposes.
	 *
	 * @param transTyper The new TransFileTyper object.
	 */

	public void
	setTransFileTyper( TarTransFileTyper transTyper )
		{
		this.transTyper = transTyper;
		}

	/**
	 * Set user and group information that will be used to fill in the
	 * tar archive's entry headers. Since Java currently provides no means
	 * of determining a user name, user id, group name, or group id for
	 * a given File, TarArchive allows the programmer to specify values
	 * to be used in their place.
	 *
	 * @param userId The user Id to use in the headers.
	 * @param userName The user name to use in the headers.
	 * @param groupId The group id to use in the headers.
	 * @param groupName The group name to use in the headers.
	 */

	public void
	setUserInfo(
			int userId, String userName,
			int groupId, String groupName )
		{
		this.userId = userId;
		this.userName = userName;
		this.groupId = groupId;
		this.groupName = groupName;
		}

	/**
	 * Get the user id being used for archive entry headers.
	 *
	 * @return The current user id.
	 */

	public int
	getUserId()
		{
		return this.userId;
		}

	/**
	 * Get the user name being used for archive entry headers.
	 *
	 * @return The current user name.
	 */

	public String
	getUserName()
		{
		return this.userName;
		}

	/**
	 * Get the group id being used for archive entry headers.
	 *
	 * @return The current group id.
	 */

	public int
	getGroupId()
		{
		return this.groupId;
		}

	/**
	 * Get the group name being used for archive entry headers.
	 *
	 * @return The current group name.
	 */

	public String
	getGroupName()
		{
		return this.groupName;
		}

	/**
	 * Get the current temporary directory path. Because Java's
	 * File did not support temporary files until version 1.2,
	 * TarArchive manages its own concept of the temporary
	 * directory. The temporary directory defaults to the
	 * 'user.dir' System property.
	 *
	 * @return The current temporary directory path.
	 */

	public String
	getTempDirectory()
		{
		return this.tempPath;
		}

	/**
	 * Set the current temporary directory path.
	 *
	 * @param path The new temporary directory path.
	 */

	public void
	setTempDirectory( String path )
		{
		this.tempPath = path;
		}

	/**
	 * Get the archive's record size. Because of its history, tar
	 * supports the concept of buffered IO consisting of BLOCKS of
	 * RECORDS. This allowed tar to match the IO characteristics of
	 * the physical device being used. Of course, in the Java world,
	 * this makes no sense, WITH ONE EXCEPTION - archives are expected
	 * to be propertly "blocked". Thus, all of the horrible TarBuffer
	 * support boils down to simply getting the "boundaries" correct.
	 *
	 * @return The record size this archive is using.
	 */

	public int
	getRecordSize()
		{
		if ( this.tarIn != null )
			{
			return this.tarIn.getRecordSize();
			}
		else if ( this.tarOut != null )
			{
			return this.tarOut.getRecordSize();
			}
		
		return TarBuffer.DEFAULT_RCDSIZE;
		}

	/**
	 * Get a path for a temporary file for a given File. The
	 * temporary file is NOT created. The algorithm attempts
	 * to handle filename collisions so that the name is
	 * unique.
	 *
	 * @return The temporary file's path.
	 */

	private String
	getTempFilePath( File eFile )
		{
		String pathStr =
			this.tempPath + File.separator
				+ eFile.getName() + ".tmp";

		for ( int i = 1 ; i < 5 ; ++i )
			{
			File f = new File( pathStr );

			if ( ! f.exists() )
				break;

			pathStr =
				this.tempPath + File.separator
					+ eFile.getName() + "-" + i + ".tmp";
			}

		return pathStr;
		}

	/**
	 * Close the archive. This simply calls the underlying
	 * tar stream's close() method.
	 */

	public void
	closeArchive()
		throws IOException
		{
		if ( this.tarIn != null )
			{
			this.tarIn.close();
			}
		else if ( this.tarOut != null )
			{
			this.tarOut.close();
			}
		}

	/**
	 * Perform the "list" command and list the contents of the archive.
	 * NOTE That this method uses the progress display to actually list
	 * the conents. If the progress display is not set, nothing will be
	 * listed!
	 */

	public void
	listContents()
		throws IOException, InvalidHeaderException
		{
		for ( ; ; )
			{
			TarEntry entry = this.tarIn.getNextEntry();


			if ( entry == null )
				{
				if ( this.debug )
					{
					System.err.println( "READ EOF RECORD" );
					}
				break;
				}

			if ( this.progressDisplay != null )
				this.progressDisplay.showTarProgressMessage
					( entry.getName() );
			}
		}

	/**
	 * Perform the "extract" command and extract the contents of the archive.
	 *
	 * @param destDir The destination directory into which to extract.
	 */

	public void
	extractContents( File destDir )
		throws IOException, InvalidHeaderException
		{
		for ( ; ; )
			{
			TarEntry entry = this.tarIn.getNextEntry();

			if ( entry == null )
				{
				if ( this.debug )
					{
					System.err.println( "READ EOF RECORD" );
					}
				break;
				}

			this.extractEntry( destDir, entry );
			}
		}

	/**
	 * Extract an entry from the archive. This method assumes that the
	 * tarIn stream has been properly set with a call to getNextEntry().
	 *
	 * @param destDir The destination directory into which to extract.
	 * @param entry The TarEntry returned by tarIn.getNextEntry().
	 */

	private void
	extractEntry( File destDir, TarEntry entry )
		throws IOException
		{
		if ( this.verbose )
			{
			if ( this.progressDisplay != null )
				this.progressDisplay.showTarProgressMessage
					( entry.getName() );
			}

		String name = entry.getName();
		name = name.replace( '/', File.separatorChar );

		File destFile = new File( destDir, name );

		if ( entry.isDirectory() )
			{
			if ( ! destFile.exists() )
				{
				if ( ! destFile.mkdirs() )
					{
					throw new IOException
						( "error making directory path '"
							+ destFile.getPath() + "'" );
					}
				}
			}
		else
			{
			File subDir = new File( destFile.getParent() );

			if ( ! subDir.exists() )
				{
				if ( ! subDir.mkdirs() )
					{
					throw new IOException
						( "error making directory path '"
							+ subDir.getPath() + "'" );
					}
				}

			if ( this.keepOldFiles && destFile.exists() )
				{
				if ( this.verbose )
					{
					if ( this.progressDisplay != null )
						this.progressDisplay.showTarProgressMessage
							( "not overwriting " + entry.getName() );
					}
				}
			else
				{
				boolean asciiTrans = false;

				FileOutputStream out =
					new FileOutputStream( destFile );

				if ( this.asciiTranslate )
					{
					MimeType mime = null;
					String contentType = null;

					try {
						contentType =
							FileTypeMap.getDefaultFileTypeMap().
								getContentType( destFile );

						mime = new MimeType( contentType );

						if ( mime.getPrimaryType().
								equalsIgnoreCase( "text" ) )
							{
							asciiTrans = true;
							}
						else if ( this.transTyper != null )
							{
							if ( this.transTyper.isAsciiFile( entry.getName() ) )
								{
								asciiTrans = true;
								}
							}
						}
					catch ( MimeTypeParseException ex )
						{ }

					if ( this.debug )
						{
						System.err.println
							( "EXTRACT TRANS? '" + asciiTrans
								+ "'  ContentType='" + contentType
								+ "'  PrimaryType='" + mime.getPrimaryType()
								+ "'" );
						}
					}

				PrintWriter outw = null;
				if ( asciiTrans )
					{
					outw = new PrintWriter( out );
					}

				byte[] rdbuf = new byte[32 * 1024];

				for ( ; ; )
					{
					int numRead = this.tarIn.read( rdbuf );

					if ( numRead == -1 )
						break;
					
					if ( asciiTrans )
						{
						for ( int off = 0, b = 0 ; b < numRead ; ++b )
							{
							if ( rdbuf[ b ] == 10 )
								{
								String s = new String
									( rdbuf, off, (b - off) );

								outw.println( s );

								off = b + 1;
								}
							}
						}
					else
						{
						out.write( rdbuf, 0, numRead );
						}
					}

				if ( asciiTrans )
					outw.close();
				else
					out.close();
				}
			}
		}

	/**
	 * Write an entry to the archive. This method will call the putNextEntry()
	 * and then write the contents of the entry, and finally call closeEntry()
	 * for entries that are files. For directories, it will call putNextEntry(),
	 * and then, if the recurse flag is true, process each entry that is a
	 * child of the directory.
	 *
	 * @param entry The TarEntry representing the entry to write to the archive.
	 * @param recurse If true, process the children of directory entries.
	 */

	public void
	writeEntry( TarEntry oldEntry, boolean recurse )
		throws IOException
		{
		boolean asciiTrans = false;
		boolean unixArchiveFormat = oldEntry.isUnixTarFormat();

		File tFile = null;
		File eFile = oldEntry.getFile();

		// Work on a copy of the entry so we can manipulate it.
		// Note that we must distinguish how the entry was constructed.
		//
		TarEntry entry = (TarEntry) oldEntry.clone();

		if ( this.verbose )
			{
			if ( this.progressDisplay != null )
				this.progressDisplay.showTarProgressMessage
					( entry.getName() );
			}

		if ( this.asciiTranslate
				 && ! entry.isDirectory() )
			{
			MimeType mime = null;
			String contentType = null;

			try {
				contentType =
					FileTypeMap.getDefaultFileTypeMap().
						getContentType( eFile );

				mime = new MimeType( contentType );

				if ( mime.getPrimaryType().
						equalsIgnoreCase( "text" ) )
					{
					asciiTrans = true;
					}
				else if ( this.transTyper != null )
					{
					if ( this.transTyper.isAsciiFile( eFile ) )
						{
						asciiTrans = true;
						}
					}
				}
			catch ( MimeTypeParseException ex )
				{
				// IGNORE THIS ERROR...
				}

			if ( this.debug )
				{
				System.err.println
					( "CREATE TRANS? '" + asciiTrans
						+ "'  ContentType='" + contentType
						+ "'  PrimaryType='" + mime.getPrimaryType()
						+ "'" );
				}

			if ( asciiTrans )
				{
				String tempFileName =
					this.getTempFilePath( eFile );

				tFile = new File( tempFileName );

				BufferedReader in =
					new BufferedReader
						( new InputStreamReader
							( new FileInputStream( eFile ) ) );

				BufferedOutputStream out =
					new BufferedOutputStream
						( new FileOutputStream( tFile ) );

				for ( ; ; )
					{
					String line = in.readLine();
					if ( line == null )
						break;

					out.write( line.getBytes() );
					out.write( (byte)'\n' );
					}

				in.close();
				out.flush();
				out.close();

				entry.setSize( tFile.length() );

				eFile = tFile;
				}
			}

		String newName = null;

		if ( this.rootPath != null )
			{
			if ( entry.getName().startsWith( this.rootPath ) )
				{
				newName =
					entry.getName().substring
						( this.rootPath.length() + 1 );
				}
			}

		if ( this.pathPrefix != null )
			{
			newName = (newName == null)
				? this.pathPrefix + "/" + entry.getName()
				: this.pathPrefix + "/" + newName;
			}

		if ( newName != null )
			{
			entry.setName( newName );
			}

		this.tarOut.putNextEntry( entry );

		if ( entry.isDirectory() )
			{
			if ( recurse )
				{
				TarEntry[] list = entry.getDirectoryEntries();

				for ( int i = 0 ; i < list.length ; ++i )
					{
					TarEntry dirEntry = list[i];

					if ( unixArchiveFormat )
						dirEntry.setUnixTarFormat();

					this.writeEntry( dirEntry, recurse );
					}
				}
			}
		else
			{
			FileInputStream in =
				new FileInputStream( eFile );

			byte[] eBuf = new byte[ 32 * 1024 ];
			for ( ; ; )
				{
				int numRead = in.read( eBuf, 0, eBuf.length );

				if ( numRead == -1 )
					break;

				this.tarOut.write( eBuf, 0, numRead );
				}

			in.close();

			if ( tFile != null )
				{
				tFile.delete();
				}
			
			this.tarOut.closeEntry();
			}
		}

	}


