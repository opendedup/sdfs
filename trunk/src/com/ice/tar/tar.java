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
import java.net.*;
import java.util.zip.*;
import javax.activation.*;

/**
 * The tar class implements a weak reproduction of the
 * traditional UNIX tar command. It currently supports
 * creating, listing, and extracting from archives. It
 * also supports GZIP-ed archives with the '-z' flag.
 * See the usage (-? or --usage) for option details.
 *
 * <pre>
 * usage: com.ice.tar.tar has three basic modes:
 * 
 *   com.ice.tar -c [options] archive files...
 *      Create new archive containing files.
 * 
 *   com.ice.tar -t [options] archive
 *      List contents of tar archive
 * 
 *   com.ice.tar -x [options] archive
 *      Extract contents of tar archive.
 * 
 * options:
 *    -f file, use 'file' as the tar archive
 *    -v, verbose mode
 *    -z, use GZIP compression
 *    -D, debug archive and buffer operation
 *    -b blks, set blocking size to (blks * 512) bytes
 *    -o, write a V7 format archive rather than ANSI
 *    -u name, set user name to 'name'
 *    -U id, set user id to 'id'
 *    -g name, set group name to 'name'
 *    -G id, set group id to 'id'
 *    -?, print usage information
 *    --trans, translate 'text/*' files
 *    --mime file, use this mime types file and translate
 *    --usage, print usage information
 *    --version, print version information
 * 
 * The translation options will translate from local line
 * endings to UNIX line endings of '\\n' when writing tar
 * archives, and from UNIX line endings into local line endings
 * when extracting archives.
 * 
 * Written by Tim Endres
 * This software has been placed into the public domain.
 * </pre>
 *
 * @version $Revision: 1.10 $
 * @author Timothy Gerard Endres, <time@gjt.org>
 * @see TarArchive
 */

public class
tar extends Object
	implements TarProgressDisplay
	{
	/**
	 * Flag that determines if debugging information is displayed.
	 */
	private boolean		debug;
	/**
	 * Flag that determines if verbose feedback is provided.
	 */
	private boolean		verbose;
	/**
	 * Flag that determines if IO is GZIP-ed ('-z' option).
	 */
	private boolean		compressed;
	/**
	 * True if we are listing the archive. False if writing or extracting.
	 */
	private boolean		listingArchive;
	/**
	 * True if we are writing the archive. False if we are extracting it.
	 */
	private boolean		writingArchive;
	/**
	 * True if we are writing an old UNIX archive format (sets entry format).
	 */
	private boolean		unixArchiveFormat;
	/**
	 * True if we are not to overwrite existing files.
	 */
	private boolean		keepOldFiles;
	/**
	 * True if we are to convert ASCII text files from local line endings
	 * to the UNIX standard '\n'.
	 */
	private boolean		asciiTranslate;
	/**
	 * True if a MIME file has been loaded with the '--mime' option.
	 */
	private boolean		mimeFileLoaded;

	/**
	 * The archive name provided on the command line, null if stdio.
	 */
	private String		archiveName;

	/**
	 * The blocksize to use for the tar archive IO. Set by the '-b' option.
	 */
	private int			blockSize;

	/**
	 * The userId to use for files written to archives. Set by '-U' option.
	 */
	private int			userId;
	/**
	 * The userName to use for files written to archives. Set by '-u' option.
	 */
	private String		userName;
	/**
	 * The groupId to use for files written to archives. Set by '-G' option.
	 */
	private int			groupId;
	/**
	 * The groupName to use for files written to archives. Set by '-g' option.
	 */
	private String		groupName;


	/**
	 * The main entry point of the tar class.
	 */
	public static void
	main( String argv[] )
		{
		tar app = new tar();

		app.instanceMain( argv );
		}

	/**
	 * Establishes the default userName with the 'user.name' property.
	 */
	public
	tar()
		{
		this.debug = false;
		this.verbose = false;
		this.compressed = false;
		this.archiveName = null;
		this.listingArchive = false;
		this.writingArchive = true;
		this.unixArchiveFormat = false;
		this.keepOldFiles = false;
		this.asciiTranslate = false;

		this.blockSize = TarBuffer.DEFAULT_BLKSIZE;

		String sysUserName =
			System.getProperty( "user.name" );

		this.userId = 0;
		this.userName =
			( (sysUserName == null) ? "" : sysUserName );

		this.groupId = 0;
		this.groupName = "";
		}

	/**
	 * This is the "real" main. The class main() instantiates a tar object
	 * for the application and then calls this method. Process the arguments
	 * and perform the requested operation.
	 */
	public void
	instanceMain( String argv[] )
		{
		TarArchive archive = null;

		int argIdx = this.processArguments( argv );

		if ( writingArchive )				// WRITING
			{
			OutputStream outStream = System.out;

			if ( this.archiveName != null
					&& ! this.archiveName.equals( "-" ) )
				{
				try {
					outStream = new FileOutputStream( this.archiveName );
					}
				catch ( IOException ex )
					{
					outStream = null;
					ex.printStackTrace( System.err );
					}
				}

			if ( outStream != null )
				{
				if ( this.compressed )
					{
					try {
						outStream = new GZIPOutputStream( outStream );
						}
					catch ( IOException ex )
						{
						outStream = null;
						ex.printStackTrace( System.err );
						}
					}
				
				archive = new TarArchive( outStream, this.blockSize );
				}
			}
		else								// EXTRACING OR LISTING
			{
			InputStream inStream = System.in;

			if ( this.archiveName != null
					&& ! this.archiveName.equals( "-" ) )
				{
				try {
					inStream = new FileInputStream( this.archiveName );
					}
				catch ( IOException ex )
					{
					inStream = null;
					ex.printStackTrace( System.err );
					}
				}

			if ( inStream != null )
				{
				if ( this.compressed )
					{
					try {
						inStream = new GZIPInputStream( inStream );
						}
					catch ( IOException ex )
						{
						inStream = null;
						ex.printStackTrace( System.err );
						}
					}

				archive = new TarArchive( inStream, this.blockSize );
				}
			}

		if ( archive != null )						// SET ARCHIVE OPTIONS
			{
			archive.setDebug( this.debug );
			archive.setVerbose( this.verbose );
			archive.setTarProgressDisplay( this );
			archive.setKeepOldFiles( this.keepOldFiles );
			archive.setAsciiTranslation( this.asciiTranslate );

			archive.setUserInfo(
					this.userId, this.userName,
					this.groupId, this.groupName );
			}

		if ( archive == null )
			{
			System.err.println( "no processing due to errors" );
			}
		else if ( this.writingArchive )				// WRITING
			{
			for ( ; argIdx < argv.length ; ++argIdx )
				{
				try {
					File f = new File( argv[ argIdx ] );
				
					TarEntry entry = new TarEntry( f );

					if ( this.unixArchiveFormat )
						entry.setUnixTarFormat();
					else
						entry.setUSTarFormat();

					archive.writeEntry( entry, true );
					}
				catch ( IOException ex )
					{
					ex.printStackTrace( System.err );
					}
				}
			}
		else if ( this.listingArchive )				// LISTING
			{
			try {
				archive.listContents();
				}
			catch ( InvalidHeaderException ex )
				{
				ex.printStackTrace( System.err );
				}
			catch ( IOException ex )
				{
				ex.printStackTrace( System.err );
				}
			}
		else										// EXTRACTING
			{
			String userDir =
				System.getProperty( "user.dir", null );

			File destDir = new File( userDir );
			if ( ! destDir.exists() )
				{
				if ( ! destDir.mkdirs() )
					{
					destDir = null;
					Throwable ex = new Throwable
						( "ERROR, mkdirs() on '" + destDir.getPath()
							+ "' returned false." );
					ex.printStackTrace( System.err );
					}
				}

			if ( destDir != null )
				{
				try {
					archive.extractContents( destDir );
					}
				catch ( InvalidHeaderException ex )
					{
					ex.printStackTrace( System.err );
					}
				catch ( IOException ex )
					{
					ex.printStackTrace( System.err );
					}
				}
			}

		if ( archive != null )						// CLOSE ARCHIVE
			{
			try {
				archive.closeArchive();
				}
			catch ( IOException ex )
				{
				ex.printStackTrace( System.err );
				}
			}
		}

	/**
	 * Process arguments, handling options, and return the index of the
	 * first non-option argument.
	 *
	 * @return The index of the first non-option argument.
	 */

	private int
	processArguments( String args[] )
		{
		int idx = 0;
		boolean gotOP = false;

		for ( ; idx < args.length ; ++idx )
			{
			String arg = args[ idx ];

			if ( ! arg.startsWith( "-" ) )
				break;

			if ( arg.startsWith( "--" ) )
				{
				if ( arg.equals( "--usage" ) )
					{
					this.usage();
					System.exit(1);
					}
				else if ( arg.equals( "--version" ) )
					{
					this.version();
					System.exit(1);
					}
				else if ( arg.equals( "--trans" ) )
					{
					this.asciiTranslate = true;

					String jafClassName =
						"javax.activation.FileTypeMap";

					try {
						Class jafClass =
							Class.forName( jafClassName );

						if ( ! this.mimeFileLoaded )
							{
							URL mimeURL =
								tar.class.getResource
									( "/com/ice/tar/asciimime.txt" );

							URLConnection mimeConn =
								mimeURL.openConnection();

							InputStream in = mimeConn.getInputStream();

							FileTypeMap.setDefaultFileTypeMap
								( new MimetypesFileTypeMap( in ) );
							}
						}
					catch ( ClassNotFoundException ex )
						{
						System.err.println
						( "Could not load the class named '"
							+ jafClassName + "'.\n"
							+ "The Java Activation package must "
							+ "be installed to use ascii translation." );
						System.exit(1);
						}
					catch ( IOException ex )
						{
						ex.printStackTrace( System.err );
						System.exit(1);
						}
					}
				else if ( arg.equals( "--mime" ) )
					{
					this.mimeFileLoaded = true;

					String jafClassName =
						"javax.activation.FileTypeMap";

					File mimeFile = new File( args[ ++idx ] );

					try {
						Class jafClass =
							Class.forName( jafClassName );

						FileTypeMap.setDefaultFileTypeMap(
							new MimetypesFileTypeMap(
								new FileInputStream( mimeFile ) ) );
						}
					catch ( ClassNotFoundException ex )
						{
						System.err.println
						( "Could not load the class named '"
							+ jafClassName + "'.\n"
							+ "The Java Activation package must "
							+ "be installed to use ascii translation." );
						System.exit(1);
						}
					catch ( FileNotFoundException ex )
						{
						System.err.println
						( "Could not open the mimetypes file '"
							+ mimeFile.getPath() + "', " + ex.getMessage() );
						}
					}
				else
					{
					System.err.println
						( "unknown option: " + arg );
					this.usage();
					System.exit(1);
					}
				}
			else for ( int cIdx = 1 ; cIdx < arg.length() ; ++cIdx )
				{
				char ch = arg.charAt( cIdx );

				if ( ch == '?' )
					{
					this.usage();
					System.exit(1);
					}
				else if ( ch == 'f' )
					{
					this.archiveName = args[ ++idx ];
					}
				else if ( ch == 'z' )
					{
					this.compressed = true;
					}
				else if ( ch == 'c' )
					{
					gotOP = true;
					this.writingArchive = true;
					this.listingArchive = false;
					}
				else if ( ch == 'x' )
					{
					gotOP = true;
					this.writingArchive = false;
					this.listingArchive = false;
					}
				else if ( ch == 't' )
					{
					gotOP = true;
					this.writingArchive = false;
					this.listingArchive = true;
					}
				else if ( ch == 'k' )
					{
					this.keepOldFiles = true;
					}
				else if ( ch == 'o' )
					{
					this.unixArchiveFormat = true;
					}
				else if ( ch == 'b' )
					{
					try {
						int blks = Integer.parseInt( args[ ++idx ] );
						this.blockSize =
							( blks * TarBuffer.DEFAULT_RCDSIZE );
						}
					catch ( NumberFormatException ex )
						{
						ex.printStackTrace( System.err );
						}
					}
				else if ( ch == 'u' )
					{
					this.userName = args[ ++idx ];
					}
				else if ( ch == 'U' )
					{
					String idStr = args[ ++idx ];
					try {
						this.userId = Integer.parseInt( idStr );
						}
					catch ( NumberFormatException ex )
						{
						this.userId = 0;
						ex.printStackTrace( System.err );
						}
					}
				else if ( ch == 'g' )
					{
					this.groupName = args[ ++idx ];
					}
				else if ( ch == 'G' )
					{
					String idStr = args[ ++idx ];
					try {
						this.groupId = Integer.parseInt( idStr );
						}
					catch ( NumberFormatException ex )
						{
						this.groupId = 0;
						ex.printStackTrace( System.err );
						}
					}
				else if ( ch == 'v' )
					{
					this.verbose = true;
					}
				else if ( ch == 'D' )
					{
					this.debug = true;
					}
				else
					{
					System.err.println
						( "unknown option: " + ch );
					this.usage();
					System.exit(1);
					}
				}
			}

		if ( ! gotOP )
			{
			System.err.println
				( "you must specify an operation option (c, x, or t)" );
			this.usage();
			System.exit(1);
			}

		return idx;
		}

	 // I N T E R F A C E   TarProgressDisplay

	 /**
	 * Display progress information by printing it to System.out.
	 */

	public void
	showTarProgressMessage( String msg )
		{
		System.out.println( msg );
		}

	/**
	 * Print version information.
	 */

	private void
	version()
		{
		System.err.println
			( "Release 2.4 - $Revision: 1.10 $ $Name:  $" );
		}

	/**
	 * Print usage information.
	 */

	private void
	usage()
		{
		System.err.println( "usage: com.ice.tar.tar has three basic modes:" );
		System.err.println( "  com.ice.tar -c [options] archive files..." );
		System.err.println( "    Create new archive containing files." );
		System.err.println( "  com.ice.tar -t [options] archive" );
		System.err.println( "    List contents of tar archive" );
		System.err.println( "  com.ice.tar -x [options] archive" );
		System.err.println( "    Extract contents of tar archive." );
		System.err.println( "" );
		System.err.println( "options:" );
		System.err.println( "   -f file, use 'file' as the tar archive" );
		System.err.println( "   -v, verbose mode" );
		System.err.println( "   -z, use GZIP compression" );
		System.err.println( "   -D, debug archive and buffer operation" );
		System.err.println( "   -b blks, set blocking size to (blks * 512) bytes" );
		System.err.println( "   -o, write a V7 format archive rather than ANSI" );
		System.err.println( "   -u name, set user name to 'name'" );
		System.err.println( "   -U id, set user id to 'id'" );
		System.err.println( "   -g name, set group name to 'name'" );
		System.err.println( "   -G id, set group id to 'id'" );
		System.err.println( "   -?, print usage information" );
		System.err.println( "   --trans, translate 'text/*' files" );
		System.err.println( "   --mime file, use this mime types file and translate" );
		System.err.println( "   --usage, print usage information" );
		System.err.println( "   --version, print version information" );
		System.err.println( "" );
		System.err.println( "The translation options will translate from local line" );
		System.err.println( "endings to UNIX line endings of '\\n' when writing tar" );
		System.err.println( "archives, and from UNIX line endings into local line endings" );
		System.err.println( "when extracting archives." );
		System.err.println( "" );
		System.err.println( "Written by Tim Endres" );
		System.err.println( "" );
		System.err.println( "This software has been placed into the public domain." );
		System.err.println( "" );

		this.version();

		System.exit( 1 );
		}

	}


