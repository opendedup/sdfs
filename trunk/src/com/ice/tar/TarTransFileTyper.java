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

import java.io.File;

/**
 * This interface indicates if a file qualifies for ASCII translation.
 * To support customization of TAR translation, this interface allows
 * the programmer to provide an object that will check files that do
 * not match the MIME types file's check for 'text/*' types. To provide
 * your own typer, subclass this class and set the TarArchive's TransFileTyper
 * via the method setTransFileTyper().
 */

public class
TarTransFileTyper
	{
	/**
	 * Return true if the file should be translated as ASCII.
	 *
	 * @param f The file to be checked to see if it need ASCII translation.
	 */

	public boolean
	isAsciiFile( File f )
		{
		return false;
		}

	/**
	 * Return true if the file should be translated as ASCII based on its name.
	 * The file DOES NOT EXIST. This is called during extract, so all we know
	 * is the file name.
	 *
	 * @param name The name of the file to be checked to see if it need ASCII
	 *        translation.
	 */

	public boolean
	isAsciiFile( String name )
		{
		return false;
		}

	}
