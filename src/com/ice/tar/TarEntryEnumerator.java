/*
** Tim feel free to integrate this code here.
**
** This code has been placed into the Public Domain.
** This code was written by David M. Gaskin in 1999.
**
*/

package com.ice.tar;

import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * Enumerate the contents of a "tar" file.
 *
 * Last updated 26th Mar 1999.
 *
 * @author  David. M. Gaskin.
 * @version Version 1.0 Mar 1999
 * @since    Version 1.0
 */

public
class		TarEntryEnumerator
implements	Enumeration
	{
	/**
	 * The instance on which the enumeration works.
	 */
	private TarInputStream	tis = null;

	/**
	 * Has EndOfFile been reached?
	 */
	private boolean			eof = false;

	/**
	 * The read ahead entry (or <B><I>null</I></B> if no read ahead exists)
	 */
	private TarEntry		readAhead = null;

	/**
	 * Construct an instance given a TarInputStream. This method is package
	 * private because it is not initially forseen that an instance of this class
	 * should be constructed from outside the package. Should it become necessary
	 * to construct an instance of this class from outside the package in which it
	 * exists then the constructor should be made <B>protected</B> and an empty
	 * subclass should be written in the other package.
	 *
	 * @param <B>tis</B> the <B>TarInputStream</B> on which this enumeration has
	 *  to be based.
	 */
	public
	TarEntryEnumerator( TarInputStream tis )
		{
		this.tis      = tis;
		eof           = false;
		}

	/**
	 * Return the next element in the enumeration. This is a required method
	 * for implementing <B>java.util.Enumeration</B>.
	 *
	 * @return the next Object in the enumeration
	 * @exception <B>NoSuchElementException</B> should an attempt be made to
	 *  read beyond EOF
	 */
	public Object
	nextElement()
		throws NoSuchElementException
		{
		if ( eof && ( readAhead == null ) )
			throw new NoSuchElementException();

		TarEntry rc = null;
		if ( readAhead != null )
			{
			rc        = readAhead;
			readAhead = null;
			}
		else
			{
			rc = getNext();
			}

		return rc;
		}

	/**
	 * Return <B>true</B> if there are more elements in the enumeration.
	 *
	 * @return <B>true</B> if there are more elements in the enumeration.
	 */
	public boolean
	hasMoreElements()
		{
		if (eof)
			return false;

		boolean rc = false;
		readAhead = getNext();
		if ( readAhead != null )
			rc = true;

		return rc;
		}

	/**
	 * Return the next element of <B>null</B> if there is no next element or
	 * if an error occured.
	 *
	 * @return the next element of <B>null</B> if there is no next element or
	 * if an error occured.
	 */
	private TarEntry
	getNext()
		{
		TarEntry rc = null;
		try {
			rc = tis.getNextEntry();
			}
		catch ( IOException ex )
			{
			// null will be returned but should not occur
			ex.printStackTrace();
			}

		if ( rc == null )
			eof = true;

		return rc;
		}
	}
