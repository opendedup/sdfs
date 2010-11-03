/*
** Contributed by "Bay" <bayard@generationjava.com>
**
** This code has been placed into the public domain.
*/

package com.ice.tar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.zip.GZIPOutputStream;


// we extend TarOutputStream to have the same type, 
// BUT, we don't use ANY methods. It's all about 
// typing.

/**
 * Outputs tar.gz files. Added functionality that it 
 * doesn't need to know the size of an entry. If an 
 * entry has zero size when it is put in the Tar, then 
 * it buffers it until it's closed and it knows the size.
 *
 * @author "Bay" <bayard@generationjava.com>
 */

public
class		TarGzOutputStream
extends		TarOutputStream
	{
    private TarOutputStream			tos = null;
    private GZIPOutputStream		gzip = null;
    private ByteArrayOutputStream	bos = null;
    private TarEntry				currentEntry = null;

	public
	TarGzOutputStream( OutputStream out )
		throws IOException
		{
		super( null );
		this.gzip = new GZIPOutputStream( out );
		this.tos = new TarOutputStream( this.gzip );
		this.bos = new ByteArrayOutputStream();
		}

	// proxy all methods, but buffer if unknown size

	public void
	setDebug( boolean b )
		{
		this.tos.setDebug(b);
		}

	public void
	setBufferDebug( boolean b )
		{
		this.tos.setBufferDebug(b);
		}

	public void
	finish()
		throws IOException
		{
		if ( this.currentEntry != null )
			{
			closeEntry();
			}

		this.tos.finish();
		}

	public void
	close()
		throws IOException
		{
		this.tos.close();
		this.gzip.finish();
		}

	public int
	getRecordSize()
		{
		return this.tos.getRecordSize();
		}

	public void
	putNextEntry(TarEntry entry)
		throws IOException
		{
		if ( entry.getSize() != 0 )
			{
			this.tos.putNextEntry( entry );
			}
		else
			{
			this.currentEntry = entry;
			}
		}

	public void
	closeEntry()
		throws IOException
		{
		if(this.currentEntry == null)
			{
			this.tos.closeEntry();
			}
		else
			{
			this.currentEntry.setSize( bos.size() );
			this.tos.putNextEntry( this.currentEntry );
			this.bos.writeTo( this.tos );
			this.tos.closeEntry();
			this.currentEntry = null;
			this.bos = new ByteArrayOutputStream();
			}
		}

	public void
	write( int b )
		throws IOException
		{
		if ( this.currentEntry == null )
			{
			this.tos.write( b );
			}
		else
			{
			this.bos.write( b );
			}
		}

	public void
	write( byte[] b )
		throws IOException
		{
		if ( this.currentEntry == null )
			{
			this.tos.write( b );
			}
		else
			{
			this.bos.write( b );
			}
		}

	public void
	write( byte[] b, int start, int length )
		throws IOException
		{
		if ( this.currentEntry == null )
			{
			this.tos.write( b, start, length );
			}
		else
			{
			this.bos.write( b, start, length );
			}
		}

	}
