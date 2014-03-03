package org.rabinfingerprint.scanner;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileListing {

	public static List<File> getFileListing( File directory ) throws FileNotFoundException {
		// get files
		ArrayList<File> files = new ArrayList<File>( Arrays.<File>asList( directory.listFiles() ) );
		ArrayList<File> result = new ArrayList<File>( files );

		// recurse
		for ( File file : files ) {
			if ( !file.isDirectory() ) continue;
			result.addAll( getFileListing( file ) );
		}

		// return
		return result;
	}

}
