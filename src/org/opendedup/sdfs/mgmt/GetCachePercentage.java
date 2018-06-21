package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.primitives.Longs;

public class GetCachePercentage {

	MetaDataDedupFile mf = null;
	MetaDataDedupFile sdf = null;
	String sfile;
	boolean overwrite;
	
	public Element getResult(String file) throws IOException {
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if(f.isDirectory())
			throw new IOException("requeste file " + file + " is a directory");
		mf = MetaFileStore.getNCMF(new File(internalPath));
		this.sfile = file;
		return this.checkDedupFile();
	}

	private Element checkDedupFile() throws IOException {
		SDFSLogger.getLog().info("Checking Cache Percentage for " + mf.getDfGuid());
		Set<Long> blks = new HashSet<Long>();
		LongByteArrayMap ddb = LongByteArrayMap.getMap(mf.getDfGuid(), mf.getLookupFilter());
		mf.getIOMonitor().clearFileCounters(false);
		if (ddb.getVersion() < 2)
			throw new IOException("only files version 2 or later can be imported");
		try {
			long ct = 0;
			ddb.iterInit();
			for (;;) {
				LongKeyValue kv = ddb.nextKeyValue(false);
				ct++;
				if (kv == null)
					break;
				SparseDataChunk ck = kv.getValue();
				TreeMap<Integer, HashLocPair> al = ck.getFingers();
				for (HashLocPair p : al.values()) {
					blks.add(Longs.fromByteArray(p.hashloc));

				}
			}
			SDFSLogger.getLog().info("new objects of size " + blks.size() + " iter count is " + ct);
			int misses = 0;
			for (Long l : blks) {
				SDFSLogger.getLog().debug("importing " + l);
				
				if(!HashBlobArchive.isCached(l)) {
					misses++;
				}
			}
			try {
				ddb.close();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing file [" + mf.getPath() + "]", e);
			}
			Document doc = XMLUtils.getXMLDoc("cache");
			
			Element root = doc.getDocumentElement();
			root.setAttribute("average-archive-size", Integer.toString(HashBlobArchive.MAX_LEN));
			Element fe = mf.toXML(doc);
			fe.setAttribute("objects", Integer.toString(blks.size()));
			fe.setAttribute("misses", Integer.toString(misses));
			root.appendChild(fe);
			return (Element) root.cloneNode(true);
		} catch (Throwable e) {
			SDFSLogger.getLog().warn("error while checking file [" + ddb + "]", e);
			throw new IOException(e);
		} 
	}
}