package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;


import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class CopyExtents {

	public Element getResult(String srcfile,String dstfile,long sstart, long len,long dstart) throws Exception {
		Document doc = XMLUtils.getXMLDoc("copy-extent");
		Element root = doc.getDocumentElement();
		root.setAttribute("srcfile",
				srcfile);
		root.setAttribute("dstfile",
				dstfile);
		root.setAttribute("requested-source-start", Long.toString(sstart));
		root.setAttribute("requested-dest-start", Long.toString(dstart));
		root.setAttribute("lenth", Long.toString(len));
		File f = new File(Main.volume.getPath() + File.separator + srcfile);
		File nf = new File(Main.volume.getPath() + File.separator + dstfile);
		if (!f.exists())
			throw new IOException("Path not found [" + srcfile + "]");
		if (!nf.exists())
			throw new IOException("Path already exists [" + dstfile + "]");
		SparseDedupFile sdf = (SparseDedupFile)MetaFileStore.getMF(f).getDedupFile();
		DedupFileChannel sch = sdf.getChannel(99);
		SparseDedupFile ddf = (SparseDedupFile)MetaFileStore.getMF(nf).getDedupFile();
		DedupFileChannel dch = sdf.getChannel(99);
		try {
			long written = 0;
			
			while(written < len) {
				written += ddf.putSparseDataChunk(written+dstart, sdf.getSparseDataChunk(written+sstart));
			}
			root.setAttribute("written", Long.toString(written));
		} catch (Exception e) {
			throw e;
		} finally {
			sdf.unRegisterChannel(sch, 99);
			ddf.unRegisterChannel(dch, 99);
		}
		return (Element) root.cloneNode(true);
	}

}
