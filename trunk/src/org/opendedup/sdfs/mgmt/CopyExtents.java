package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;




import java.util.Arrays;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
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
		MetaDataDedupFile smf = MetaFileStore.getMF(f);
		if(smf.length() < len)
			len = smf.length();
		SparseDedupFile sdf = (SparseDedupFile)MetaFileStore.getMF(f).getDedupFile();
		DedupFileChannel sch = sdf.getChannel(99);
		SparseDedupFile ddf = (SparseDedupFile)MetaFileStore.getMF(nf).getDedupFile();
		DedupFileChannel dch = ddf.getChannel(99);
		try {
			long written = 0;
			
			while(written < len) {
				long _sstart = written+sstart;
				long _dstart = written+dstart;
				long _spos = this.getChuckPosition(_sstart);
				long _dpos = this.getChuckPosition(_dstart);
				
				if(_spos != _sstart || _dstart != _dpos) {
					WritableCacheBuffer sbuf = (WritableCacheBuffer)ddf.getWriteBuffer(_sstart);
					int _sspos = (int)(_spos-_sstart);
					int _slen = sbuf.getLength() - _sspos;
					if(_slen > (len-written))
						_slen = (int)(len-written);
					byte [] b = sbuf.getReadChunk(_sspos, _slen);
					WritableCacheBuffer dbuf = (WritableCacheBuffer)ddf.getWriteBuffer(_dstart);
					int _dspos = (int)(_dpos-_dstart);
					int _dlen = dbuf.getLength() - _dspos;
					if(b.length > _dlen) {
						b = Arrays.copyOf(b, _dlen);
					}
					dbuf.write(b, _dspos);
					written += b.length;
				} else {
					written += ddf.putSparseDataChunk(written+dstart, sdf.getSparseDataChunk(written+sstart));
				}
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
	
	private long getChuckPosition(long location) {
		long place = location / Main.CHUNK_LENGTH;
		place = place * Main.CHUNK_LENGTH;
		return place;
	}

}
