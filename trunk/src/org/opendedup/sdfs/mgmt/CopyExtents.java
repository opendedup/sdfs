package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class CopyExtents {
	SDFSEvent evt;

	public Element getResult(String srcfile, String dstfile, long sstart,
			long len, long dstart) throws Exception {
		Document doc = XMLUtils.getXMLDoc("copy-extent");
		Element root = doc.getDocumentElement();
		root.setAttribute("srcfile", srcfile);
		root.setAttribute("dstfile", dstfile);
		root.setAttribute("requested-source-start", Long.toString(sstart));
		root.setAttribute("requested-dest-start", Long.toString(dstart));
		root.setAttribute("lenth", Long.toString(len));
		File f = new File(Main.volume.getPath() + File.separator + srcfile);
		File nf = new File(Main.volume.getPath() + File.separator + dstfile);
		if (!f.exists())
			throw new IOException("Path not found [" + srcfile + "]");
		if (!nf.exists())
			throw new IOException("Path not found [" + dstfile + "]");
		MetaDataDedupFile smf = MetaFileStore.getMF(f);
		MetaDataDedupFile dmf = MetaFileStore.getMF(nf);
		// SDFSLogger.getLog().info(
		// "Received " + srcfile + " to " + dstfile + " len = " + len
		// + " sstart=" + sstart + " dstart=" + dstart);
		/*
		 * 
		 * if (dmf.getBackingFile() == null) { dmf.setBackingFile(f.getPath());
		 * evt = SDFSEvent.snapEvent("Snapshot Intiated for " + f.getPath() +
		 * " to " + nf.getPath(), f); MetaFileStore.snapshot(f.getPath(),
		 * nf.getPath(), false, evt); } else if (dmf.getBackingFile() !=
		 * f.getPath()) { SDFSLogger.getLog().warn("Looks like opt-synth");
		 * dmf.setInterWeaveCP(true); } if (dmf.isInterWeaveCP()) {
		 */
		if (smf.length() < len)
			len = smf.length();
		SparseDedupFile sdf = (SparseDedupFile) smf.getDedupFile();
		SparseDedupFile ddf = (SparseDedupFile) dmf.getDedupFile();
		try {
			long written = 0;
			long _spos = this.getChuckPosition(sstart);
			long _dpos = this.getChuckPosition(dstart);
			Lock l = ddf.getWriteLock();
			l.lock();
			try {
				ddf.writeCache();
				while (written < len) {
					long _sstart = written + sstart;
					long _dstart = written + dstart;
					_spos = this.getChuckPosition(_sstart);
					_dpos = this.getChuckPosition(_dstart);
					long _rem = len - written;
					int _so = (int) (_sstart - _spos);
					int _do = (int) (_dstart - _dpos);

					SparseDataChunk sdc = sdf.getSparseDataChunk(_spos);
					SparseDataChunk ddc = ddf.getSparseDataChunk(_dpos);
					if (ddc.getFingers().size() >= LongByteArrayMap.MAX_ELEMENTS_PER_AR) {
						SDFSLogger
								.getLog()
								.warn("SparseDataChunk array is larger that available capacity, rehashing");
						DedupFileChannel dch = ddf.getChannel(-34);
						DedupFileChannel sch = sdf.getChannel(-34);
						int wb = 0;
						if (_rem >= Main.CHUNK_LENGTH)
							wb = Main.CHUNK_LENGTH;
						else
							wb = (int) _rem;
						ByteBuffer buf = ByteBuffer.allocateDirect(wb);
						sch.read(buf, 0, wb, _spos);
						buf.position(0);
						dch.writeFile(buf, wb, 0, _do, true);
						ddf.writeCache();
						ddf.unRegisterChannel(dch, -34);
						sdf.unRegisterChannel(sch, -34);
						
						written += wb;
					} else {
						HashLocPair p = sdc.getWL(_so);
						p.hashloc[7]=1;
						if (p.nlen > _rem) {
							p.nlen = (int) _rem;
							p.hashloc[7]=2;
						}
						p.pos = _do;
						int ep = p.pos + p.nlen;
						if (ep > Main.CHUNK_LENGTH) {
							p.nlen = Main.CHUNK_LENGTH - p.pos;
							p.hashloc[7]=3;
						}
						/*
						 * SDFSLogger.getLog().info( "at pos=" + _dstart +
						 * " putting " + p);
						 */
						ddc.putHash(p);
						ddf.putSparseDataChunk(_dpos, ddc);
						ddf.mf.getIOMonitor().addVirtualBytesWritten(p.nlen,
								true);
						ddf.mf.getIOMonitor().addDulicateData(p.nlen, true);

						written += p.nlen;
					}

				}
				ddf.writeCache();
			} finally {
				long el = written + dstart;
				if (el > dmf.length()) {
					dmf.setLength(el, false);
				}
				l.unlock();
			}
			root.setAttribute("written", Long.toString(written));
		} catch (Exception e) {
			SDFSLogger.getLog().error("error in copy extent", e);
			throw e;
		} finally {
		}
		/*
		 * } else { root.setAttribute("written", Long.toString(len)); }
		 */
		return (Element) root.cloneNode(true);

	}

	private long getChuckPosition(long location) {
		long place = location / Main.CHUNK_LENGTH;
		place = place * Main.CHUNK_LENGTH;
		return place;
	}

}
