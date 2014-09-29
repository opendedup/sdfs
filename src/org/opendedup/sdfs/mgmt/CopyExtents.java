package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
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
		//SDFSLogger.getLog().info(
		//		"Received " + srcfile + " to " + dstfile + " len = " + len
		//				+ " sstart=" + sstart + " dstart=" + dstart);
		/*
		 * 
		 * if (dmf.getBackingFile() == null) { dmf.setBackingFile(f.getPath());
		 * evt = SDFSEvent.snapEvent("Snapshot Intiated for " + f.getPath() +
		 * " to " + nf.getPath(), f); MetaFileStore.snapshot(f.getPath(),
		 * nf.getPath(), false, evt); } else if (dmf.getBackingFile() !=
		 * f.getPath()) { SDFSLogger.getLog().warn("Looks like opt-synth");
		 * dmf.setInterWeaveCP(true); } if (dmf.isInterWeaveCP()) {
		 */if (smf.length() < len)
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

					if (_spos != _sstart || _dstart != _dpos
							|| Main.CHUNK_LENGTH > _rem) {

						SparseDataChunk sdc = sdf.getSparseDataChunk(_spos);
						SparseDataChunk ddc = ddf.getSparseDataChunk(_dpos);
						HashLocPair p = sdc.getWL(_so);
						if (p.nlen > _rem) {
							p.nlen = (int) _rem;
						}
						p.pos = _do;
						int ep = p.pos + p.nlen;
						if (ep > Main.CHUNK_LENGTH) {
							p.nlen = Main.CHUNK_LENGTH - p.pos;
						}
						//SDFSLogger.getLog().info(
						//		"at pos=" + _dstart + " putting " + p);
						ddc.putHash(p);
						ddf.putSparseDataChunk(_dpos,ddc);
						written += p.nlen;
						/*
						 * boolean bw = false;
						 * 
						 * HashLocPair p = sdc.getHash(_so); if (p != null &&
						 * p.len <= _rem) {
						 * 
						 * 
						 * p.pos = _do; bw = ddc.putHash(p); } if (bw) { written
						 * += p.len; fr += p.len; } else { int _wl =
						 * sdc.getWL(_so); if (_wl > -1) wl = _wl;
						 * 
						 * int _depos = _do + wl; if (_depos >
						 * Main.CHUNK_LENGTH) { wl = Main.CHUNK_LENGTH - _do;
						 * SDFSLogger.getLog().info( srcfile +
						 * " changing wl from " + _wl + " to " + wl); }
						 * DeferredWrite dr = new DeferredWrite(); dr.sp =
						 * _sstart; dr.wl = wl; dr.dp = _dstart; al.add(dr); /*
						 * SDFSLogger.getLog().info("reading " + wl); ByteBuffer
						 * bf = ByteBuffer.allocate(wl); sch.read(bf, 0, wl,
						 * _sstart);
						 * 
						 * bf.position(0); dch.writeFile(bf, wl, 0, _dstart,
						 * false); br += wl; written += wl; }
						 */
					} else {
						ddf.putSparseDataChunk(written + dstart,
								sdf.getSparseDataChunk(written + sstart));
						written += Main.CHUNK_LENGTH;
					}

				}
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
