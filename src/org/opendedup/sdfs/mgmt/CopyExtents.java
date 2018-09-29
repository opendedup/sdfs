package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.RestoreArchive;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.FileClosedException;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class CopyExtents {
	SDFSEvent evt;

	protected static LoadingCache<String, DedupFileChannel> writeChannels = CacheBuilder.newBuilder().maximumSize(Main.maxOpenFiles*2)
			.concurrencyLevel(64).expireAfterAccess(120, TimeUnit.SECONDS)
			.removalListener(new RemovalListener<String, DedupFileChannel>() {
				public void onRemoval(RemovalNotification<String, DedupFileChannel> removal) {
					DedupFileChannel ck = removal.getValue();
					ck.getDedupFile().unRegisterChannel(ck, -1);
					// flushingBuffers.put(pos, ck);
				}
			}).build(new CacheLoader<String, DedupFileChannel>() {
				public DedupFileChannel load(String f) throws IOException, FileClosedException {
					SparseDedupFile sdf = (SparseDedupFile) MetaFileStore.getMF(f).getDedupFile(true);
					return sdf.getChannel(-1);
				}

			});

	public Element getResult(String srcfile, String dstfile, long sstart, long len, long dstart) throws Exception {
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
		SparseDedupFile sdf = (SparseDedupFile) smf.getDedupFile(true);
		SparseDedupFile ddf = (SparseDedupFile) dmf.getDedupFile(true);
		ddf.setReconstructed(true);
		long _spos = -1;
		long _dpos = -1;
		try {
			long written = 0;
			_spos = this.getChuckPosition(sstart);
			_dpos = this.getChuckPosition(dstart);
			Lock l = ddf.getWriteLock();
			l.lock();
			writeChannels.get(f.getPath());
			writeChannels.get(nf.getPath());
			try {
				while (written < len) {
					long _sstart = written + sstart;
					long _dstart = written + dstart;
					_spos = this.getChuckPosition(_sstart);
					_dpos = this.getChuckPosition(_dstart);
					long _rem = len - written;
					int _so = (int) (_sstart - _spos);
					int _do = (int) (_dstart - _dpos);
					boolean insdone = false;
					DedupFileChannel ch = null;
					int tries = 0;
					while (!insdone) {
						try {
							ch = sdf.getChannel(-1);
							SparseDataChunk sdc = sdf.getSparseDataChunk(_spos);
							/*
							if(sdc.getFingers().size() == 0) {
								int _nlen = 4 *1024;
								if(_nlen > _rem) {
									_nlen = (int)_rem;
								}
								dc.writeFile(ByteBuffer.allocate(_nlen), _nlen, 0, _dpos, true);
								//ddf.mf.getIOMonitor().addVirtualBytesWritten(p.nlen, true);
								//ddf.mf.getIOMonitor().addDulicateData(p.nlen, true);
								//ddf.mf.setLastModified(System.currentTimeMillis());
								written += _nlen;
								if(written >= len)
									insdone = true;
							} else {
							*/
							WritableCacheBuffer ddc = (WritableCacheBuffer) ddf.getWriteBuffer(_dpos);
							ddc.writeAccelBuffer();
							HashLocPair p = sdc.getWL(_so);

							if (p.nlen > _rem) {
								p.nlen = (int) _rem;
							}
							p.pos = _do;
							int ep = p.pos + p.nlen;
							if (ep > Main.CHUNK_LENGTH) {
								p.nlen = Main.CHUNK_LENGTH - p.pos;
							}
							try {
								ddc.copyExtent(p);
							} catch (DataArchivedException e) {
								if (Main.checkArchiveOnRead) {
									SDFSLogger.getLog()
											.warn("Archived data found in " + sdf.getMetaFile().getPath() + " at "
													+ _spos
													+ ". Recovering data from archive. This may take up to 4 hours");
									RestoreArchive.recoverArchives(smf);
									return getResult(srcfile, dstfile, sstart, len, dstart);
								} else
									throw e;
							}
							dmf.getIOMonitor().addVirtualBytesWritten(p.nlen, true);
							dmf.getIOMonitor().addDulicateData(p.nlen, true);
							dmf.setLastModified(System.currentTimeMillis());
							written += p.nlen;
							insdone = true;
							
							//}
						} catch (org.opendedup.sdfs.io.FileClosedException e) {
							if(tries > 100) 
								throw new IOException("tried to open file 100 ties and failed " + smf.getPath());
							insdone = false;
						} catch (Exception e) {
							throw e;
						} finally {
							if(ch != null) {
								try {
									sdf.unRegisterChannel(ch, -1);
								}catch(Exception e) {
									
								}
							}
						}
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
			SDFSLogger.getLog().error("error in copy extent src=" + srcfile + " dst=" + dstfile + " sstart=" + sstart
					+ " dstart=" + dstart + " len=" + len + " spos" + _spos + " dpos=" + _dpos, e);
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
