package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.util.XMLUtils;
import org.simpleframework.http.Method;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.eventbus.EventBus;

import fuse.Errno;
import fuse.FuseException;
import fuse.FuseFtypeConstants;

public class Io {
	AtomicLong nextHandleNo = new AtomicLong(1000);
	ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
	public final String mountedVolume;
	public final String mountPoint;

	private static EventBus eventBus = new EventBus();

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public Io(String mountedVolume, String mountPoint) {

		SDFSLogger.getLog().info("mounting " + mountedVolume + " to " + mountPoint);

		if (!mountedVolume.endsWith("/"))
			mountedVolume = mountedVolume + "/";
		this.mountedVolume = mountedVolume;
		if (!mountPoint.endsWith("/"))
			mountPoint = mountPoint + "/";

		this.mountPoint = mountPoint;
		File f = new File(this.mountedVolume);
		if (!f.exists())
			f.mkdirs();
	}

	private File resolvePath(String path) throws FuseException {
		String pt = mountedVolume + path.trim();
		File _f = new File(pt);

		if (!_f.exists()) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("No such node");

			_f = null;
			throw new FuseException().initErrno(Errno.ENOENT);
		}
		return _f;
	}

	private DedupFileChannel getFileChannel(String path, long handleNo) throws FuseException {
		DedupFileChannel ch = this.dedupChannels.get(handleNo);
		if (ch == null) {
			File f = this.resolvePath(path);
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile(false).getChannel(-2);
				try {
					if (this.dedupChannels.containsKey(handleNo)) {
						ch.getDedupFile().unRegisterChannel(ch, -2);
						ch = this.dedupChannels.get(handleNo);
					} else {
						this.dedupChannels.put(handleNo, ch);
					}
				} catch (Exception e) {

				} finally {
					SDFSLogger.getLog().debug("number of channels is " + this.dedupChannels.size());
				}
			} catch (Exception e) {
				SDFSLogger.getLog().debug("unable to open file" + f.getPath(), e);
				throw new FuseException("unable to open file " + path).initErrno(Errno.EINVAL);
			}
		}
		return ch;
	}

	private DedupFileChannel getFileChannel(long handleNo) throws FuseException {
		DedupFileChannel ch = this.dedupChannels.get(handleNo);
		if (ch == null) {
			SDFSLogger.getLog().debug("unable to read file " + handleNo);
			throw new FuseException("error reading " + handleNo).initErrno(Errno.EBADFD);
		}
		return ch;
	}

	public void processIo(Request req, Response rsp, String path) throws IOException {
		String[] tokens = path.split("/");
		switch (req.getMethod()) {
		case Method.PUT:
			this.handlePut(req, rsp, tokens);
			break;
		case Method.GET:
			this.handleGet(req, rsp, tokens);
			break;
		case Method.DELETE:
			this.handleDelete(req, rsp, tokens);
			break;
		default:
			throw new IOException("method not implemented " + req.getMethod());
		}
	}

	// public static final byte[] writeof = "<result status=\"success\"
	// msg=\"file written\"/>".getBytes();

	void handlePut(Request req, Response rsp, String[] path) {

		long fh = Long.parseLong(path[1]);
		long start = Long.parseLong(path[2]);
		int len = Integer.parseInt(path[3]);
		try {
			ByteBuffer buf = ByteBuffer.allocate(len);
			req.getByteChannel().read(buf);
			
			if (buf.position() != len) {
				SDFSLogger.getLog().warn("length is " + len + " buffer size " + buf.position());
				
				throw new FuseException().initErrno(Errno.EIO);
			}
			buf.position(0);
			this.write(fh, buf, start);
			rsp.setCode(200);
			rsp.close();
		} catch (FuseException e) {
			SDFSLogger.getLog().error("error during write",e);
			this.printError(req, rsp, e.getErrno(), e);

		} catch (Exception e) {
			SDFSLogger.getLog().error("error during write",e);
			this.printError(req, rsp, -1, e);
		}

	}

	private void printError(Request req, Response rsp, int errid, Exception err) {
		Document doc = null;
		Element result = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
		} catch (Exception e3) {
			rsp.setCode(501);
			PrintStream body = null;
			try {
				body = rsp.getPrintStream();
			} catch (IOException e2) {
				SDFSLogger.getLog().error("unable to get body", e2);
			}
			body.println(e3.getMessage());
			SDFSLogger.getLog().error("invalid path " + req.getPath());
			body.close();
		}
		DOMImplementation impl = builder.getDOMImplementation();
		// Document.
		doc = impl.createDocument(null, "result", null);
		// Root element.
		result = doc.getDocumentElement();
		SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), err);
		result.setAttribute("status", "failed");
		result.setAttribute("msg", err.toString());
		result.setAttribute("error", Integer.toString(-1));
		String rsString = null;
		try {
			rsString = XMLUtils.toXMLString(doc);
		} catch (TransformerException e2) {
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e2);
			try {
				rsp.close();
			} catch (IOException e) {
				SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
			}
		}
		// SDFSLogger.getLog().debug(rsString);
		rsp.setCode(501);
		rsp.setContentType("text/xml");
		byte[] rb = rsString.getBytes();
		rsp.setContentLength(rb.length);
		try {
			rsp.getOutputStream().write(rb);
			rsp.getOutputStream().flush();
			rsp.close();
		} catch (IOException e) {
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
		}
	}

	private int getFtype(String path) throws FuseException {
		// SDFSLogger.getLog().info("Path=" + path);
		String pt = mountedVolume + path;
		File _f = new File(pt);

		if (!Files.exists(Paths.get(_f.getPath()), LinkOption.NOFOLLOW_LINKS)) {
			throw new FuseException().initErrno(Errno.ENOENT);
		}
		Path p = Paths.get(_f.getPath());
		try {
			boolean isSymbolicLink = Files.isSymbolicLink(p);
			if (isSymbolicLink)
				return FuseFtypeConstants.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtypeConstants.TYPE_DIR;
			else if (_f.isFile()) {
				return FuseFtypeConstants.TYPE_FILE;
			}
		} catch (Exception e) {
			_f = null;
			p = null;
			SDFSLogger.getLog().warn(path + " does not exist", e);
			throw new FuseException("No such node").initErrno(Errno.ENOENT);
		}
		SDFSLogger.getLog().error("could not determine type for " + path);
		throw new FuseException().initErrno(Errno.ENOENT);
	}

	public void unlink(String path) throws FuseException {
		// SDFSLogger.getLog().info("22222 " + path);
		// Thread.currentThread().setName("19
		// "+Long.toString(System.currentTimeMillis()));
		try {
			path = URLDecoder.decode(path, "UTF-8");
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("removing " + path);
			if (!Main.safeClose) {
				try {
					this.getFileChannel(path, -1).getDedupFile().forceClose();
				} catch (IOException e) {
					SDFSLogger.getLog().error("unable to close file " + path, e);
				}
			}
			if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {
				Path p = new File(mountedVolume + path).toPath();
				// SDFSLogger.getLog().info("deleting symlink " + f.getPath());
				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(path));
					eventBus.post(new MFileDeleted(mf));
					Files.delete(p);
					return;
				} catch (IOException e) {
					SDFSLogger.getLog().warn("unable to delete symlink " + p);
					throw new FuseException().initErrno(Errno.ENOSYS);
				}
			} else {

				File f = this.resolvePath(path);
				try {
					MetaFileStore.getMF(f).clearRetentionLock();
					if (MetaFileStore.removeMetaFile(f.getPath(), true, true)) {
						// SDFSLogger.getLog().info("deleted file " +
						// f.getPath());
						return;
					} else {
						SDFSLogger.getLog().warn("unable to delete file " + f.getPath());
						throw new FuseException().initErrno(Errno.EACCES);
					}
				} catch (FuseException e) {
					throw e;
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to file file " + path, e);
					throw new FuseException().initErrno(Errno.EACCES);
				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
	}

	public void write(long fh, ByteBuffer buf, long offset) throws FuseException {
		// SDFSLogger.getLog().info("writing " + path);
		// SDFSLogger.getLog().debug("writing " + buf.capacity() + " to " +
		// path);
		// Thread.currentThread().setName("21
		// "+Long.toString(System.currentTimeMillis()));
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			/*
			 * log.info("writing data to  " +path + " at " + offset +
			 * " and length of " + buf.capacity());
			 */
			// byte[] b = new byte[buf.capacity()];
			// buf.position(0);
			// buf.get(b);
			// buf.position(0);
			// SDFSLogger.getLog().info("writing " + path + " len" +
			// buf.capacity() + "==" + new String(b) + "==/n/n");

			DedupFileChannel ch = this.getFileChannel(fh);
			try {
				ch.writeFile(buf, buf.capacity(), 0, offset, true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to write to file" + fh, e);
				throw new FuseException().initErrno(Errno.EACCES);
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			throw new FuseException().initErrno(Errno.EACCES);
		}
	}

	void handleGet(Request req, Response rsp, String[] path) throws IOException {
		Document doc = null;
		Element result = null;
		boolean closed = false;
		/*
		 * for (String s : path) { SDFSLogger.getLog().info("getting " + s); }
		 */
		try {
			String tp = path[0].toLowerCase();
			if (tp.equals("read")) {
				long fh = Long.parseLong(path[1]);
				long start = Long.parseLong(path[2]);
				int len = Integer.parseInt(path[3]);
				ByteBuffer bf = ByteBuffer.wrap(new byte[len]);
				try {

					int read = this.read(fh, bf, start);
					//SDFSLogger.getLog().info("Bytes read is " + read);
					byte[] k = null;
					if (read == len)
						k = bf.array();
					else {
						k = new byte[read];
						bf.position(0);
						bf.get(k);
					}
					long time = System.currentTimeMillis();
					rsp.setContentType("application/octet-stream");
					rsp.addValue("Server", "SDFS Management Server");
					rsp.setDate("Date", time);
					rsp.setDate("Last-Modified", time);
					rsp.setContentLength(k.length);
					OutputStream out = rsp.getOutputStream();
					IOUtils.write(k, out);
					out.flush();
					out.close();
					closed = true;
				} catch (FuseException e) {
					this.printError(req, rsp, e.getErrno(), e);

				} catch (Exception e) {
					this.printError(req, rsp, -1, e);
				}
				return;
			}
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			DOMImplementation impl = builder.getDOMImplementation();
			// Document.
			doc = impl.createDocument(null, "result", null);
			// Root element.
			result = doc.getDocumentElement();
			result.setAttribute("status", "failed");
			result.setAttribute("msg", "could not authenticate user");
			switch (tp) {
			case "createfile":
				this.mknod(path[1]);
				result.setAttribute("status", "success");
				result.setAttribute("msg", "file create");
				break;
			case "openfile":
				long hndl = this.open(path[1]);
				result.setAttribute("status", "success");
				result.setAttribute("msg", "file opended");
				result.setAttribute("handle", Long.toString(hndl));
				break;
			case "closefile":
				this.release(Long.parseLong(path[1]));
				result.setAttribute("status", "success");
				result.setAttribute("msg", "file closed");
				break;
			}
		} catch (FuseException e) {

			rsp.setCode(500);
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
			result.setAttribute("status", "failed");
			result.setAttribute("msg", e.toString());
			result.setAttribute("error", Integer.toString(e.getErrno()));

		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
			rsp.setCode(500);
			result.setAttribute("status", "failed");
			result.setAttribute("msg", e.toString());
			result.setAttribute("error", Integer.toString(-1));
		} finally {
			if (!closed) {
				String rsString = null;
				try {
					rsString = XMLUtils.toXMLString(doc);
				} catch (TransformerException e) {
					SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
					rsp.close();
				}
				// SDFSLogger.getLog().debug(rsString);
				rsp.setContentType("text/xml");
				byte[] rb = rsString.getBytes();
				rsp.setContentLength(rb.length);
				rsp.getOutputStream().write(rb);
				rsp.getOutputStream().flush();
				rsp.close();
			}
		}
	}

	void handleDelete(Request req, Response rsp, String[] path) throws IOException {
		String type = path[0];
		Document doc = null;
		Element result = null;

		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			DOMImplementation impl = builder.getDOMImplementation();
			// Document.
			doc = impl.createDocument(null, "result", null);
			// Root element.
			result = doc.getDocumentElement();
			result.setAttribute("status", "failed");
			result.setAttribute("msg", "could not authenticate user");
			if (type.equalsIgnoreCase("file")) {
				this.unlink(path[1]);
				result.setAttribute("status", "success");
				result.setAttribute("msg", "file deleted " + path[1]);
			} else {
				throw new Exception("process not implemented " + type);
			}
		} catch (FuseException e) {

			rsp.setCode(500);
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
			result.setAttribute("status", "failed");
			result.setAttribute("msg", e.toString());
			result.setAttribute("error", Integer.toString(e.getErrno()));

		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
			rsp.setCode(500);
			result.setAttribute("status", "failed");
			result.setAttribute("msg", e.toString());
			result.setAttribute("error", Integer.toString(-1));
		} finally {
			String rsString = null;
			try {
				rsString = XMLUtils.toXMLString(doc);
			} catch (TransformerException e) {
				SDFSLogger.getLog().debug("unable to process get request " + req.getTarget(), e);
				rsp.close();
			}
			// SDFSLogger.getLog().debug(rsString);
			rsp.setContentType("text/xml");
			byte[] rb = rsString.getBytes();
			rsp.setContentLength(rb.length);
			rsp.getOutputStream().write(rb);
			rsp.getOutputStream().flush();
			rsp.close();
		}

	}

	public int read(long fh, ByteBuffer buf, long offset) throws FuseException {
		// SDFSLogger.getLog().info("1911 " + path);

		if (Main.volume.isOffLine())
			throw new FuseException("Volume Offline").initErrno(Errno.ENODEV);
		try {
			DedupFileChannel ch = this.getFileChannel((Long) fh);
			//SDFSLogger.getLog().info("Reading " + ch.openFile().getPath() + " pos=" +offset + " len=" + buf.capacity());
			int read = ch.read(buf, 0, buf.capacity(), offset);
			/*
			 * if (buf.position() < buf.capacity()) { byte[] k = new
			 * byte[buf.capacity() - buf.position()]; buf.put(k); //
			 * SDFSLogger.getLog().info("zzz=" //
			 * +(buf.capacity()-buf.position())); } byte[] b = new
			 * byte[buf.capacity()]; buf.position(0); buf.get(b);
			 * buf.position(0);
			 * 
			 * SDFSLogger.getLog().info("read " + path + " len" + buf.capacity()
			 * + "offset " + offset + "read" + read + "==" + new String(b) +
			 * "==\n\n");
			 */
			if (read == -1)
				read = 0;
			return read;
		} catch (DataArchivedException e) {
			throw new FuseException("File Archived").initErrno(Errno.ENODATA);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to read file " + fh, e);
			throw new FuseException("error reading " + fh).initErrno(Errno.ENODATA);
		}
	}

	public void release(long fh) throws FuseException {
		// SDFSLogger.getLog().info("199 " + path);
		try {
			DedupFileChannel ch = this.dedupChannels.remove(fh);
			if (!Main.safeClose)
				return;
			if (ch != null) {
				ch.getDedupFile().unRegisterChannel(ch, -2);
				CloseFile.close(ch.getFile(), ch.isWrittenTo());
				ch = null;
			} else {
				SDFSLogger.getLog().info("channel not found");
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error releasing" + fh, e);
			throw new FuseException("error releasing " + fh).initErrno(Errno.EBADFD);
		}
	}

	private void mknod(String path) throws FuseException {

		try {
			path = URLDecoder.decode(path, "UTF-8");
			File f = new File(this.mountedVolume + path);
			
			if (Main.volume.isOffLine())
				throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
			if (Main.volume.isFull()) {
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			}
			if (f.exists()) {
				// SDFSLogger.getLog().info("42=");
				f = null;
				throw new FuseException("file exists").initErrno(Errno.EEXIST);
			} else {
				SDFSLogger.getLog().debug("creating file " + f.getPath());
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.unmarshal();
				// SDFSLogger.getLog().info("44=");
				/*
				 * // Wait up to 5 seconds for file to be created int z = 5000;
				 * int i = 0; while (!f.exists()) { i++; if (i == z) { throw new
				 * FuseException("file creation timed out for " +
				 * path).initErrno(Errno.EBUSY); } else { try { Thread.sleep(1);
				 * } catch (InterruptedException e) { throw new FuseException(
				 * "file creation interrupted for " + path)
				 * .initErrno(Errno.EACCES); } }
				 * 
				 * }
				 */
				try {
					// mf.setMode(mode);
				} finally {
					f = null;
				}

			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error making " + path, e);
			throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
		}
	}

	public long open(String path) throws FuseException {
		// SDFSLogger.getLog().info("555=" + path);
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			path = URLDecoder.decode(path, "UTF-8");
			long z = this.nextHandleNo.incrementAndGet();
			this.getFileChannel(path, z);
			// SDFSLogger.getLog().info("555=" + path + " z=" + z);
			return z;
		} catch (FuseException e) {
			SDFSLogger.getLog().debug("error while opening file " + path, e);
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while opening file " + path, e);
			throw new FuseException("error opending " + path).initErrno(Errno.ENODATA);
		}
	}

}
