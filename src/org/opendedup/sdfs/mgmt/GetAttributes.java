package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class GetAttributes {


	
	
	public Element getResult(String cmd, String file, boolean shortList) throws IOException {
		if (file.equals("lastClosedFile")) {
			try {
				MetaDataDedupFile mf = CloseFile.lastClosedFile;
				Document doc = XMLUtils.getXMLDoc("files");
				Element fe = mf.toXML(doc);
				Element root = doc.getDocumentElement();
				root.appendChild(fe);
				return (Element) root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("unable to fulfill request on file " + file, e);
				throw new IOException("request to fetch attributes failed because " + e.toString());
			}
		}
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if (f.isDirectory()) {
			try {
				Path dir = FileSystems.getDefault().getPath(f.getPath());
				DirectoryStream<Path> stream = Files.newDirectoryStream(dir);

				Document doc = XMLUtils.getXMLDoc("files");
				Element root = doc.getDocumentElement();
				for (Path p : stream) {
					File _mf = p.toFile();
					MetaDataDedupFile mf = MetaFileStore.getNCMF(_mf);
					try {
					
					if (shortList) {
						Element fl = doc.createElement("file-info");
						fl.setAttribute("file-name", URLEncoder.encode(_mf.getName(), "UTF-8"));
						if (_mf.isDirectory()) {
							fl.setAttribute("type", "directory");
						} else {
							fl.setAttribute("type", "file");
						}
						root.appendChild(fl);
					} else {
						
						Element fe = mf.toXML(doc);
						root.appendChild(fe);
					}
					_mf = null;
					}catch(Exception e) {
						SDFSLogger.getLog().error("unable to load file " + _mf.getPath(),e);
					}
				}
				return (Element) root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to fulfill request on file " + file, e);
				throw new IOException("request to fetch attributes failed because " + e.toString());
			}
		} else {

			try {
				MetaDataDedupFile mf = MetaFileStore.getNCMF(new File(internalPath));
				
				Document doc = XMLUtils.getXMLDoc("files");
				Element fe = mf.toXML(doc);
				Element root = doc.getDocumentElement();
				root.appendChild(fe);
				return (Element) root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to fulfill request on file " + file, e);
				throw new IOException("request to fetch attributes failed because " + e.toString());
			}
		}
	}
	
	
	
	
	

}
