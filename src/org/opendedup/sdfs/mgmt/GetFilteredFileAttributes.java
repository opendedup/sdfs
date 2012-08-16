package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetFilteredFileAttributes {
	
	public int currentLevel = 0;
	public boolean includeFiles;
	public boolean includeFolders;
	public int level;
	public Element getResult(String cmd, String file,boolean includeFiles, boolean includeFolders, int level) throws IOException {
		this.level = level;
		this.includeFiles = includeFiles;
		this.includeFolders = includeFolders;
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if (f.isDirectory()) {
			try {
				Document doc = XMLUtils.getXMLDoc("file-info");
				Element root = doc.getDocumentElement();
				root.setAttribute("file-name", "SDFS Root");
				this.traverse(doc, root,f);
				
				return (Element)root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		} else {
			MetaDataDedupFile mf = MetaFileStore.getMF(internalPath);
			try {
				Document doc = XMLUtils.getXMLDoc("files");
				Element fe = mf.toXML(doc);
				Element root = doc.getDocumentElement();
				root.appendChild(fe);
				return (Element)root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		}
	}
	
	private Element traverse(Document doc,Element parentEl, File parentFile) throws DOMException, ParserConfigurationException, IOException {
		
		File[] files = parentFile.listFiles();
		for (int i = 0; i < files.length; i++) {
			if(files[i].isDirectory() && this.includeFolders) {
				MetaDataDedupFile mf = MetaFileStore.getMF(files[i]
				                .getPath());
				Element fe = mf.toXML(doc);
				parentEl.appendChild(fe);
				if(this.level == -1 || this.currentLevel < this.level) {
					this.traverse(doc, fe, files[i]);
				}
			}
			
			if(files[i].isFile() && this.includeFiles) {
				MetaDataDedupFile mf = MetaFileStore.getMF(files[i]
				                 				                .getPath());
				Element fe = mf.toXML(doc);
				parentEl.appendChild(fe);
			}
			
			
		}
		this.currentLevel ++;
		return parentEl;
	}

}
