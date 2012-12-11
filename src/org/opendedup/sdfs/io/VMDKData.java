package org.opendedup.sdfs.io;

import org.opendedup.util.CloneMagic;

public class VMDKData implements java.io.Serializable, Cloneable {
	/**
	 * class maps to VMDK data for a specific MetaDataDedupFile. VMDK data is
	 * read in stream and stored in this object for later use such as for
	 * growing, shrinking, or mounting VMDKs
	 */
	private static final long serialVersionUID = 3865840902295774858L;
	String version;
	String encoding;
	String cid;
	String parentCID;
	String createType;
	String access;
	long blocks;
	String vmfsType;
	String diskFile;
	String virtualHWVersion;
	String uuid;
	long cylinders;
	int heads;
	String adapterType;
	int sectors;
	String toolsVersion;

	public VMDKData() {

	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	public String getParentCID() {
		return parentCID;
	}

	public void setParentCID(String parentCID) {
		this.parentCID = parentCID;
	}

	public String getCreateType() {
		return createType;
	}

	public void setCreateType(String createType) {
		this.createType = createType;
	}

	public String getAccess() {
		return access;
	}

	public void setAccess(String access) {
		this.access = access;
	}

	public long getBlocks() {
		return blocks;
	}

	public void setBlocks(long blocks) {
		this.blocks = blocks;
	}

	public String getVmfsType() {
		return vmfsType;
	}

	public void setVmfsType(String vmfsType) {
		this.vmfsType = vmfsType;
	}

	public String getDiskFile() {
		return diskFile;
	}

	public void setDiskFile(String diskFile) {
		this.diskFile = diskFile;
	}

	public String getVirtualHWVersion() {
		return virtualHWVersion;
	}

	public void setVirtualHWVersion(String virtualHWVersion) {
		this.virtualHWVersion = virtualHWVersion;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public long getCylinders() {
		return cylinders;
	}

	public void setCylinders(long cylinders) {
		this.cylinders = cylinders;
	}

	public int getHeads() {
		return heads;
	}

	public void setHeads(int heads) {
		this.heads = heads;
	}

	public String getAdapterType() {
		return adapterType;
	}

	public void setAdapterType(String adapterType) {
		this.adapterType = adapterType;
	}

	public int getSectors() {
		return sectors;
	}

	public void setSectors(int sectors) {
		this.sectors = sectors;
	}

	public String getToolsVersion() {
		return toolsVersion;
	}

	public void setToolsVersion(String toolsVersion) {
		this.toolsVersion = toolsVersion;
	}

	@Override
	public VMDKData clone() {
		return (VMDKData) CloneMagic.clone(this);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("# Disk DescriptorFile \n");
		sb.append("version=" + this.version + "\n");
		sb.append("encoding=\"" + this.encoding + "\"\n");
		sb.append("CID=" + this.cid + "\n");
		sb.append("parentCID=" + this.parentCID + "\n");
		sb.append("createType=\"" + this.createType + "\"\n");
		sb.append("# Extent description\n");
		sb.append(this.access + " " + this.blocks + " \"" + this.vmfsType
				+ "\" \"" + this.diskFile + "\" 0\n");
		sb.append("# The Disk Data Base\n");
		sb.append("ddb.virtualHWVersion = \"" + this.virtualHWVersion + "\"\n");
		sb.append("ddb.uuid = \"" + this.uuid + "\"\n");
		sb.append("ddb.geometry.cylinders = \"" + this.cylinders + "\"\n");
		sb.append("ddb.geometry.heads = \"" + this.heads + "\"\n");
		sb.append("ddb.geometry.sectors = \"" + this.sectors + "\"\n");
		sb.append("ddb.adapterType = \"" + this.adapterType + "\"\n");
		return sb.toString();
	}

}
