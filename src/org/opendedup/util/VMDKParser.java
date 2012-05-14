package org.opendedup.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.BufferClosedException;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.FileClosedException;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.VMDKData;

public class VMDKParser {
	static long gb = 1024 * 1024 * 1024;
	static long twogb = 2 * 1024 * 1024 * 1024;

	public static MetaDataDedupFile writeFile(String path, String fileName,
			long size) throws IOException, BufferClosedException,
			HashtableFullException, FileClosedException {
		path = path + File.separator + fileName;
		File f = new File(path);
		if (!f.exists())
			f.mkdirs();
		else {
			throw new IOException("Cannot create vmdk at path " + path
					+ "because path aleady exists");
		}
		long blockL = size / 512;
		int heads = 255;
		int sectors = 63;
		if (size < gb) {
			heads = 64;
			sectors = 32;
		} else if (size < twogb) {
			heads = 128;
			sectors = 32;
		}
		long cylanders = (size - 512) / (heads * sectors * 512);
		if (cylanders < 0)
			cylanders = 1;
		StringBuffer sb = new StringBuffer(Main.CHUNK_LENGTH);
		sb.append("# Disk DescriptorFile \n");
		sb.append("version=1 \n");
		sb.append("encoding=\"UTF-8\"\n");
		sb.append("CID=" + RandomGUID.getVMDKCID() + "\n");
		sb.append("parentCID=ffffffff\n");
		sb.append("createType=\"vmfs\"\n");
		sb.append("# Extent description\n");
		sb.append("RW " + blockL + " VMFS \"" + fileName + "-flat.vmdk\" 0\n");
		sb.append("# The Disk Data Base\n");
		sb.append("ddb.virtualHWVersion = \"6\"\n");
		sb.append("ddb.uuid = \"" + RandomGUID.getVMDKGUID() + "\"\n");
		sb.append("ddb.geometry.cylinders = \"" + cylanders + "\"\n");
		sb.append("ddb.geometry.heads = \"" + heads + "\"\n");
		sb.append("ddb.geometry.sectors = \"" + sectors + "\"\n");
		sb.append("ddb.adapterType = \"buslogic\"\n");
		MetaDataDedupFile vmd = MetaFileStore.getMF(path + File.separator
				+ fileName + ".vmdk");
		DedupFileChannel ch = vmd.getDedupFile().getChannel(-1);
		ByteBuffer b = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
		byte[] strB = sb.toString().getBytes();
		b.put(strB);
		vmd.setLength(strB.length, true);
		vmd.getDedupFile().getWriteBuffer(0, true).write(b.array(), 0);
		vmd.getDedupFile().writeCache();
		vmd.sync();
		vmd.getDedupFile().writeCache();
		MetaDataDedupFile vmdk = MetaFileStore.getMF(path + File.separator
				+ fileName + "-flat.vmdk");
		vmdk.setLength(size, true);
		vmdk.getIOMonitor().setActualBytesWritten(0);
		vmdk.getIOMonitor().setBytesRead(0);
		vmdk.getIOMonitor().setDuplicateBlocks(0);
		vmdk.sync();
		ch.getDedupFile().unRegisterChannel(ch, -1);
		SDFSLogger.getLog().info(
				"Created vmdk of size " + vmdk.length() + " at " + path
						+ File.separator + fileName);
		return vmdk;

	}

	public static VMDKData parserVMDKFile(byte[] b)
			throws NumberFormatException, IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		BufferedReader br = new BufferedReader(new InputStreamReader(bis));
		String line;
		VMDKData data = new VMDKData();
		while ((line = br.readLine()) != null) {
			String[] vals = line.split("=");
			if (vals[0].trim().equalsIgnoreCase("version"))
				data.setVersion(vals[1].trim().replaceAll("\"", ""));
			if (vals[0].trim().equalsIgnoreCase("encoding"))
				data.setEncoding(vals[1].trim().replaceAll("\"", ""));
			if (vals[0].trim().equalsIgnoreCase("CID"))
				data.setCid(vals[1].trim().replaceAll("\"", ""));
			if (vals[0].trim().equalsIgnoreCase("parentCID"))
				data.setParentCID(vals[1].trim().replaceAll("\"", ""));
			if (vals[0].trim().equalsIgnoreCase("createType"))
				data.setCreateType(vals[1].trim().replaceAll("\"", ""));
			if (vals[0].trim().toUpperCase().startsWith("RW")) {
				String sVals[] = vals[0].split(" ");
				data.setAccess(sVals[0]);
				data.setBlocks(Long.parseLong(sVals[1].replaceAll("\"", "")));
				data.setVmfsType(sVals[2].replaceAll("\"", ""));
				data.setDiskFile(sVals[3].replaceAll("\"", ""));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.virtualHWVersion")) {
				data.setVirtualHWVersion(vals[1].trim().replaceAll("\"", ""));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.uuid")) {
				data.setUuid(vals[1].trim().replaceAll("\"", ""));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.geometry.cylinders")) {
				data.setCylinders(Long.parseLong(vals[1].trim().replaceAll(
						"\"", "")));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.geometry.heads")) {
				data.setHeads(Integer.parseInt(vals[1].trim().replaceAll("\"",
						"")));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.geometry.sectors")) {
				data.setSectors(Integer.parseInt(vals[1].trim().replaceAll(
						"\"", "")));
			}
			if (vals[0].trim().equalsIgnoreCase("ddb.adapterType")) {
				data.setAdapterType(vals[1].trim().replaceAll("\"", ""));
			}

		}
		if (data.getUuid() == null)
			return null;
		else
			return data;
	}

	public static VMDKData parseVMDKFile(String path) throws IOException {
		FileInputStream fileinputstream = new FileInputStream(path);

		int numberBytes = fileinputstream.available();
		byte bytearray[] = new byte[numberBytes];
		fileinputstream.read(bytearray);
		return parserVMDKFile(bytearray);
	}

	public static void main(String[] args) throws IOException {
		System.out
				.println(parseVMDKFile("/media/vmware/vmfs/Win2k8-3/Win2k8-3.vmdk"));
	}

}
