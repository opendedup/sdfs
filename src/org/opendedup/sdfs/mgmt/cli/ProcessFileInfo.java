package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Locale;

import org.opendedup.util.StorageUnit;
import org.opendedup.grpc.FileInfoRequest;
import org.opendedup.grpc.FileInfoResponse;
import org.opendedup.grpc.FileMessageResponseOrBuilder;
import org.opendedup.grpc.IOMonitorResponse;
import org.opendedup.grpc.VolumeServiceGrpc;
import org.opendedup.grpc.errorCodes;
import org.opendedup.grpc.VolumeServiceGrpc.VolumeServiceBlockingStub;

public class ProcessFileInfo {
	public static void runCmd(String file) {
		try {
			VolumeServiceBlockingStub volumeStub = VolumeServiceGrpc.newBlockingStub(MgmtServerConnection.channel);
			FileInfoRequest req = FileInfoRequest.newBuilder().setCompact(false).setFileName(file).build();
			FileMessageResponseOrBuilder resp = null;
			try {
				resp = volumeStub.getFileInfo(req);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			if (resp.getErrorCode() != errorCodes.NOERR) {
				System.err.println(resp.getError());
				System.err.println(resp.getErrorCode());
				System.exit(1);
			}
			Iterator<FileInfoResponse> iter = resp.getResponseList().iterator();
			while (iter.hasNext()) {
				FileInfoResponse fr = iter.next();
				System.out.println("#");
				System.out.printf("file name : %s\n", fr.getFilePath());
				System.out.printf("sdfs path : %s\n", fr.getSdfsPath());
				System.out.printf("logical size: %d\n",fr.getSize());
				System.out.printf("symlink : %s\n", fr.getSymlink());
				System.out.printf("symlink path: %s\n", fr.getSymlinkPath());
				System.out.printf("file type : %s\n", fr.getType());
				if (fr.getType() == FileInfoResponse.fileType.FILE) {
					IOMonitorResponse io = fr.getIoMonitor(0);
					System.out.printf("map file guid : %s\n", fr.getMapGuid());
					System.out.printf("file open : %b\n", fr.getOpen());
					System.out.printf("real bytes written : %d\n", io.getActualBytesWritten());
					System.out.printf("format real data written : %s\n",
							StorageUnit.of(io.getActualBytesWritten())
									.format(io.getActualBytesWritten()));
					System.out.printf("virtual bytes written : %d\n", io.getVirtualBytesWritten());
					System.out.printf("format virtual data written : %s\n",
							StorageUnit.of(io.getVirtualBytesWritten())
									.format(io.getVirtualBytesWritten()));
					System.out.printf("duplicate data bytes: %d\n", io.getDuplicateBlocks());
					System.out.printf("format duplicate data : %s\n",
							StorageUnit.of(io.getDuplicateBlocks())
									.format(io.getDuplicateBlocks()));
					System.out.printf("bytes read : %d\n", io.getBytesRead());
					System.out.printf("format data read: %s\n",
							StorageUnit.of(io.getBytesRead())
									.format(io.getBytesRead()));

					long realBytes = io.getActualBytesWritten();
					long dedupBytes = io.getDuplicateBlocks();
					if (dedupBytes == 0) {
						System.out.printf("dedup rate : %d%%\n", 100);
					} else if (realBytes == 0) {
						System.out.printf("dedup rate : %d%%\n", 0);
					} else {
						double dedupRate = (((double) dedupBytes / (double) (dedupBytes + realBytes)) * 100);
						if (Double.isFinite(dedupRate)) {
							DecimalFormat twoDForm = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
							twoDForm.applyPattern("#.##");
							dedupRate = Double.valueOf(twoDForm.format(dedupRate));

							System.out.printf("dedup rate : %s%%\n", Double.toString(dedupRate));
						} else {
							System.out.printf("dedup rate : %s%%\n", "100");
						}

					}
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

	}

	public static void main(String[] args) {
		runCmd("/");
	}

}
