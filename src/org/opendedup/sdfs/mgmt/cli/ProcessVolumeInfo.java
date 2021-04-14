package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import org.opendedup.grpc.VolumeServiceGrpc;
import org.opendedup.grpc.VolumeServiceGrpc.VolumeServiceBlockingStub;
import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse;
import org.opendedup.util.StorageUnit;

public class ProcessVolumeInfo {

	public static void runCmd() {
		VolumeServiceBlockingStub volumeStub = VolumeServiceGrpc.newBlockingStub(MgmtServerConnection.channel);
		VolumeInfoRequest req = VolumeInfoRequest.newBuilder().build();
		VolumeInfoResponse resp = null;
		try {
			resp = volumeStub.getVolumeInfo(req);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		long capacitySz = resp.getCapactity();
		double maxPFull = resp.getMaxPercentageFull();
		long currentSz = resp.getCurrentSize();
		System.out.println("Files : " + resp.getFiles());
		System.out.println("Volume Offline : " + resp.getOffline());
		long dedupSz = resp.getDuplicateBytes();
		long compSz = resp.getDseCompSize();
		long dseSz = resp.getDseSize();
		System.out.printf("Volume Capacity : %s\n", StorageUnit.of(capacitySz).format(capacitySz));
		System.out.printf("Volume Current Logical Size : %s\n", StorageUnit.of(currentSz).format(currentSz));
		if (maxPFull < 0)
			System.out.printf("Volume Max Percentage Full : %s\n", "Unlimited");
		else
			System.out.printf("Volume Max Percentage Full : %s%%\n", maxPFull * 100);
		System.out.printf("Volume Duplicate Data Written : %s\n", StorageUnit.of(dedupSz).format(dedupSz));
		System.out.printf("Unique Blocks Stored: %s\n", StorageUnit.of(dseSz).format(dseSz));
		System.out.printf("Unique Blocks Stored after Compression : %s\n", StorageUnit.of(compSz).format(compSz));
		if (dseSz <= 0 || capacitySz <= 0) {
			System.out.printf("Volume Virtual Dedup Rate (Unique Blocks Stored/Current Size) : %d%%\n", 0);
		} else {
			try {
				double dedupRate = ((1 - ((double) dseSz / (double) currentSz)) * 100);
				DecimalFormat twoDForm = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
				twoDForm.applyPattern("#.##");
				dedupRate = Double.valueOf(twoDForm.format(dedupRate));
				System.out.printf("Volume Virtual Dedup Rate (Unique Blocks Stored/Current Size) : %s%%\n",
						Double.toString(dedupRate));
			} catch (Exception e) {
				System.out.printf("Volume Virtual Dedup Rate (Unique Blocks Stored/Current Size) : %d%%\n", 0);
			}
		}
		if (compSz <= 0 || currentSz <= 0) {
			System.out.printf("Volume Actual Storage Savings (Compressed Unique Blocks Stored/Current Size): %d%%\n",
					0);
		} else {
			double dedupRate = (1 - ((double) compSz / (double) currentSz)) * 100;
			DecimalFormat twoDForm = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
			twoDForm.applyPattern("#.##");
			dedupRate = Double.valueOf(twoDForm.format(dedupRate));
			System.out.printf("Volume Actual Storage Savings (Compressed Unique Blocks Stored/Current Size) : %s%%\n",
					Double.toString(dedupRate));
		}
		if (compSz <= 0 || dseSz <= 0) {
			System.out.printf("Compression Rate: %d%%\n", 0);
		} else {
			try {
				double compRate = (1 - ((double) compSz / (double) dseSz)) * 100;
				DecimalFormat twoDForm = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
				twoDForm.applyPattern("#.##");
				compRate = Double.valueOf(twoDForm.format(compRate));
				System.out.printf("Compression Rate: %s%%\n", Double.toString(compRate));
			} catch (Exception e) {
				System.out.printf("Compression Rate : %d%%\n", 0);
			}
		}
	}

	public static void main(String[] args) {
		runCmd();
	}

}
