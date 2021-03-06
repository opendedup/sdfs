package org.opendedup.sdfs.mgmt.cli;

import java.util.concurrent.TimeUnit;

import org.opendedup.grpc.Shutdown.ShutdownRequest;
import org.opendedup.grpc.Shutdown.ShutdownResponse;
import org.opendedup.grpc.VolumeServiceGrpc;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.VolumeServiceGrpc.VolumeServiceBlockingStub;

import io.grpc.Status.Code;

public class ProcessShutdown {
	public static void runCmd() {
		VolumeServiceBlockingStub volumeStub = VolumeServiceGrpc.newBlockingStub(MgmtServerConnection.channel);
		
		try {
			ShutdownRequest req = ShutdownRequest.newBuilder().build();
			ShutdownResponse resp = volumeStub.withDeadlineAfter(5,TimeUnit.SECONDS).shutdownVolume(req);
			if(resp.getErrorCode() != errorCodes.NOERR) {
				System.out.println(resp.getError());
				System.exit(1);
			}
		}catch(io.grpc.StatusRuntimeException e) {
			if(e.getStatus().getCode() == Code.DEADLINE_EXCEEDED) {
				return;
			} else {
				System.out.println(e.getStatus());
				e.printStackTrace();
				System.exit(0);
			}

		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
