package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;

public class SetEnablePerfMonCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		try {
			boolean dedup = Boolean.parseBoolean(cmd);

			Main.volume.setUsePerfMon(dedup, true);
			if (dedup)
				SDFSEvent.perfMonEvent("Enabled performance montitor")
						.endEvent("Enabled performance monitor");
			else
				SDFSEvent.perfMonEvent("Disabled performance montitor")
						.endEvent("Disabled performance monitor");
			return "SUCCESS Dedup Success: set dedup to ["
					+ Main.volume.getName() + "]  [" + dedup + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"ERROR Perfmon in perfmon event: unable to set perfmon on ["
							+ Main.volume.getName() + "] " + "to [" + cmd
							+ "] because :" + e.getMessage(), e);
			SDFSEvent.perfMonEvent("Disabled performance montitor").endEvent(
					"ERROR Perfmon in perfmon event: unable to set perfmon on ["
							+ Main.volume.getName() + "] " + "to [" + cmd
							+ "] because :" + e.getMessage());
			throw new IOException(e);
		}
	}

}
