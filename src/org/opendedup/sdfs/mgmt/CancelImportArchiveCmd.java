package org.opendedup.sdfs.mgmt;

import org.opendedup.sdfs.replication.ArchiveImporter;

public class CancelImportArchiveCmd {

	public String getResult(String eventId) throws Exception {
		ArchiveImporter.stopJob(eventId);
		return "canceled import job " + eventId;
	}

}
