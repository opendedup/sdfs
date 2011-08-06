package org.opendedup.sdfs.mgmt;

import java.io.IOException;

public interface XtendedCmd {
	public abstract String getResult(String cmd, String file)
			throws IOException;

}
