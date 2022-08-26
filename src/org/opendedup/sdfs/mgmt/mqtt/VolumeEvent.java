package org.opendedup.sdfs.mgmt.mqtt;

import java.io.Serializable;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class VolumeEvent implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	public long volumeID;
	public long volumeTS;
	public long internalTS;
	public String actionType;
	public long sequence;
	transient JsonObject obj;
	public String target;
	public String jsonStr;
	private static transient final String MFD = "mfileDelete";
	private static transient final String MFU = "mfileWritten";
	private static transient final String DBU = "sfileWritten";
	private static transient final String DBD = "sfileDeleted";

	public VolumeEvent(String jsonStr) {
		obj = JsonParser.parseString(jsonStr).getAsJsonObject();
		this.volumeID = obj.get("volumeid").getAsLong();
		this.volumeTS = obj.get("timestamp").getAsLong();
		if(!obj.has("internalts")) {

			this.internalTS = System.currentTimeMillis();
			obj.addProperty("internalts", this.internalTS);
		}
		else
			this.internalTS = obj.get("internalts").getAsLong();

		this.sequence = obj.get("sequence").getAsLong();
		this.actionType = obj.get("actionType").getAsString();
		this.target = obj.get("object").getAsString();
		if(obj.has("file"))
			this.target = obj.get("file").getAsString();
		while (this.target.startsWith("/"))
			this.target = this.target.substring(1);
		while (this.target.startsWith("\\"))
			this.target = this.target.substring(1);
		obj.addProperty("target", this.target);
		this.jsonStr = obj.toString();
	}

	public String getJsonString() {
		return this.jsonStr;
	}

	public void setActionType(String actionType) {
		this.actionType = actionType;
		obj = JsonParser.parseString(jsonStr).getAsJsonObject();
		obj.addProperty("actionType", this.target);
		this.jsonStr = obj.toString();
	}

	public String getChangeID() {
		return this.sequence + "-" + this.volumeID;
	}

	public boolean isMFDelete() {
		return actionType.equals(MFD);
	}

	public boolean isMFUpdate() {
		return actionType.equals(MFU);
	}

	public boolean isDBUpdate() {
		return actionType.equals(DBU);
	}
	public boolean isDBDelete() {
		return actionType.equals(DBD);
	}

	public long getVolumeID() {
		return this.volumeID;
	}

	public long getVolumeTS() {
		return this.volumeTS;
	}

	public long getInternalTS() {
		return this.internalTS;
	}

	public String getActionType() {
		return this.actionType;
	}

	public String getTarget() {
		return this.target;
	}


}