package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class StateBean extends AbstractBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String probeID;
	private String hasID;
	private int state;
	public String getProbeID() {
		return probeID;
	}
	public void setProbeID(String probeID) {
		this.probeID = probeID;
	}
	public String getHasID() {
		return hasID;
	}
	public void setHasID(String hasID) {
		this.hasID = hasID;
	}
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
}
