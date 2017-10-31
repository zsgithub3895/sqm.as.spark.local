package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class InfoCountBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private long count;
	private String provinceID;
	private String cityID;
	private String platform;
	private String deviceProvider;
	private String fwVersion;

	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public String getProvinceID() {
		return provinceID;
	}
	public void setProvinceID(String provinceID) {
		this.provinceID = provinceID;
	}
	public String getCityID() {
		return cityID;
	}
	public void setCityID(String cityID) {
		this.cityID = cityID;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getDeviceProvider() {
		return deviceProvider;
	}
	public void setDeviceProvider(String deviceProvider) {
		this.deviceProvider = deviceProvider;
	}
	public String getFwVersion() {
		return fwVersion;
	}
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}
}
