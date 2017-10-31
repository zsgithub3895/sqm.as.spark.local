package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;


public class LagPhaseRateBean implements Serializable{
	private String provinceID;
	private Long userCount;
	private Long seriousCount;
	private Long moreCount;
	private Long occasionallyCount;
	private Long normalCount;
	private String cityID;
	private String indexTime;
	private Double healthStatus;
	private Double kartunUserRate;
	private Long terminalCount;
	private String deviceProvider;
	private String platform;
	private String fwVersion;
	public Long getTerminalCount() {
		return terminalCount;
	}
	public void setTerminalCount(Long terminalCount) {
		this.terminalCount = terminalCount;
	}
	public Double getKartunUserRate() {
		return kartunUserRate;
	}
	public void setKartunUserRate(Double kartunUserRate) {
		this.kartunUserRate = kartunUserRate;
	}
	public Double getHealthStatus() {
		return healthStatus;
	}
	public void setHealthStatus(Double healthStatus) {
		this.healthStatus = healthStatus;
	}
	public String getIndexTime() {
		return indexTime;
	}
	public String getCityID() {
		return cityID;
	}
	public void setCityID(String cityID) {
		this.cityID = cityID;
	}
	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}
	public String getProvinceID() {
		return provinceID;
	}
	public void setProvinceID(String provinceID) {
		this.provinceID = provinceID;
	}
	public Long getUserCount() {
		return userCount;
	}
	public void setUserCount(Long userCount) {
		this.userCount = userCount;
	}
	public Long getSeriousCount() {
		return seriousCount;
	}
	public void setSeriousCount(Long seriousCount) {
		this.seriousCount = seriousCount;
	}
	public Long getMoreCount() {
		return moreCount;
	}
	public void setMoreCount(Long moreCount) {
		this.moreCount = moreCount;
	}
	public Long getOccasionallyCount() {
		return occasionallyCount;
	}
	public void setOccasionallyCount(Long occasionallyCount) {
		this.occasionallyCount = occasionallyCount;
	}
	public Long getNormalCount() {
		return normalCount;
	}
	public void setNormalCount(Long normalCount) {
		this.normalCount = normalCount;
	}
	public String getDeviceProvider() {
		return deviceProvider;
	}
	public void setDeviceProvider(String deviceProvider) {
		this.deviceProvider = deviceProvider;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getFwVersion() {
		return fwVersion;
	}
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}
}
