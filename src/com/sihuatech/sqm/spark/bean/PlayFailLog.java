package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class PlayFailLog implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String logType;//日志类型
	private String probeID;//设备ID
	private String deviceProvider;//终端厂商
	private String platform;//牌照方
	private String provinceID;//省份
	private String cityID;//地市
	private String fwVersion;//框架版本
	private String startSecond;//开始时间
	private String destIPAddress;//客户端IP
	private String URL;//URL
	private String hasType;//节目类型
	private String sourceIPAddress;//服务器IP
	private String statusCode;//错误码
	private String reserve1;
	private String reserve2;
	
	private long failCount;
	private String indexTime;
	private String kpiUtcSec;
	
	public  PlayFailLog(){}
	
	public String getLogType() {
		return logType;
	}
	public void setLogType(String logType) {
		this.logType = logType;
	}
	public String getProbeID() {
		return probeID;
	}
	public void setProbeID(String probeID) {
		this.probeID = probeID;
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
	public String getFwVersion() {
		return fwVersion;
	}
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}
	public String getStartSecond() {
		return startSecond;
	}
	public void setStartSecond(String startSecond) {
		this.startSecond = startSecond;
	}
	public String getDestIPAddress() {
		return destIPAddress;
	}
	public void setDestIPAddress(String destIPAddress) {
		this.destIPAddress = destIPAddress;
	}
	public String getURL() {
		return URL;
	}
	public void setURL(String uRL) {
		URL = uRL;
	}
	
	public String getHasType() {
		return hasType;
	}

	public void setHasType(String hasType) {
		this.hasType = hasType;
	}

	public String getSourceIPAddress() {
		return sourceIPAddress;
	}
	public void setSourceIPAddress(String sourceIPAddress) {
		this.sourceIPAddress = sourceIPAddress;
	}
	public String getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}
	public String getReserve1() {
		return reserve1;
	}
	public void setReserve1(String reserve1) {
		this.reserve1 = reserve1;
	}
	public String getReserve2() {
		return reserve2;
	}
	public void setReserve2(String reserve2) {
		this.reserve2 = reserve2;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}

	public String getKpiUtcSec() {
		return kpiUtcSec;
	}

	public void setKpiUtcSec(String kpiUtcSec) {
		this.kpiUtcSec = kpiUtcSec;
	}

	public long getFailCount() {
		return failCount;
	}

	public void setFailCount(long failCount) {
		this.failCount = failCount;
	}
	
}
