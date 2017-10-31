package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class TerminalState implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	private String logType; // 日志类型
	private String probeID; // 设备ID
	private String hasID;//播放标识
	private String state;//机顶盒状态
	private String KPIUTCSec;//同步时间
	private String reserve1;
	private String reserve2;
	private String deviceID; // 终端ID
	private String deviceProvider; // 终端厂商
	private String platform; // 牌照方
	private String provinceID; // 省份
	private String fwVersion; // 框架版本
	private String cityID; //地市
	
	private String indexTime;
	private long playCountCount;//播放用户数 由hasID查
	private long playUserCount;//流用户数 由probeID查
	private long startUserCount;//开机用户数
	
	public TerminalState(){}
	
	public TerminalState(String indexTime){
		this.indexTime = indexTime;
	}
	
    public String getLogType() {
		return logType;
	}

	public String getCityID() {
	return cityID;
}

public void setCityID(String cityID) {
	this.cityID = cityID;
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

	public String getHasID() {
		return hasID;
	}

	public void setHasID(String hasID) {
		this.hasID = hasID;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getKPIUTCSec() {
		return KPIUTCSec;
	}

	public void setKPIUTCSec(String kPIUTCSec) {
		KPIUTCSec = kPIUTCSec;
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

	public String getDeviceID() {
		return deviceID;
	}

	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
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

	public String getFwVersion() {
		return fwVersion;
	}

	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}

	public long getPlayCountCount() {
		return playCountCount;
	}

	public void setPlayCountCount(long playCountCount) {
		this.playCountCount = playCountCount;
	}

	public long getPlayUserCount() {
		return playUserCount;
	}

	public void setPlayUserCount(long playUserCount) {
		this.playUserCount = playUserCount;
	}

	public long getStartUserCount() {
		return startUserCount;
	}

	public void setStartUserCount(long startUserCount) {
		this.startUserCount = startUserCount;
	}

	

	//	@Override
//	public String toString() {
//		return "TerminalInfo [logType=" + logType + ", probeID=" + probeID + ", deviceID=" + deviceID
//				+ ", deviceProvider=" + deviceProvider + ", platform=" + platform + ", provinceID=" + provinceID
//				+ ", cityID=" + cityID + ", fwVersion=" + fwVersion + ", deviceModelID=" + deviceModelID
//				+ ", deviceVersion=" + deviceVersion + ", mode=" + mode + ", userID=" + userID + ", probeIP=" + probeIP
//				+ ", mac=" + mac + ", mac2=" + mac2 + ", evVersion=" + evVersion + ", managerName=" + managerName + "]";
//	}
//	public TerminalState(String deviceProvider, String platform, String provinceID, String fwVersion,
//			 String probeID, String hasID) {
//		super();
//		this.probeID = probeID;
//		this.deviceProvider = deviceProvider;
//		this.platform = platform;
//		this.provinceID = provinceID;
//		this.fwVersion = fwVersion;
//		this.hasID = hasID;
//	}
}
