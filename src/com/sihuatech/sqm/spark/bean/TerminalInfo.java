package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

import com.sihuatech.sqm.spark.common.Constants;

public class TerminalInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String logType; // 日志类型
	private String probeID; // 设备ID
	private String deviceID; // 终端ID
	private String deviceProvider; // 终端厂商
	private String platform; // 牌照方
	private String provinceID; // 省份
	private String cityID; // 地市
	private String fwVersion; // 框架版本
	private String deviceModelID; // 终端型号
	private String deviceVersion; // 终端版本
	private String mode; // 机顶盒模式
	private String userID; // 用户ID
	private String probeIP; // 探针IP
	private String mac; // 有线MAC
	private String mac2; // 无线MAC
	private String evVersion; // EVQA版本
	private String managerName; // 备注
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
	public String getDeviceModelID() {
		return deviceModelID;
	}
	public void setDeviceModelID(String deviceModelID) {
		this.deviceModelID = deviceModelID;
	}
	public String getDeviceVersion() {
		return deviceVersion;
	}
	public void setDeviceVersion(String deviceVersion) {
		this.deviceVersion = deviceVersion;
	}
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getProbeIP() {
		return probeIP;
	}
	public void setProbeIP(String probeIP) {
		this.probeIP = probeIP;
	}
	public String getMac() {
		return mac;
	}
	public void setMac(String mac) {
		this.mac = mac;
	}
	public String getMac2() {
		return mac2;
	}
	public void setMac2(String mac2) {
		this.mac2 = mac2;
	}
	public String getEvVersion() {
		return evVersion;
	}
	public void setEvVersion(String evVersion) {
		this.evVersion = evVersion;
	}
	public String getManagerName() {
		return managerName;
	}
	public void setManagerName(String managerName) {
		this.managerName = managerName;
	}
	@Override
	public String toString() {
		String d = Constants.FIELD_DELIMITER;
		return logType + d + probeID + d + deviceID + d + deviceProvider + d + platform
				+ d + provinceID + d + cityID + d + fwVersion + d + deviceModelID
				+ d + deviceVersion + d + mode + d + userID + d + probeIP + d + mac + d + mac2
				+ d + evVersion + d + managerName;
	}
}
