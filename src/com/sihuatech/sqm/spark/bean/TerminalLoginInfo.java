package com.sihuatech.sqm.spark.bean;

public class TerminalLoginInfo {

	/**
	 * 设备id
	 * */
	private String probeID;
	
	/**
	 * 牌照方
	 * */
	private String platform;
	
	/**
	 * 省份
	 * */
	private String provinceID;
	
	/**
	 * 地市
	 * */
	private String cityID;
	/**
	 *  终端厂商
	 */
	private String deviceProvider; 
	/**
	 * 框架版本
	 */
	private String fwVersion; 
	
	/**
	 * 登录服务器ip
	 * */
	private String serverIP;
	
	/**
	 * 登陆次数
	 * */
	private String pages;
	
	/**
	 * 登陆成功次数
	 * */
	private String sucPages;
	
	/**
	 * 登录操作时长(us)
	 * */
	private String sucDuration;
	
	/**
	 * 预留字段1
	 * */
	private String reserve1;
	
	/**
	 * 预留字段2
	 * */
	private String reserve2;

	public String getProbeID() {
		return probeID;
	}

	public void setProbeID(String probeID) {
		this.probeID = probeID;
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

	public String getServerIP() {
		return serverIP;
	}

	public void setServerIP(String serverIP) {
		this.serverIP = serverIP;
	}

	public String getPages() {
		return pages;
	}

	public void setPages(String pages) {
		this.pages = pages;
	}

	public String getSucPages() {
		return sucPages;
	}

	public void setSucPages(String sucPages) {
		this.sucPages = sucPages;
	}

	public String getSucDuration() {
		return sucDuration;
	}

	public void setSucDuration(String sucDuration) {
		this.sucDuration = sucDuration;
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
