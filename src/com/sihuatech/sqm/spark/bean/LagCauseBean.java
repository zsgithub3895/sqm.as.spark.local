package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class LagCauseBean implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String provinceID; // 省份
	private String cityID; // 地市
	private String platform; // 牌照方
	private String deviceProvider; // 终端厂商
	private String fwVersion; // 框架版本
	private String indexTime;//分析时间
	private long causeType1;// 运营商网络问题
	private long causeType2;// 家庭网络问题
	private long causeType3;// CDN平台问题
	private long causeType4;// 终端问题

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

	public long getCauseType1() {
		return causeType1;
	}

	public void setCauseType1(long causeType1) {
		this.causeType1 = causeType1;
	}

	public long getCauseType2() {
		return causeType2;
	}

	public void setCauseType2(long causeType2) {
		this.causeType2 = causeType2;
	}

	public long getCauseType3() {
		return causeType3;
	}

	public void setCauseType3(long causeType3) {
		this.causeType3 = causeType3;
	}

	public long getCauseType4() {
		return causeType4;
	}

	public void setCauseType4(long causeType4) {
		this.causeType4 = causeType4;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}
}
