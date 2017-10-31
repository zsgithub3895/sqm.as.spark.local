package com.sihuatech.sqm.spark.bean;

/**
 * 父类bean，所有bean中包含的维度列于此：<br />
 * <b>运营商、省份、地市、牌照方、终端厂商、服务框架这六个维度按这个顺序从高到底的优先级排序</b>
 * @author chenyj
 *
 */
public abstract class AbstractBean {
	// 运营商暂时不需要
	protected String provinceID;
	protected String cityID;
	protected String platform;
	protected String deviceProvider;
	protected String fwVersion;
	protected String kPIUTCSec;
	
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
	public String getkPIUTCSec() {
		return kPIUTCSec;
	}
	public void setkPIUTCSec(String kPIUTCSec) {
		this.kPIUTCSec = kPIUTCSec;
	}
}
