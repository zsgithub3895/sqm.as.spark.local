package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

/**
 * Http错误码日志
 *
 */
public class HttpErrorCodeLog implements Serializable{
	private static final long serialVersionUID = 1L;
	/**
	 * 省份
	 */
	private String provinceID;
	/**
	 * 地市
	 */
	private String cityID;
	/**
	 * 终端厂商
	 */
	private String deviceProvider;
	/**
	 * 牌照方
	 */
	private String platform;
	/**
	 * 框架版本
	 */
	private String fwVersion;
	/**
	 * HTTP返回码
	 * 400或401或402或500...
	 */
	private String httpRspCode;
	/**
	 * 发生次数
	 */
	private Long count;
	
	private long  total;
	private String indexTime;

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

	public String getHttpRspCode() {
		return httpRspCode;
	}

	public void setHttpRspCode(String httpRspCode) {
		this.httpRspCode = httpRspCode;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}
}
