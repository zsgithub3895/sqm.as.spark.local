package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

public class EPGResponseBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/** 省份 */
	private String provinceID;

	/** 地市 */
	private String cityID;

	/** 牌照方 */
	private String platform;

	/** 终端厂商 */
	private String deviceProvider;
	/** 框架版本 */
	private String fwVersion;

	/** 页面类型 1:首页,2:栏目页,3:详情页 */
	private String pageType;

	/** EPG响应耗时，单位微秒  */
	private long epgDur;

	/** HTTP请求数 */
	private long requests;
	/** 页面加载成功总数 */
	private long pageDurSucCnt;

	/** HTTP响应时长，单位微秒 */
	private long httpRspTime;

	private String indexTime;

	private long[] avgEpgDur;

	private long[][] epgTimeDelay; // 前面页面类型1、2、3对应0、1、2，后面是对应4个时延分布
	
	private String kPIUTCSec;
	
	private Long allHttpRspTime;
	
	private Long allRequest;
	/*
	 * private long range1;
	 * 
	 * private long range2;
	 * 
	 * private long range3;
	 * 
	 * private long range4;
	 */

	public EPGResponseBean() {
	}

	public Long getAllHttpRspTime() {
		return allHttpRspTime;
	}

	public void setAllHttpRspTime(Long allHttpRspTime) {
		this.allHttpRspTime = allHttpRspTime;
	}

	public Long getAllRequest() {
		return allRequest;
	}

	public void setAllRequest(Long allRequest) {
		this.allRequest = allRequest;
	}

	public String getkPIUTCSec() {
		return kPIUTCSec;
	}

	public void setkPIUTCSec(String kPIUTCSec) {
		if(StringUtils.isNotBlank(kPIUTCSec)){
			this.kPIUTCSec = kPIUTCSec.substring(0, kPIUTCSec.length()-2);
		}else{
			this.kPIUTCSec = kPIUTCSec;
		}
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

	public String getPageType() {
		return pageType;
	}

	public void setPageType(String pageType) {
		this.pageType = pageType;
	}

	public long getEpgDur() {
		return epgDur;
	}

	public void setEpgDur(long epgDur) {
		this.epgDur = epgDur;
	}

	public long getRequests() {
		return requests;
	}

	public void setRequests(long requests) {
		this.requests = requests;
	}

	public long getHttpRspTime() {
		return httpRspTime;
	}

	public void setHttpRspTime(long httpRspTime) {
		this.httpRspTime = httpRspTime;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
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

	public long getPageDurSucCnt() {
		return pageDurSucCnt;
	}

	public void setPageDurSucCnt(long pageDurSucCnt) {
		this.pageDurSucCnt = pageDurSucCnt;
	}

	public long[] getAvgEpgDur() {
		return avgEpgDur;
	}

	public void setAvgEpgDur(long[] avgEpgDur) {
		this.avgEpgDur = avgEpgDur;
	}

	public long[][] getEpgTimeDelay() {
		return epgTimeDelay;
	}

	public void setEpgTimeDelay(long[][] epgTimeDelay) {
		this.epgTimeDelay = epgTimeDelay;
	}

}
