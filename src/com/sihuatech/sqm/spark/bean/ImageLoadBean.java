package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class ImageLoadBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/** 省份 */
	private String provinceId;
	
	/** 牌照方 */
	private String platform;
	
	/** 地市 */
	private String cityId;
	
	/** 终端厂商  */
	private String deviceProvider; 
	
	/** 框架版本  */
	private String fwVersion; 

	/** 图片加载耗时，单位微秒 */
	private long pictureDur;
	
	/** http请求总次数 */
	private long requests;
	
	/** 图片加载成功总数 */
	private long pageDurSucCnt;
	
	/** 图片加载及时数 */
	private long pageDurLE3sCnt;
	
	private double fastrate;
	
	private double succrate;
	
	private String indexTime;
	
	private String kpiUtcSec;

	public ImageLoadBean(){}
	
	public ImageLoadBean( String indexTime , double fastrate ){
		this.fastrate = fastrate;
		this.indexTime = indexTime;
	}
	
	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getProvinceId() {
		return provinceId;
	}

	public String getCityId() {
		return cityId;
	}

	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}

	public void setCityId(String cityId) {
		this.cityId = cityId;
	}

	public long getPictureDur() {
		return pictureDur;
	}

	public void setPictureDur(long pictureDur) {
		this.pictureDur = pictureDur;
	}

	public long getRequests() {
		return requests;
	}

	public void setRequests(long requests) {
		this.requests = requests;
	}

	public long getPageDurSucCnt() {
		return pageDurSucCnt;
	}

	public void setPageDurSucCnt(long pageDurSucCnt) {
		this.pageDurSucCnt = pageDurSucCnt;
	}

	public long getPageDurLE3sCnt() {
		return pageDurLE3sCnt;
	}

	public void setPageDurLE3sCnt(long pageDurLE3sCnt) {
		this.pageDurLE3sCnt = pageDurLE3sCnt;
	}

	public double getFastrate() {
		return fastrate;
	}

	public void setFastrate(double fastrate) {
		this.fastrate = fastrate;
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

	public String getKpiUtcSec() {
		return kpiUtcSec;
	}

	public void setKpiUtcSec(String kpiUtcSec) {
		this.kpiUtcSec = kpiUtcSec;
	}

	public double getSuccrate() {
		return succrate;
	}

	public void setSuccrate(double succrate) {
		this.succrate = succrate;
	}

}
