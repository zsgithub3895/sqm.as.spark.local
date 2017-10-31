package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class LagPhaseBehaviorLog implements Serializable{
	private static final long serialVersionUID = 1L;
	private int logType; // 日志类型 取值：4
	private String hasID;//播放标识
	private String probeID; // 设备ID
	private String deviceProvider; // 终端厂商
	private String platform; // 牌照方
	private String provinceID; // 省份
	private String cityID; // 地市
	private String fwVersion; // 框架版本
	private String startSecond;	//卡顿开始时间	时间格式：YYYYMMDDHHMISS 
	private String kPIUTCSec;
	private String exportId; //	卡顿主要原因 1：服务器性能问题 2：带宽不足 3：OTT终端问题 4：HTTP响应错误等
	private String freezeTime;//	卡顿时长	单位微秒，预留，0，大于3秒，后续播放器会更准
	private String URL;//	节目的URL	
	private String hasType;//	节目类型	1:HLS直播,2:HLS点播,3:MP4点播,4:TS点播等
	private String reserve1;//预留字段1
	private String reserve2;//预留字段2	
	
	private String indexTime;
	private long playCount;//卡顿次数
	private double playCountRate;//自然缓冲率
	
	private long faultUser;//卡顿用户数
	private double playUserRate;//卡顿用户率
	
	private long faultCount;//每种卡顿原因对应的用户数（exportId）
	
	public LagPhaseBehaviorLog(){}
	
	public String getkPIUTCSec() {
		return kPIUTCSec;
	}

	public void setkPIUTCSec(String kPIUTCSec) {
		this.kPIUTCSec = kPIUTCSec;
	}

	public LagPhaseBehaviorLog(String indexTime){
		this.indexTime = indexTime;
	}
	
	public LagPhaseBehaviorLog(String indexTime, long faultCount){
		this.indexTime = indexTime;
		this.faultCount = faultCount;
	}
	
	public LagPhaseBehaviorLog(String indexTime, long playCount, double playCountRate){
		this.indexTime = indexTime;
		this.playCount = playCount;
		this.playCountRate = playCountRate;
	}
	

	

	
	public LagPhaseBehaviorLog(int logType,String hasID,String probeID, String deviceProvider, String platform, String provinceID, String cityID,String fwVersion,String exportId,String kPIUTCSec) {
		super();
		this.logType=logType;
		this.hasID=hasID;
		this.probeID = probeID;
		this.deviceProvider = deviceProvider;
		this.platform = platform;
		this.provinceID = provinceID;
		this.cityID = cityID;
		this.fwVersion = fwVersion;
		this.kPIUTCSec = kPIUTCSec.substring(0,kPIUTCSec.length()-2);
		this.exportId  = exportId;
	}
	
	
	public int getLogType() {
		return logType;
	}
	public void setLogType(int logType) {
		this.logType = logType;
	}
	public String getHasID() {
		return hasID;
	}
	public void setHasID(String hasID) {
		this.hasID = hasID;
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
	public String getExportId() {
		return exportId;
	}
	public void setExportId(String exportId) {
		this.exportId = exportId;
	}
	public String getFreezeTime() {
		return freezeTime;
	}
	public void setFreezeTime(String freezeTime) {
		this.freezeTime = freezeTime;
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

	public double getPlayCountRate() {
		return playCountRate;
	}

	public void setPlayCountRate(double playCountRate) {
		this.playCountRate = playCountRate;
	}

	public long getFaultCount() {
		return faultCount;
	}

	public void setFaultCount(long faultCount) {
		this.faultCount = faultCount;
	}

	public double getPlayUserRate() {
		return playUserRate;
	}

	public void setPlayUserRate(double playUserRate) {
		this.playUserRate = playUserRate;
	}

	public long getFaultUser() {
		return faultUser;
	}

	public void setFaultUser(long faultUser) {
		this.faultUser = faultUser;
	}

	public long getPlayCount() {
		return playCount;
	}

	public void setPlayCount(long playCount) {
		this.playCount = playCount;
	}

}
