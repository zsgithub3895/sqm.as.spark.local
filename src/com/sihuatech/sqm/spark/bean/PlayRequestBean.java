package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

/**
 * 首帧指标
 * @author yan
 *
 */
public class PlayRequestBean implements Serializable{
	private static final long serialVersionUID = 1L;
	/**
	 * 播放标识
	 */
	private String hasID;
	/**
	 * 终端厂商
	 */
	private String deviceProvider;
	/**
	 * 牌照方
	 */
	private String platform;
	/**
	 * 省份
	 */
	private String provinceId;
	/**
	 * 地市
	 */
	private String cityId;
	/**
	 * 框架版本
	 */
	private String fwVersion;
	/**
	 * 获取首帧数据的时长
	 */
	private Long latency;
	
	/**
	 * 设备ID
	 */
	private String probeID;
	
	/**
	 * 节目类型
	 */
	private String hasType;
	/**
	 * HTTP流量
	 */
	private long downBytes; 
	/**
	 * 卡顿主要原因 
	 */
	private String expertID;
	/**
	 * 下载时间
	 */
	private long downSeconds;
	/**
	 * 分片 HTTP 响应时延(us)
	 */
	private String httpRspTime;
	/**
	 * 播放列表 HTTP 响应时延(us)
	 */
	private String m3u8HttpRspTime;
	/**
	 * TCP时延长
	 */
	private String tcpSynTime;
	/**
	 * TCP 低窗口包数
	 */
	private String tcpLowWinPkts;
	/**
	 * TCP 重传率(%)
	 */
	private String tcpRetrasRate;	
	
	/**
	 * 卡顿时长
	 */
	private Long freezeTime;
	/**
	 * 卡顿次数
	 */
	private long freezeCount;
	/**
	 * 播放时长
	 */
	private long playSeconds;
	
	private String KPIUTCSec;
	
	private String KPIUTCSec12;//参数的采样时间	上报数据时当前时间    截取到分钟
	private long latencyCount;//首帧数据数量
	private String indexTime;
	private double latencyAVG;

	
	public Long getFreezeTime() {
		return freezeTime;
	}
	public void setFreezeTime(Long freezeTime) {
		this.freezeTime = freezeTime;
	}
	public long getDownBytes() {
		return downBytes;
	}
	public void setDownBytes(long downBytes) {
		this.downBytes = downBytes;
	}
	public long getPlaySeconds() {
		return playSeconds;
	}
	public void setPlaySeconds(long playSeconds) {
		this.playSeconds = playSeconds;
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
	public String getProvinceId() {
		return provinceId;
	}
	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}
	public String getCityId() {
		return cityId;
	}
	public void setCityId(String cityId) {
		this.cityId = cityId;
	}
	public String getFwVersion() {
		return fwVersion;
	}
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}
	public Long getLatency() {
		return latency;
	}
	public void setLatency(Long latency) {
		this.latency = latency;
	}
	public String getProbeID() {
		return probeID;
	}
	public void setProbeID(String probeID) {
		this.probeID = probeID;
	}
	public String getHasType() {
		return hasType;
	}
	public void setHasType(String hasType) {
		this.hasType = hasType;
	}
	public String getExpertID() {
		return expertID;
	}
	public void setExpertID(String expertID) {
		this.expertID = expertID;
	}
	
	public long getDownSeconds() {
		return downSeconds;
	}
	public void setDownSeconds(long downSeconds) {
		this.downSeconds = downSeconds;
	}
	public String getHttpRspTime() {
		return httpRspTime;
	}
	public void setHttpRspTime(String httpRspTime) {
		this.httpRspTime = httpRspTime;
	}
	public String getTcpSynTime() {
		return tcpSynTime;
	}
	public void setTcpSynTime(String tcpSynTime) {
		this.tcpSynTime = tcpSynTime;
	}
	public String getTcpLowWinPkts() {
		return tcpLowWinPkts;
	}
	public void setTcpLowWinPkts(String tcpLowWinPkts) {
		this.tcpLowWinPkts = tcpLowWinPkts;
	}
	public String getTcpRetrasRate() {
		return tcpRetrasRate;
	}
	public void setTcpRetrasRate(String tcpRetrasRate) {
		this.tcpRetrasRate = tcpRetrasRate;
	}
	public String getM3u8HttpRspTime() {
		return m3u8HttpRspTime;
	}
	public void setM3u8HttpRspTime(String m3u8HttpRspTime) {
		this.m3u8HttpRspTime = m3u8HttpRspTime;
	}
	public String getHasID() {
		return hasID;
	}
	public void setHasID(String hasID) {
		this.hasID = hasID;
	}
	public long getFreezeCount() {
		return freezeCount;
	}
	public void setFreezeCount(long freezeCount) {
		this.freezeCount = freezeCount;
	}
	public String getKPIUTCSec() {
		return KPIUTCSec;
	}
	public void setKPIUTCSec(String kPIUTCSec) {
		KPIUTCSec = kPIUTCSec;
	}
	public String getKPIUTCSec12() {
		return KPIUTCSec12;
	}
	public void setKPIUTCSec12(String kPIUTCSec12) {
		KPIUTCSec12 = kPIUTCSec12;
	}
	public long getLatencyCount() {
		return latencyCount;
	}
	public void setLatencyCount(long latencyCount) {
		this.latencyCount = latencyCount;
	}
	public String getIndexTime() {
		return indexTime;
	}
	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}
	public double getLatencyAVG() {
		return latencyAVG;
	}
	public void setLatencyAVG(double latencyAVG) {
		this.latencyAVG = latencyAVG;
	}
	
}
