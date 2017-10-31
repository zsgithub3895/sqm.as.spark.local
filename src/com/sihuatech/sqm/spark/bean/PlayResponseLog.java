package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;
import java.util.List;

public class PlayResponseLog implements Serializable{
	private static final long serialVersionUID = 1L;
	private int logType; // 日志类型
	private String hasID;//播放标识
	private String startSecond;//节目开始时间yyyyMMddhhmmss
	private String probeID; // 设备ID
	private String deviceProvider; // 终端厂商
	private String platform; // 牌照方
	private String provinceID; // 省份
	private String cityID; // 地市
	private String fwVersion; // 框架版本
	private String hasType;//节目类型
	private String URL;//节目的URL
	private long playSeconds;//节目播放时长
	private long freezeCount;//卡顿次数
	private long freezeTime;//卡顿时长
	private String endSecond;//节目播放结束时间
	private long latency;//首帧数据时长
	private long downBytes;//下载字节数（节目总流量）
	private String expertID;//卡顿主要原因
	private long downSeconds;//下载时间
	private String httpRspTime;//分片 HTTP 响应时延(us)
	private String m3u8HttpRspTime;//播放列表 HTTP 响应时延(us)
	private String tcpSynTime;//TCP时延长
	private String tcpLowWinPkts;//TCP 低窗口包数
	private String tcpRetrasRate;//TCP 重传率(%)
	
	private long userNumber;
	private String indexTime;
	private double latencyAVG;
	private long downBytesSUM;
	private long playSum;//播放持续总时长
	
	private String KPIUTCSec;//参数的采样时间	上报数据时当前时间
	private String KPIUTCSec12;//参数的采样时间	上报数据时当前时间    截取到分钟
	
//	private String sourceIPAddress;//视频源 IP地址		
//	private String destIPAddress;//视频目的IP 地址		
//	private String programName;//节目名称(utf-8)编码		
//	private String fileSize;//文件大小(bytes),当前所下载视频文件的大小（即总字节数）	
//	private String buffSeconds;//缓冲时长(seconds)		
//	private String KPIUTCSec;//参数的采样时间	上报数据时当前时间	
//	private String downSeconds;//下载时间(seconds)。到当前采样点为止,视频文件下载持续的时长（以秒为单位）	
//	private String svrChgCount;//服务器更换次数。视频播放过程中，源视频服务器切换的次数	
//	private String bandWChgCount;//带宽更换次数 视频播放过程中， HLS分片码率切换的次数	
//	private String statusCode;//HTTP返回码,比如 200 OK 平均值是最后收到的响应码	
//	private String httpRspTime;//分片 HTTP 响应时延(us)		
//	private String m3u8HttpRspTime;//播放列表 HTTP 响应时延(us)		
//	private String moveHttpRspTime;//调度服务器 HTTP 响应时延(us)		
//	private String avgThroughput;//下载速率(bps)	视频流的下载吞吐率，该指标包括平均值、最大值、最小值	
//	private String tcpSynTime;//TCP 建立时间(us)		
//	private String m3u8TcpSynTime;//播放列表 TCP 建立时间(us)		
//	private String moveTcpSynTime;//调度服务器 TCP 建立时间(us)		
//	private String tcpOutSeqPkts;//TCP 乱序包数		
//	private String tcpRetrasPkts;//TCP 重传包数		
//	private String tcpDupPkts;//TCP 重复包数		
//	private String tcpLowWinPkts;//TCP 低窗口包数		
//	private String tcpLowWinSize;//TCP 低窗口大小		
//	private String tcpRetrasRate;//TCP 重传率(%)		
//	private String tcpDupRate;//TCP 重复率(%)		
//	private String segCount;//下载分片数 	节目流中，下载的 HLS 分片个数	
//	private String MOS;//分片 OV-Score 该指标包括平均值、最大值、最小值	
//	private String segDelay;//HLS 分片下载时间偏离		
//	private String segLag;//HLS 分片间隔偏离	
//	private String usedCPU;//CPU 使用率		
//	private String usedMEM;//Memory 占用率		
//	private String width;//视频宽度,节目的分辨率宽度	
//	private String height;//视频高度,节目的分辨率宽度	
//	private String frameRate;//视频帧率,节目的帧率	
//	private String reserve1;//预留字段1
//	private String reserve2;//预留字段2
	private long totalBussiness;//业务在线指标总和
	private Double bussinessRate;//业务在线指标占比
	private List<String> probeIDList;//一次数据所有的probeID

	public PlayResponseLog() {}
	
    public PlayResponseLog(String indexTime, long userNumber) {
		this.indexTime = indexTime;
		this.userNumber = userNumber;
	}
    public PlayResponseLog(long userNumber, long totalBussiness, String provinceID, List<String> probeIDList, String hasType) {
		this.userNumber = userNumber;
		this.totalBussiness = totalBussiness;
		this.provinceID = provinceID;
		this.probeIDList = probeIDList;
		this.hasType = hasType;
	}
	public long getLatency() {
		return latency;
	}
	public void setLatency(long latency) {
		this.latency = latency;
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
	public String getStartSecond() {
		return startSecond;
	}
	public void setStartSecond(String startSecond) {
		this.startSecond = startSecond;
	}
	public String getProbeID() {
		return probeID;
	}
	public void setProbeID(String probeID) {
		this.probeID = probeID;
	}
	public String getCityID() {
		return cityID;
	}
	public void setCityID(String cityID) {
		this.cityID = cityID;
	}
	public String getHasType() {
		return hasType;
	}
	public void setHasType(String hasType) {
		this.hasType = hasType;
	}
	public String getURL() {
		return URL;
	}
	public void setURL(String uRL) {
		URL = uRL;
	}

	public long getFreezeCount() {
		return freezeCount;
	}

	public void setFreezeCount(long freezeCount) {
		this.freezeCount = freezeCount;
	}

	public long getPlaySeconds() {
		return playSeconds;
	}
	public void setPlaySeconds(long playSeconds) {
		this.playSeconds = playSeconds;
	}
	public long getFreezeTime() {
		return freezeTime;
	}
	public void setFreezeTime(long freezeTime) {
		this.freezeTime = freezeTime;
	}
	public String getEndSecond() {
		return endSecond;
	}
	public void setEndSecond(String endSecond) {
		this.endSecond = endSecond;
	}
	
	public long getDownBytes() {
		return downBytes;
	}
	public void setDownBytes(long downBytes) {
		this.downBytes = downBytes;
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
	public String getKPIUTCSec12() {
		return KPIUTCSec12;
	}

	public void setKPIUTCSec12(String kPIUTCSec12) {
		this.KPIUTCSec12 = kPIUTCSec12;
	}

	public PlayResponseLog(String deviceProvider, String platform, String provinceID, String fwVersion,String hasType,
			long latency,long downBytes,String expertID,String probeID,String hasID,String KPIUTCSec,String cityID,
			long playSeconds,long downSeconds,long freezeTime,String KPIUTCSec12) {
		super();
		this.deviceProvider = deviceProvider;
		this.platform = platform;
		this.provinceID = provinceID;
		this.fwVersion = fwVersion;
		this.hasType = hasType;
		this.latency = latency;
		this.downBytes = downBytes;
		this.expertID = expertID;
		this.probeID = probeID;
		this.hasID = hasID;
		//this.startSecond = startSecond;
		this.KPIUTCSec = KPIUTCSec;
		this.cityID = cityID;
		this.playSeconds = playSeconds;
		this.downSeconds = downSeconds;
		this.freezeTime = freezeTime;
		this.KPIUTCSec12 = KPIUTCSec12;
	}
	public PlayResponseLog(String deviceProvider, String platform, String provinceID, String fwVersion, String probeID,
			long playSeconds, long freezeTime, String hasID, String cityID) {
		super();
		this.deviceProvider = deviceProvider;
		this.platform = platform;
		this.provinceID = provinceID;
		this.fwVersion = fwVersion;
		this.probeID = probeID;
		this.playSeconds = playSeconds;
		this.freezeTime = freezeTime;
		this.hasID = hasID;
		this.cityID = cityID;
	}

	public long getUserNumber() {
		return userNumber;
	}

	public void setUserNumber(long userNumber) {
		this.userNumber = userNumber;
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

	public long getDownBytesSUM() {
		return downBytesSUM;
	}

	public void setDownBytesSUM(long downBytesSUM) {
		this.downBytesSUM = downBytesSUM;
	}

	public String getKPIUTCSec() {
		return KPIUTCSec;
	}

	public void setKPIUTCSec(String kPIUTCSec) {
		KPIUTCSec = kPIUTCSec;
	}

	public long getPlaySum() {
		return playSum;
	}

	public void setPlaySum(long playSum) {
		this.playSum = playSum;
	}

	public long getTotalBussiness() {
		return totalBussiness;
	}

	public void setTotalBussiness(long totalBussiness) {
		this.totalBussiness = totalBussiness;
	}

	public Double getBussinessRate() {
		return bussinessRate;
	}

	public void setBussinessRate(Double bussinessRate) {
		this.bussinessRate = bussinessRate;
	}

	public List<String> getProbeIDList() {
		return probeIDList;
	}

	public void setProbeIDList(List<String> probeIDList) {
		this.probeIDList = probeIDList;
	}
	
}
