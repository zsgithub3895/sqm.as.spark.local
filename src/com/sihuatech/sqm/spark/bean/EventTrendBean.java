package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EventTrendBean implements Serializable {
	/**
	 * 事件趋势
	 */
	private static final long serialVersionUID = 1L;
	// 设备ID
	private String probeID;
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
	/** 事件类型 */
	private String eventID;
	/** 发生次数 */
	private long count;
	
	private String kpiUtcSec;
	private String indexTime;
	
	/**  运营商网络问题   */
	private long operator;
	/**  家庭网络问题    */
	private long family;
	/** CDN平台问题     */
	private long cdn;
	/** 终端问题    */
	private long stb;

	private long event1;
	private long event2;
	private long event3;
	private long event4;
	private long event6;
	private long event7;
	private long event8;
	private long event10;
	private long event11;
	private long event12;
	private long event13;
	private long event14;
	private long event15;
	private long event16;

	public String getProbeID() {
		return probeID;
	}

	public void setProbeID(String probeID) {
		this.probeID = probeID;
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

	public String getEventID() {
		return eventID;
	}

	public void setEventID(String eventID) {
		this.eventID = eventID;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String getKpiUtcSec() {
		return kpiUtcSec;
	}

	public void setKpiUtcSec(String kpiUtcSec) {
		this.kpiUtcSec = kpiUtcSec;
	}

	public long getOperator() {
		return operator;
	}

	public long getFamily() {
		return family;
	}

	public long getCdn() {
		return cdn;
	}

	public long getStb() {
		return stb;
	}

	public void setOperator(long operator) {
		this.operator = operator;
	}

	public void setFamily(long family) {
		this.family = family;
	}

	public void setCdn(long cdn) {
		this.cdn = cdn;
	}

	public void setStb(long stb) {
		this.stb = stb;
	}

	public long getEvent1() {
		return event1;
	}

	public long getEvent2() {
		return event2;
	}

	public long getEvent3() {
		return event3;
	}

	public long getEvent4() {
		return event4;
	}

	public long getEvent6() {
		return event6;
	}

	public long getEvent7() {
		return event7;
	}

	public long getEvent8() {
		return event8;
	}

	public long getEvent10() {
		return event10;
	}

	public long getEvent11() {
		return event11;
	}

	public long getEvent12() {
		return event12;
	}

	public long getEvent13() {
		return event13;
	}

	public long getEvent14() {
		return event14;
	}

	public long getEvent15() {
		return event15;
	}

	public long getEvent16() {
		return event16;
	}

	public void setEvent1(long event1) {
		this.event1 = event1;
	}

	public void setEvent2(long event2) {
		this.event2 = event2;
	}

	public void setEvent3(long event3) {
		this.event3 = event3;
	}

	public void setEvent4(long event4) {
		this.event4 = event4;
	}

	public void setEvent6(long event6) {
		this.event6 = event6;
	}

	public void setEvent7(long event7) {
		this.event7 = event7;
	}

	public void setEvent8(long event8) {
		this.event8 = event8;
	}

	public void setEvent10(long event10) {
		this.event10 = event10;
	}

	public void setEvent11(long event11) {
		this.event11 = event11;
	}

	public void setEvent12(long event12) {
		this.event12 = event12;
	}

	public void setEvent13(long event13) {
		this.event13 = event13;
	}

	public void setEvent14(long event14) {
		this.event14 = event14;
	}

	public void setEvent15(long event15) {
		this.event15 = event15;
	}

	public void setEvent16(long event16) {
		this.event16 = event16;
	}

	public String getIndexTime() {
		return indexTime;
	}

	public void setIndexTime(String indexTime) {
		this.indexTime = indexTime;
	}
	
}