package com.sihuatech.sqm.spark.strategy;

import org.apache.commons.lang3.StringUtils;

import com.sihuatech.sqm.spark.bean.TerminalInfo;
import com.sihuatech.sqm.spark.common.Constants;

/**
 * 设备信息策略
 * @author chenyj
 *
 */
public class InfoStrategy {
	/*** 日志类型 ***/
	public static final String LOG_TYPE = "1";
	/*** 日志标识 ***/
	public static final String LOG_ID = "INFO";
	/*** 日志有效字段数 ***/
	public static final int VALID_SIZE = 17;
	/*** 实时计算维度 ***/
	public static final String[] RT_DIMENSIONS = { "provinceID", "cityID", "platform",
			"deviceProvider", "fwVersion" };
	/*** 离线计算维度 ***/
	public static final String[] NRT_DIMENSIONS = { "provinceID", "cityID", "platform",
			"deviceProvider", "fwVersion" };
	
	public static boolean validate(String log) {
		boolean isValid = false;
		if (StringUtils.isNotBlank(log)) {
			String[] logArr = log.split(Constants.FIELD_DELIMITER, -1);
			isValid = logArr.length >= VALID_SIZE && LOG_TYPE.equals(logArr[0]);
		}
		return isValid;
	}

	public static TerminalInfo buildBean(String log) {
		TerminalInfo info = null;
		if (StringUtils.isNotBlank(log)) {
			String[] logArr = log.split(Constants.FIELD_DELIMITER, -1);
			info = new TerminalInfo();
			info.setLogType(logArr[0]);
			info.setProbeID(logArr[1]);
			info.setDeviceID(logArr[2]);
			info.setDeviceProvider(logArr[3]);
			info.setPlatform(logArr[4]);
			info.setProvinceID(logArr[5]);
			info.setCityID(logArr[6]);
			info.setFwVersion(logArr[7]);
			info.setDeviceModelID(logArr[8]);
			info.setDeviceVersion(logArr[9]);
			info.setMode(logArr[10]);
			info.setUserID(logArr[11]);
			info.setProbeIP(logArr[12]);
			info.setMac(logArr[13]);
			info.setMac2(logArr[14]);
			info.setEvVersion(logArr[15]);
			info.setManagerName(logArr[16]);
		}
		return info;
	}
}
