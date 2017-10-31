package com.sihuatech.sqm.spark.common;

/**
 * 系统常量
 * @author chenyj
 *
 */
public final class Constants {
	/*** 日志类型：设备信息 ***/
	public static final String LOGTYPE_INFO = "1";
	/*** 日志类型：设备状态 ***/
	public static final String LOGTYPE_STATE = "2";
	/*** 日志类型：播放请求 ***/
	public static final String LOGTYPE_PLAYREQ = "3";
	/*** 日志类型：卡顿行为 ***/
	public static final String LOGTYPE_LAG = "4";
	/*** 日志类型：播放失败 ***/
	public static final String LOGTYPE_PLAYFAIL = "5";
	/*** 日志类型：EPG响应 ***/
	public static final String LOGTYPE_EPG = "6";
	/*** 日志类型：图片加载 ***/
	public static final String LOGTYPE_IMAGE = "7";
	/*** 日志分隔符 ***/
	public static final String FIELD_DELIMITER = String.valueOf((char) 0x7F);
	/*** 路径分隔符 ***/
	public static final String DIRECTORY_DELIMITER = System.getProperty("file.separator");

	/*** 日志标识：设备状态 ***/
	public static final String LOG_STATE = "STATE";
	
	private Constants() {
		// disable explicit object creation
	}
}
