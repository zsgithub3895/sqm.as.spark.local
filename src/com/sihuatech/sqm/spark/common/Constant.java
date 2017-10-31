package com.sihuatech.sqm.spark.common;

import java.util.Arrays;
import java.util.List;

public class Constant {
	public static final String FIRST_FRAME = "FIRST_FRAME";
	public static final String ALL_PLAY = "ALL_PLAY";// 播放总次数
	public static final String ALL_USER = "ALL_USER";// 播放用户数
	public static final String ALL_START = "ALL_START";// 开机用户数
	public static final String LAG_PLAY = "LAG_PLAY_RATE";//自然缓冲率
	public static final String LAG_CAUSE = "LAG_CAUSE";//故障各原因次数
	public static final String FAULT_USER = "FAULT_USER";//故障用户数
	public static final String PLAY_FAIL = "PLAY_FAIL";//播放失败次数
	public static final String OFFLINE_ALL_PLAY = "ALL_PLAY_OFFLINE";//离线播放总次数
	public static final String PROGRAME_TYPE = "PROGRAME_TYPE";//业务在线时长
	public static final String LAG_PHASE = "LAG_PHASE";//卡顿分布
	public static final String EPG_TIME_DELAY = "EPG_TIME_DELAY";//EPG时延分布
	public static final String EPG_TIME_DELAY15R = "EPG_TIME_DELAY15R";//EPG时延分布15
	public static final String EPG_TIME_DELAY60R = "EPG_TIME_DELAY60R";//EPG时延分布60
    public static final String EVENT_TREND = "EVENT_TREND";//事件趋势
	public static final String HTTP_ERRORCODE = "HTTP_ERRORCODE";//http错误

	public static final String KEY_PREFIX = "EPG_RSP";//epg延迟
	public static final String KEY_PREFIX15R = "EPG_RSP15R";//epg延迟15min
	public static final String KEY_PREFIX60R = "EPG_RSP60R";//epg延迟60min
	public static final String KEY_PREFIX_IMAGE = "IMAGE_LOAD";//图片加载(及时率和成功率)
	
	public static final String DIRECTORY_DELIMITER = System.getProperty("file.separator");
	public static final String DELIMITER = "-";

	/* 时间维度取值范围 */
	public static final List<String> PERIOD_ENUM = Arrays.asList("HOUR", "DAY", "WEEK", "MONTH", "QUARTER","HALFYEAR", "YEAR");
	public static final String DOWN_BYTES = "DOWN_BYTES";

	/* 机顶盒状态任务号 */ 
	public static final String STATE_NUM = "200";// 表示所有任务
	public static final String PLAY_NUM = "201";// 表示总播放次数、流用户数
	public static final String START_NUM = "202";// 开机用户数
	/* 机顶盒状态离线分析任务枚举值 */
	public static final List<String> STATE_TASK_ENUM = Arrays.asList(STATE_NUM, PLAY_NUM,START_NUM);

	/**
	 * 播放请求日志离线分析任务号
	 */
	public static final String PLAYREQUEST_ALL_TASK = "300";// 所有任务
	public static final String PLAYREQUEST_FIRSTFRAME_TASK = "301";// 首帧
	public static final String PLAYREQUEST_LAG_PHASE = "305";//卡顿分布
	public static final String PLAYREQUEST_FREEZE = "306";//总业务分布  HTTP流量  业务在线
	public static final String STREAM_AND_PLAY_COUNT = "307";//流用户 播放次数
	/*  播放请求离线分析任务枚举值 */
	public static final List<String> PLAYREQUEST_TASK_ENUM = Arrays.asList(PLAYREQUEST_ALL_TASK,
			PLAYREQUEST_FIRSTFRAME_TASK,PLAYREQUEST_LAG_PHASE,PLAYREQUEST_FREEZE,STREAM_AND_PLAY_COUNT);
	
	/* 卡顿行为任务号 */
	public static final String LAG_NUM = "400";// 表示所有任务
	public static final String FAULT_NUM = "402";//// 故障原因
	public static final String CAUSE_NUM = "403";// 故障原因分布
	public static final String NATURAL_NUM = "404";// 自然播放率
	/* 卡顿行为离线分析任务枚举值 */
	public static final List<String> LAG_TASK_ENUM = Arrays.asList(LAG_NUM,FAULT_NUM,CAUSE_NUM,NATURAL_NUM);
	
	/**
	 * 播放失败日志离线分析任务号
	 */
	public static final String PLAY_FIAL_ALL_TASK = "500";
	public static final String PLAY_SUCC_TASK = "501";
	/* 播放失败日志离线分析任务枚举值 */
	public static final List<String> FAIL_TASK_ENUM = Arrays.asList(PLAY_FIAL_ALL_TASK, PLAY_SUCC_TASK);
	
	/**
	 * EPG日志离线分析任务号
	 */
	public static final String EPG_RESPONSE_ALL_TASK = "600";// 所有任务
	public static final String EPG_RESPONSE_DISTRIBUTION_TASK = "602";// EPG时延分布
	public static final String DETAIL_LOAD_SUCC_TASK = "603";// 详情页加载成功率
	/* EPG日志离线分析任务枚举值 */
	public static final List<String> EPG_TASK_ENUM = Arrays.asList(EPG_RESPONSE_ALL_TASK,
			EPG_RESPONSE_DISTRIBUTION_TASK,DETAIL_LOAD_SUCC_TASK);

	/**
	 * 图片加载日志离线分析任务号
	 */
	public static final String IMAGE_ALL_TASK = "700";// 所有任务
	public static final String IMAGE_FAST_LOAD_RATE_TASK = "701";// 图片加载及时率和成功率
	/* 图片加载离线分析任务枚举值 */
	public static final List<String> IMAGE_TASK_ENUM = Arrays.asList(IMAGE_ALL_TASK, IMAGE_FAST_LOAD_RATE_TASK);
	
	public static final String LOGIN_ALL_TASK = "900";//所有任务
	public static final String LOGIN_SUCC_TASK = "901";//登录成功率指标
	/* 机顶盒登陆日志离线分析任务枚举值*/
	public static final List<String> LOGIN_TASK_ENUM = Arrays.asList(LOGIN_ALL_TASK, LOGIN_SUCC_TASK);
	
	public static final String ERRORCODE_ALL_TASK = "1000";//所有任务
	public static final String ERRORCODE_DISTRIBUTION_TASK = "1001";//http错误码分布
	/* 错误码日志离线分析任务枚举值*/
	public static final List<String> ERRORCODE_TASK_ENUM = Arrays.asList(ERRORCODE_ALL_TASK, ERRORCODE_DISTRIBUTION_TASK);
	
	/**
	 * 事件趋势离线分析任务号
	 */
	public static final String EVENT_ALL_TASK = "1100";
	public static final List<String> EVENT_TASK_ENUM =Arrays.asList(EVENT_ALL_TASK);


	/**
	 * redis中list的存储最大值
	 */
	public static final int REDIS_LIST_SIZE = 30;
	
	/**
	 *实时计算15分钟,1小时，redis中后缀为R15,R60
	 */
	public static final String R15 = "R15";
	public static final String R60 = "R60";
	
	public static final String FIF_QUARTER = "00";
	public static final String SEC_QUARTER = "15";
	public static final String THI_QUARTER = "30";
	public static final String FOU_QUARTER = "45";
	public static final String SUFFIX = "5959";
}
