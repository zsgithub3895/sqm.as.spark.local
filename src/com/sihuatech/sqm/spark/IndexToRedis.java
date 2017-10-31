package com.sihuatech.sqm.spark;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.alibaba.fastjson.JSON;
import com.sihuatech.sqm.spark.bean.EPGResponseBean;
import com.sihuatech.sqm.spark.bean.EventTrendBean;
import com.sihuatech.sqm.spark.bean.HttpErrorCodeLog;
import com.sihuatech.sqm.spark.bean.ImageLoadBean;
import com.sihuatech.sqm.spark.bean.InfoCountBean;
import com.sihuatech.sqm.spark.bean.LagCauseBean;
import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;
import com.sihuatech.sqm.spark.bean.LagPhaseRateBean;
import com.sihuatech.sqm.spark.bean.PlayFailLog;
import com.sihuatech.sqm.spark.bean.PlayRequestBean;
import com.sihuatech.sqm.spark.bean.PlayResponseLog;
import com.sihuatech.sqm.spark.bean.TerminalState;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.redis.RedisClient;
import com.sihuatech.sqm.spark.strategy.InfoStrategy;
import com.sihuatech.sqm.spark.util.DateUtil;

public class IndexToRedis {
	private static Logger logger = Logger.getLogger(IndexToRedis.class);
	static DecimalFormat df = new DecimalFormat("0.0000");
	private static final int POLLING_TIMES = 3; // 默认轮询3次
	private static RedisClient client = null;
	private static final Pattern TAB = Pattern.compile("\t");
	
	static {
		client = new RedisClient();
		try {
			client.init();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void toRedis(HashMap<String, Double> hm) {
		for (Entry<String, Double> en : hm.entrySet()) {
			if (en != null && en.getValue() != null) {
				client.setObject(en.getKey(), Double.parseDouble(df.format(en.getValue())));
			}
		}
	}

	public static void toRedisToLong(HashMap<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					List<LagPhaseBehaviorLog> list = (List<LagPhaseBehaviorLog>) client.getObject(en.getKey() + "#1");
					if (null == list || list.size() == 0) {
						list = new ArrayList<LagPhaseBehaviorLog>();
					} else if (list.size() > 29) {
						list.remove(0);
					}
					list.add(new LagPhaseBehaviorLog(time, en.getValue()));
					client.setObject(en.getKey() + "#1", list);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	/**
	 * 图片加载日志实时
	 */
	public static void imageToRedis(Map<String, String> hm) {
		Calendar hour = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(0));
		Calendar day = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(1));
		Calendar week = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(2));
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String pre_hour = df.format(hour.getTime());
		String pre_day = df.format(day.getTime());
		String pre_week = sdf.format(week.getTime());
		try {
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] keys = TAB.split(en.getKey());
					String values[] = en.getValue().split("#", -1);
					String kpiUtcSec = keys[1];
					String fif_index_time = DateUtil.getQuarterOfHour(kpiUtcSec);
					String hour_index_time = kpiUtcSec.substring(0,10);
					long pageDurSucCnt = Long.valueOf(values[0]);// 加载成功数
					long pageDurLE3sCnt = Long.valueOf(values[1]);// 加载及时数
					long requests_total = Long.valueOf(values[2]);// http请求总次数
					Map<String, ImageLoadBean> map = (Map<String, ImageLoadBean>) client.getObject(Constant.KEY_PREFIX_IMAGE + "#" + keys[0]);
					Map<String, ImageLoadBean> fif_map = (Map<String, ImageLoadBean>) client.getObject(Constant.KEY_PREFIX_IMAGE + "#" + keys[0]+"#"+Constant.R15);
					Map<String, ImageLoadBean> hour_map = (Map<String, ImageLoadBean>) client.getObject(Constant.KEY_PREFIX_IMAGE + "#" + keys[0]+"#"+Constant.R60);
					/**1分钟*/
					imageCommen(keys[0],map,kpiUtcSec,pageDurSucCnt,pageDurLE3sCnt,requests_total,pre_hour);
					/**15分钟*/
					String fif_key = keys[0]+"#"+Constant.R15;
					imageCommen(fif_key,fif_map,fif_index_time,pageDurSucCnt,pageDurLE3sCnt,requests_total,pre_day);
					/**1小时*/
					String hour_key = keys[0]+"#"+Constant.R60;
					imageCommen(hour_key,hour_map,hour_index_time,pageDurSucCnt,pageDurLE3sCnt,requests_total,pre_week);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	public static void epgToRedisOnLong(Map<String, String> hm, String time, String period) {
		try {
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] keys = TAB.split(en.getKey());
					String kPIUTCSec = keys[2];
					String values[] = en.getValue().split("#",-1);
					Long allHttpRspTime =  Long.valueOf(values[0]);
					Long allRequest =  Long.valueOf(values[1]);
					Map<String,EPGResponseBean> redisCache = (Map<String,EPGResponseBean>) client.getObject(Constant.KEY_PREFIX + "#" + keys[0]);
					Map<String,EPGResponseBean> redisCache15R = (Map<String,EPGResponseBean>) client.getObject(Constant.KEY_PREFIX15R + "#" + keys[0]);
					Map<String,EPGResponseBean> redisCache60R = (Map<String,EPGResponseBean>) client.getObject(Constant.KEY_PREFIX60R + "#" + keys[0]);
					//分别更新1 15 60 redis 数据 
					updateCache(keys,kPIUTCSec,allHttpRspTime,allRequest,redisCache);
					updateCache(keys,DateUtil.getQuarterOfHour(kPIUTCSec),allHttpRspTime,allRequest,redisCache15R);
					updateCache(keys,kPIUTCSec.substring(0, kPIUTCSec.length()-2)+"00",allHttpRspTime,allRequest,redisCache60R);
					//清除redis 超时数据
					cancleTimeOutCache(redisCache,60*60*1000);//一小时  毫秒数
					cancleTimeOutCache(redisCache15R,24*60*60*1000);//一天  毫秒数
					cancleTimeOutCache(redisCache60R,7*24*60*60*1000);//七天  毫秒数
					client.setObject(Constant.KEY_PREFIX + "#" + keys[0], redisCache);
					client.setObject(Constant.KEY_PREFIX15R + "#" + keys[0], redisCache15R);
					client.setObject(Constant.KEY_PREFIX60R + "#" + keys[0], redisCache60R);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	private static void cancleTimeOutCache(Map<String, EPGResponseBean> redisCache, int timeOut) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		Iterator<Map.Entry<String,EPGResponseBean>> it = redisCache.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,EPGResponseBean> entry = it.next();  
			try {
				Long time = df.parse(entry.getKey()).getTime();
				if(time > timeOut){
					it.remove();
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	private static void updateCache(String[] keys, String kPIUTCSec, Long allHttpRspTime, Long allRequest, Map<String, EPGResponseBean> redisCache) {
		if (null != redisCache && redisCache.size() > 0) {
			EPGResponseBean ePGResponseBean = redisCache.get(keys[2]);
			if(null == ePGResponseBean){
				ePGResponseBean = new EPGResponseBean();
				ePGResponseBean.setIndexTime(kPIUTCSec);
				long[] avgEpgDur = new long[3];
				ePGResponseBean.setAllHttpRspTime(allHttpRspTime);
				ePGResponseBean.setAllRequest(allRequest);
				avgEpgDur[Integer.parseInt(keys[1])-1] = ePGResponseBean.getAllHttpRspTime()/ePGResponseBean.getAllRequest();
				ePGResponseBean.setAvgEpgDur(avgEpgDur);
				redisCache.put(kPIUTCSec, ePGResponseBean);
			}else{
				ePGResponseBean.setAllHttpRspTime(allHttpRspTime + ePGResponseBean.getAllHttpRspTime());
				ePGResponseBean.setAllRequest(allRequest + ePGResponseBean.getAllRequest());
				long[] avgEpgDur = ePGResponseBean.getAvgEpgDur();
				avgEpgDur[Integer.parseInt(keys[1])-1] = ePGResponseBean.getAllHttpRspTime()/ePGResponseBean.getAllRequest();
				ePGResponseBean.setAvgEpgDur(avgEpgDur);
				redisCache.put(kPIUTCSec, ePGResponseBean);
			}
		}else{
			redisCache = new HashMap<String,EPGResponseBean>();
			EPGResponseBean ePGResponseBean = new EPGResponseBean();
			ePGResponseBean.setIndexTime(kPIUTCSec);
			long[] avgEpgDur = new long[3];
			ePGResponseBean.setAllHttpRspTime(allHttpRspTime);
			ePGResponseBean.setAllRequest(allRequest);
			avgEpgDur[Integer.parseInt(keys[1])-1] = ePGResponseBean.getAllHttpRspTime()/ePGResponseBean.getAllRequest();
			ePGResponseBean.setAvgEpgDur(avgEpgDur);
			redisCache.put(kPIUTCSec, ePGResponseBean);
		}
	}

	/** 实时 EPG响应时延分布入redis **/
	public static void epgTimeDelayToRedis(Map<String, String> hm, String time) {
		try {
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] keys = TAB.split(en.getKey());
					String kPIUTCSec = keys[2];
					String[] value = en.getValue().split("#",-1);
					Map<String,EPGResponseBean> redisCacheMin = (Map<String,EPGResponseBean>) client.getObject(Constant.EPG_TIME_DELAY + "#" + keys[0]);
					Map<String,EPGResponseBean> redisCache15R = (Map<String,EPGResponseBean>) client.getObject(Constant.EPG_TIME_DELAY15R + "#" + keys[0]);
					Map<String,EPGResponseBean> redisCache60R = (Map<String,EPGResponseBean>) client.getObject(Constant.EPG_TIME_DELAY60R + "#" + keys[0]);
					updateCacheForTimeDelay(redisCacheMin,keys,value,kPIUTCSec);
					updateCacheForTimeDelay(redisCache15R,keys,value,DateUtil.getQuarterOfHour(kPIUTCSec));
					updateCacheForTimeDelay(redisCache60R,keys,value,kPIUTCSec.substring(0, kPIUTCSec.length()-2)+"00");
					//清除redis 超时数据
					cancleTimeOutCache(redisCacheMin,60*60*1000);//一小时  毫秒数
					cancleTimeOutCache(redisCache15R,24*60*60*1000);//一天  毫秒数
					cancleTimeOutCache(redisCache60R,7*24*60*60*1000);//七天  毫秒数
					client.setObject(Constant.EPG_TIME_DELAY + "#" + keys[0], redisCacheMin);
					client.setObject(Constant.EPG_TIME_DELAY15R + "#" + keys[0], redisCache15R);
					client.setObject(Constant.EPG_TIME_DELAY60R + "#" + keys[0], redisCache60R);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	private static void updateCacheForTimeDelay(Map<String, EPGResponseBean> redisCacheMin, String[] keys,String[] value, String kPIUTCSec) {
		if (null != redisCacheMin && redisCacheMin.size() > 0) {
			EPGResponseBean epg = redisCacheMin.get(keys[2]);
			if(null == epg){
				epg = new EPGResponseBean();
				epg.setIndexTime(kPIUTCSec);
				long[][] epgTimeDelay = new long[3][4];
				epgTimeDelay[Integer.parseInt(keys[1])-1][0] = Long.parseLong(value[0]);
				epgTimeDelay[Integer.parseInt(keys[1])-1][1] = Long.parseLong(value[1]);
				epgTimeDelay[Integer.parseInt(keys[1])-1][2] = Long.parseLong(value[2]);
				epgTimeDelay[Integer.parseInt(keys[1])-1][3] = Long.parseLong(value[3]);
				epg.setEpgTimeDelay(epgTimeDelay);
				redisCacheMin.put(kPIUTCSec, epg);
			}else{
			    epg.getEpgTimeDelay()[Integer.parseInt(keys[1])-1][0] += Long.parseLong(value[0]);
			    epg.getEpgTimeDelay()[Integer.parseInt(keys[1])-1][1] += Long.parseLong(value[1]);
			    epg.getEpgTimeDelay()[Integer.parseInt(keys[1])-1][2] += Long.parseLong(value[2]);
			    epg.getEpgTimeDelay()[Integer.parseInt(keys[1])-1][3] += Long.parseLong(value[3]);
			    redisCacheMin.put(kPIUTCSec, epg);
			}
		}else{
			redisCacheMin = new HashMap<String,EPGResponseBean>();
			EPGResponseBean epg = new EPGResponseBean();
			epg.setIndexTime(kPIUTCSec);
			long[][] epgTimeDelay = new long[3][4];
			epgTimeDelay[Integer.parseInt(keys[1])-1][0] = Long.parseLong(value[0]);
			epgTimeDelay[Integer.parseInt(keys[1])-1][1] = Long.parseLong(value[1]);
			epgTimeDelay[Integer.parseInt(keys[1])-1][2] = Long.parseLong(value[2]);
			epgTimeDelay[Integer.parseInt(keys[1])-1][3] = Long.parseLong(value[3]);
			epg.setEpgTimeDelay(epgTimeDelay);
			redisCacheMin.put(kPIUTCSec, epg);
		}
	}

	/** 实时 自然缓冲率入redis 
	 * @param period **/
	public static void playCountToRedis(HashMap<String, Long> hm, String time, String period) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					String key = Constant.LAG_PLAY + period + "#" + en.getKey();
					List<LagPhaseBehaviorLog> listL = (List<LagPhaseBehaviorLog>) client.getObject(key);
					if (null == listL || listL.size() == 0) {
						listL = new ArrayList<LagPhaseBehaviorLog>();
					} else if (listL.size() > 29) {
						listL.remove(0);
					}
					LagPhaseBehaviorLog lag = new LagPhaseBehaviorLog();
					lag.setPlayCount(en.getValue());
					lag.setIndexTime(time);
					listL.add(lag);
					client.setObject(key, listL);
					}
				}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	/**
	 * 事件趋势实时
	 */
	public static void eventTrendToRedis(HashMap<String, Long> hm, String time){
			Calendar hour = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(0));
			Calendar day = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(1));
			Calendar week = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(2));
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
			String pre_hour = df.format(hour.getTime());
			String pre_day = df.format(day.getTime());
			String pre_week = sdf.format(week.getTime());
			try {
				for (Entry<String, Long> en : hm.entrySet()) {
					if (en != null && en.getValue() != null) {
						String[] keys = TAB.split(en.getKey());
						int eventID = 0;
						eventID = Integer.valueOf(keys[1]);
						String kpiUtcSec = keys[2];
						long value = en.getValue();
						String fif_index_time = DateUtil.getQuarterOfHour(kpiUtcSec);
						String hour_index_time = kpiUtcSec.substring(0,10);
						Map<String,EventTrendBean> map =(Map<String, EventTrendBean>) client.getObject(Constant.EVENT_TREND + "#" +keys[0]);
						Map<String,EventTrendBean> fif_map =(Map<String, EventTrendBean>) client.getObject(Constant.EVENT_TREND + "#" +keys[0]+"#"+Constant.R15);
						Map<String,EventTrendBean> hour_map =(Map<String, EventTrendBean>) client.getObject(Constant.EVENT_TREND + "#" +keys[0]+"#"+Constant.R60);
						/**1分钟**/
						eventCommen(keys[0],map,kpiUtcSec,eventID,value,pre_hour);
						/**15分钟*/
						String fif_key = keys[0]+"#"+Constant.R15;
						eventCommen(fif_key,fif_map,fif_index_time,eventID,value,pre_day);
						/**1小时**/
						String hour_key = keys[0]+"#"+Constant.R60;
						eventCommen(hour_key,hour_map,hour_index_time,eventID,value,pre_week);
					}
				}
			} catch (Exception e) {
				logger.error("redis初始化失败!", e);
			}
		}
	
	public static void playSuccToRedisOffline(Map<String, Long> hm, String period, String time) {
		HashMap<String, Double> map = new HashMap<String, Double>();
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					double r = 0d;
					String allKey = Constant.OFFLINE_ALL_PLAY + "#" +en.getKey()+"#"+time;
					if (null != client.getObject(allKey)) {
						long value = (Long) client.getObject(allKey);
						if (value != 0) {
							long playSuccCount = value - en.getValue();
							r = Double.parseDouble(df.format((double) playSuccCount / value));
							/*String succKey = Constant.PLAY_SUCC + "#" + en.getKey();
							client.setObject(succKey, r);*/
							map.put(en.getKey(), r);
						}
					}
				}
			}

			IndexToMysql.playSuccRateToMysqlOffline(map, period, time);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}

	}

	/**
	 * 播放失败日志求出播放失败次数(实时)
	 */
	public static void playFailToRedis(Map<String, Long> hm) {
		Calendar hour = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(0));
		Calendar day = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(1));
		Calendar week = DateUtil.getBackdatedTime(Constant.PERIOD_ENUM.get(2));
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String pre_hour = df.format(hour.getTime());
		String pre_day = df.format(day.getTime());
		String pre_week = sdf.format(week.getTime());
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					String[] keys = TAB.split(en.getKey());
					String kpiUtcSec = keys[1];
					long fail_total = en.getValue();
					String fif_index_time = DateUtil.getQuarterOfHour(kpiUtcSec);
					String hour_index_time = kpiUtcSec.substring(0,10);
					Map<String,PlayFailLog>  map = (Map<String,PlayFailLog>)client.getObject(Constant.PLAY_FAIL + "#" + keys[0]);
					Map<String,PlayFailLog>  fif_map = (Map<String,PlayFailLog>)client.getObject(Constant.PLAY_FAIL + "#" + keys[0]+"#"+Constant.R15);
					Map<String,PlayFailLog>  hour_map = (Map<String,PlayFailLog>)client.getObject(Constant.PLAY_FAIL + "#" + keys[0]+"#"+Constant.R60);
					/**1分钟**/
					playFailCommen(keys[0],map,kpiUtcSec, fail_total,pre_hour);
					/**15分钟**/
					String fif_key = keys[0]+"#"+Constant.R15;
					playFailCommen(fif_key,fif_map,fif_index_time, fail_total,pre_day);
					/**1小时**/
					String hour_key = keys[0]+"#"+Constant.R60;
					playFailCommen(hour_key,hour_map,hour_index_time, fail_total,pre_week);
					}
					}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	/** 离线分析，自然播放率 *************************************************/
	public static void playCountToRedisOffline(HashMap<String, Long> hm, String time, String period) {
		HashMap<String, Double> mapMysql = new HashMap<String, Double>();
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					String mysqlKey = en.getKey() + "#" + time;
					double r = 0d;
					String delPrefix = en.getKey().substring(en.getKey().indexOf("#"));
					String newAllKey = Constant.OFFLINE_ALL_PLAY + delPrefix + "#" + time;
					logger.debug("redis中key=" + newAllKey + " 值：" + client.getObject(newAllKey));
					if (null != client.get(newAllKey)) {
						long value = Long.valueOf(client.getObject(newAllKey).toString());
						if (value != 0) {
							r = Double.parseDouble(df.format((double) en.getValue() / value));
							double rate = 1d;
							if(r > rate){
								r = 1;
							}
							logger.debug("离线的自然缓冲率Key=" + mysqlKey + "   值：" + r);
							mapMysql.put(mysqlKey + "#" + en.getValue(), r);
						} else {
							logger.debug("离线的自然缓冲率Key=" + mysqlKey + "   值0");
							mapMysql.put(mysqlKey + "#" + en.getValue(), 0d);
						}
					} else {
						logger.debug("离线的自然缓冲率Key=" + mysqlKey + "   值:0");
						mapMysql.put(mysqlKey + "#" + en.getValue(), 0d);
					}

				}
			}
			IndexToMysql.playCountToMysqlOffline(mapMysql, period);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}


	public static String getBaseInfo(String stb) {
		String baseIbfo = null;
		try {
			baseIbfo = client.get(stb);
		} catch (Exception e) {
			logger.error("从redis获取数据失败", e);
		}
		return baseIbfo;
	}

	/** 播放用户数入redis **/
	public static void playUserCountToRedisOnline(HashMap<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = Constant.ALL_USER +"#"+ en.getKey();
					List<TerminalState> list = (List<TerminalState>) client.getObject(key);
					if (null == list || list.size() == 0) {
						list = new ArrayList<TerminalState>();
					} else if (list.size() > 29) {
						list.remove(0);
					}
					TerminalState t = new TerminalState(time);
					t.setPlayUserCount(en.getValue());
					list.add(t);
					client.setObject(key, list);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	/** 开机用户数入redis **/
	public static void startCountToRedisOnline(HashMap<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = Constant.ALL_START +"#"+ en.getKey();
					List<TerminalState> list = (List<TerminalState>) client.getObject(key);
					if (null == list || list.size() == 0) {
						list = new ArrayList<TerminalState>();
					} else if (list.size() > 29) {
						list.remove(0);
					}
					TerminalState t = new TerminalState(time);
					t.setStartUserCount(en.getValue());
					list.add(t);
					client.setObject(key, list);
					
					
					//获取一个小时的数据
					String[] arr = en.getKey().split("\\#", -1);
					if (!"ALL".equals(arr[0]) && ("ALL".equals(arr[1]))) {
						String provinceId = arr[0];
						String newKey = Constant.ALL_START +"#"+provinceId+"#ALL#hour";
						List<TerminalState> list1 = (List<TerminalState>) client.getObject(newKey);
						TerminalState t1 = new TerminalState(time);
						if (null != list1 && list1.size() > 0) {
							TerminalState info = list1.get(list1.size()-1);
							if (!time.equals(info.getIndexTime())) {
								list1.clear();
							}
							t1.setStartUserCount(en.getValue());
							list1.add(t1);
							client.setObject(newKey, list1);
						}else{
							list1 = new ArrayList<TerminalState>();
							t1.setStartUserCount(en.getValue());
							list1.add(t1);
							client.setObject(newKey, list1);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	/** 故障各原因次数 
	 * @param period **/
	public static void faultToRedis(Map<String, Long> hm, String time, String period) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = Constant.LAG_CAUSE + period + "#"+ en.getKey();
					List<LagPhaseBehaviorLog> list = (List<LagPhaseBehaviorLog>) client.getObject(key);
					if (null == list || list.size() == 0) {
						list = new ArrayList<LagPhaseBehaviorLog>();
					} else if (list.size() > 29) {
						list.remove(0);
					}
					list.add(new LagPhaseBehaviorLog(time, en.getValue()));
					client.setObject(key, list);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void toRedisOnLineData(Map<String, Long> hm, String time, String period) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = Constant.PROGRAME_TYPE + "#" + en.getKey();
					List<PlayResponseLog> listP = (List<PlayResponseLog>) client.getObject(key);
					if (null == listP || listP.size() == 0) {
						listP = new ArrayList<PlayResponseLog>();
					} else if (listP.size() > 29) {
						listP.remove(0);
					}
					PlayResponseLog log = new PlayResponseLog();
					log.setIndexTime(time);
					log.setPlaySum(en.getValue());
					listP.add(log);
					client.setObject(key, listP);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void downBytesToRedis(Map<String, Long> hm, String time, String period) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = en.getKey();
					List<PlayResponseLog> listP = (List<PlayResponseLog>) client.getObject(key);
					if (null == listP || listP.size() == 0) {
						listP = new ArrayList<PlayResponseLog>();
					} else if (listP.size() > 29) {
						listP.remove(0);
					}
					PlayResponseLog p = new PlayResponseLog();
					p.setIndexTime(time);
					p.setDownBytesSUM(en.getValue());
					listP.add(p);
					client.setObject(key, listP);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void hmToRedis(Map<String, String> hm) {
		try {
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] keys = TAB.split(en.getKey());
					String kPIUTCSec = keys[1];
					String[] value = en.getValue().split("#",-1);
					String key = Constant.FIRST_FRAME + "#" + keys[0];
					Map<String, PlayRequestBean> redisCache = (Map<String, PlayRequestBean>) client.getObject(key);
					if (null != redisCache && redisCache.size() > 0) {
						PlayRequestBean res = redisCache.get(keys[1]);
						if(null == res){
							res = new PlayRequestBean();
							res.setIndexTime(kPIUTCSec);
							res.setLatency(Long.parseLong(value[1]));
							res.setLatencyCount(Long.parseLong(value[0]));
							res.setLatencyAVG(Long.parseLong(value[1])/Long.parseLong(value[0]));
							redisCache.put(kPIUTCSec, res);
						}else{
							long latency = Long.valueOf(res.getLatency() + value[1]);
							long latencyCount = Long.valueOf(res.getLatencyCount() + value[0]);
						    res.setLatency(latency);
						    res.setLatencyCount(latencyCount);
						    res.setLatencyAVG(latency/latencyCount);
							redisCache.put(kPIUTCSec, res);
						}
					}else{
						redisCache = new HashMap<String,PlayRequestBean>();
						PlayRequestBean res = new PlayRequestBean();
						res.setIndexTime(kPIUTCSec);
						res.setLatency(Long.parseLong(value[1]));
						res.setLatencyCount(Long.parseLong(value[0]));
						res.setLatencyAVG(Long.parseLong(value[1])/Long.parseLong(value[0]));
						redisCache.put(kPIUTCSec, res);
					}
					client.setObject(Constant.FIRST_FRAME + "#" + keys[0], redisCache);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void allPlayToRedisOffline(HashMap<String, Long> hm, String time,  int redisLoseTime) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String keyAll =Constant.OFFLINE_ALL_PLAY +"#"+en.getKey() + "#" + time;
					logger.debug("redis中总数key = " + keyAll + "   对应的值 =" + en.getValue());
					client.setObjectAndTime(keyAll, en.getValue(),redisLoseTime);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	public static void setAndExpireTime(String key, String value, int time) {
		try {
			client.setAndTime(key, value, time);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	public static String getByKey(String key){
		try {
			String value = client.get(key);
			if(null == value || "".equals(value)){
				return null;
			}
			return value;
		} catch (IOException e) {
			logger.error("redis初始化失败!", e);
			return null;
		}
	}
	
	public static long getLongByKey(String key){
		try {
			String value = client.get(key);
			if(null == value || "".equals(value)){
				return 0;
			}
			return Long.valueOf(value).longValue();
		} catch (IOException e) {
			logger.error("redis初始化失败!", e);
			return 0;
		}
	}
public static Object getObject(String key) {
		Object obj = null;
		try {
			obj = client.getObject(key);
		} catch (IOException e) {
			logger.error("redis初始化失败!", e);
		}
		return obj;
	}
	
	public static boolean setObject(String key, Object value) {
		boolean flag = true;
		try {
			client.setObject(key, value);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
			flag = false;
		}
		return flag;
	}

	public static void causeToRedis(Map<String,String> hm,String time){
		try {
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = en.getKey();
					List<LagCauseBean> listP = (List<LagCauseBean>) client.getObject(key);
					if (null == listP || listP.size() == 0) {
						listP = new ArrayList<LagCauseBean>();
					} else if (listP.size() > 29) {
						listP.remove(0);
					}
					String[] vArr = en.getValue().split("\\#",-1);
					LagCauseBean p = new LagCauseBean();
					p.setIndexTime(time);
					p.setCauseType1(Long.valueOf(vArr[0]));
					p.setCauseType2(Long.valueOf(vArr[1]));
					p.setCauseType3(Long.valueOf(vArr[2]));
					p.setCauseType4(Long.valueOf(vArr[3]));
					listP.add(p);
					client.setObject(key, listP);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
		
	}
	public static boolean set(String key, String value) {
		boolean flag = true;
		try {
			client.set(key, value);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
			flag = false;
		}
		return flag;
	}
	
	public static boolean saveInfoCount(List<InfoCountBean> list) {
		if (client == null) {
			logger.error("redis初始化失败!");
			return false;
		}
		long errCount = 0;
		for (InfoCountBean infoCount : list) {
			try {
				String key = InfoStrategy.LOG_ID + "#" + infoCount.getProvinceID() +
						"#" + infoCount.getCityID() + "#" + infoCount.getPlatform() +
						"#" + infoCount.getDeviceProvider() + "#" + infoCount.getFwVersion();
				client.setObject(key, infoCount);
			} catch (Exception e) {
				errCount++;
			}
		}
		return errCount == 0;
	}

	public static void lagPhaseMapToRedis(HashMap<String, String> lagPhaseMap, String time, Integer redisLoseTime) {
		try {
			for (Entry<String, String> en : lagPhaseMap.entrySet()) {
				if (en != null && en.getValue() != null) {
					String pKey = en.getKey();
					String key = pKey + "#json";
					List<String> list = (List<String>) client.getObject(key);
					if (null == list || list.size() == 0) {
						list = new ArrayList<String>();
					} else if (list.size() > 29) {
						list.remove(0);
					}
					String[] sArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					String newFaultKey = Constant.FAULT_USER  + "#" + sArr[1]  + "#" + sArr[2];
					List<LagPhaseBehaviorLog> listFault = (List<LagPhaseBehaviorLog>) client.getObject(newFaultKey);
					if (null == listFault || listFault.size() == 0) {
						listFault = new ArrayList<LagPhaseBehaviorLog>();
					} else if (listFault.size() > 29) {
						listFault.remove(0);
					}
					LagPhaseBehaviorLog lagFault = new LagPhaseBehaviorLog(time);
					LagPhaseRateBean p = new LagPhaseRateBean();
					
					Long seriousCount = Long.valueOf(vArr[3]);
					Long moreCount = Long.valueOf(vArr[2]);
					Long occasionallyCount = Long.valueOf(vArr[1]);
					Long normalCount = Long.valueOf(vArr[0]);
					Long userCount = Long.valueOf(vArr[4]);
					
					p.setProvinceID(sArr[1]);
					p.setCityID(sArr[2]);
//					p.setFwVersion(sArr[3]);
//					p.setDeviceProvider(sArr[4]);
//					p.setFwVersion(sArr[5]);
					p.setSeriousCount(seriousCount);
					p.setMoreCount(moreCount);
					p.setOccasionallyCount(occasionallyCount);
					p.setNormalCount(normalCount);
					p.setUserCount(userCount);
					p.setHealthStatus( 1-(seriousCount/(double)userCount)-(moreCount/(double)userCount/3)-(occasionallyCount/(double)userCount/10));
					p.setIndexTime(time);
					JSONObject json = new JSONObject();
					json.put("normalCount", normalCount);
					json.put("occasionallyCount", occasionallyCount);
					json.put("moreCount", moreCount);
					json.put("seriousCount", seriousCount);
					json.put("indexTime", time);
					list.add(json.toString());
					lagFault.setFaultUser(seriousCount + moreCount);
					lagFault.setIndexTime(time);
					listFault.add(lagFault);
					client.setObject(newFaultKey, listFault);
					client.setObjectAndTime(pKey, p ,redisLoseTime);
					client.setObject(key, list);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void streamUserCountMap(Map<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = en.getKey();
					List<TerminalState> listP = (List<TerminalState>) client.getObject(key);
					if (null == listP || listP.size() == 0) {
						listP = new ArrayList<TerminalState>();
					} else if (listP.size() > 29) {
						listP.remove(0);
					}
					TerminalState p = new TerminalState();
					p.setIndexTime(time);
					p.setPlayUserCount(en.getValue());
					listP.add(p);
					client.setObject(key, listP);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
		
	}
	
	public static void playCountMapToRedis(Map<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = en.getKey();
					List<TerminalState> listP = (List<TerminalState>) client.getObject(key);
					if (null == listP || listP.size() == 0) {
						listP = new ArrayList<TerminalState>();
					} else if (listP.size() > 29) {
						listP.remove(0);
					}
					TerminalState p = new TerminalState();
					p.setIndexTime(time);
					p.setPlayCountCount(en.getValue());
					listP.add(p);
					client.setObject(key, listP);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
		
	}
	
	public static void errorCodeRedisOnLong(Map<String, Long> hm, String time) {
		try {
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String key = en.getKey();
					List<HttpErrorCodeLog> listP = null;
					String listJSON = (String)client.getObject(key);
					if(listJSON == null || "".equals(listJSON)){
						listP = new ArrayList<HttpErrorCodeLog>();
					}else{
						listP = JSON.parseArray(listJSON, HttpErrorCodeLog.class);
						if (null == listP || listP.size() == 0) {
							listP = new ArrayList<HttpErrorCodeLog>();
						} else if (listP.size() > 29) {
							listP.remove(0);
						}
					}
					HttpErrorCodeLog p = new HttpErrorCodeLog();
					p.setIndexTime(time);
					p.setTotal(en.getValue());
					listP.add(p);
					listJSON = JSON.toJSONString(listP);
					client.setObject(key, listJSON);
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	private final static String UNKNOW_PROV = "2";
//	private final static Set<String> UNKNOW_CITY = new HashSet<String>() {
//		private static final long serialVersionUID = 1L;
//		{
//			add("34");
//			add("53");
//			add("73");
//			add("92");
//			add("133");
//			add("145");
//			add("157");
//			add("170");
//			add("185");
//			add("195");
//			add("209");
//			add("223");
//			add("235");
//			add("251");
//			add("261");
//			add("273");
//			add("291");
//			add("309");
//			add("326");
//			add("341");
//			add("363");
//			add("378");
//			add("382");
//			add("404");
//			add("414");
//			add("431");
//			add("439");
//			add("450");
//			add("465");
//			add("474");
//			add("480");
//		}
//	};
	
	/**
	 * 获取机顶盒区域信息
	 * @param id 目前为盒子的probeID，provinceId 待检测省，cityId待检测市
	 * @return [省, 市]
	 */
	public static String[] getStbArea(String id, String provinceId) {
//		if (!UNKNOW_PROV.equals(provinceId) && !UNKNOW_CITY.contains(cityId)) {
		if (!UNKNOW_PROV.equals(provinceId)) {
			return null;
		}
		String[] area = null;
		try {
			String stbInfo = client.get(id);
			if (stbInfo != null && stbInfo.length() > 0) {
				String[] infoArr = stbInfo.split(String.valueOf((char) 0x7F), -1);
				area = new String[]{infoArr[5], infoArr[6]};
			}
		} catch (Exception e) {
			logger.error("从redis获取数据失败", e);
		}
		return area;
	}
	
	public static void playFailCommen(String key,Map<String, PlayFailLog> map,String kpiUtcSec,long fail_total,String time_limit){
		if (null != map && map.size() > 0) {
			PlayFailLog eb = map.get(kpiUtcSec);
			if(null == eb){
				eb = new PlayFailLog();
				eb.setIndexTime(kpiUtcSec);
			}
				eb.setFailCount(eb.getFailCount()+fail_total);
				map.put(kpiUtcSec,eb);
		}else{
			map = new HashMap<String,PlayFailLog>();
			PlayFailLog eb = new PlayFailLog();
			eb.setIndexTime(kpiUtcSec);
			eb.setFailCount(fail_total);
			map.put(kpiUtcSec,eb);
		}
		delMapFail(map,time_limit);
		client.setObject(Constant.PLAY_FAIL + "#" + key, map);
	}
	
	public static void eventCommen(String key,Map<String, EventTrendBean> map,String kpiUtcSec,int eventID,long value,String time_limit){
		if (null != map && map.size() > 0) {
			EventTrendBean eb = map.get(kpiUtcSec);
			if(null == eb){
				eb = new EventTrendBean();
				eb.setIndexTime(kpiUtcSec);
			}
				setEventBean(eventID,eb,value);
				map.put(kpiUtcSec,eb);
		}else{
			map = new HashMap<String,EventTrendBean>();
			EventTrendBean eb = new EventTrendBean();
			eb.setIndexTime(kpiUtcSec);
			setEventBean(eventID,eb,value);
			map.put(kpiUtcSec,eb);
		}
		delMap(map,time_limit);
		client.setObject(Constant.EVENT_TREND + "#" + key, map);
	}
	
	public static void imageCommen(String key,Map<String, ImageLoadBean> map,String kpiUtcSec,long pageDurSucCnt,long pageDurLE3sCnt,long requests_total,String time_limit){
		if (null != map && map.size() > 0) {
			ImageLoadBean ib = map.get(kpiUtcSec);
			if (null == ib) {
				if (requests_total != 0) {
					ib = new ImageLoadBean();
					Double value = Double.valueOf(df.format((pageDurSucCnt * 1.0) / requests_total));
					Double valueSucc = Double.valueOf(df.format((pageDurLE3sCnt * 1.0) / requests_total));
					ib.setIndexTime(kpiUtcSec);
					ib.setSuccrate(value);
					ib.setFastrate(valueSucc);
					map.put(kpiUtcSec, ib);
				}
			} else {
				long pcnt = pageDurSucCnt + ib.getPageDurSucCnt();
				long pscnt = pageDurLE3sCnt + ib.getPageDurLE3sCnt();
				long total = requests_total + ib.getRequests();
				if (total != 0) {
					Double value = Double.valueOf(df.format((pcnt * 1.0) / total));
					Double valueSucc = Double.valueOf(df.format((pscnt * 1.0) / total));
					ib.setIndexTime(kpiUtcSec);
					ib.setSuccrate(value);
					ib.setFastrate(valueSucc);
					map.put(kpiUtcSec, ib);
				}
			}
		} else {
			map = new HashMap<String, ImageLoadBean>();
			if (requests_total != 0) {
				Double value = Double.valueOf(df.format((pageDurSucCnt * 1.0) / requests_total));
				Double valueSucc = Double.valueOf(df.format((pageDurLE3sCnt * 1.0) / requests_total));
				ImageLoadBean ib = new ImageLoadBean();
				ib.setIndexTime(kpiUtcSec);
				ib.setSuccrate(value);
				ib.setFastrate(valueSucc);
				map.put(kpiUtcSec, ib);
			}
		}
		delMapImage(map,time_limit);
		client.setObject(Constant.KEY_PREFIX_IMAGE + "#" + key, map);
	}
	
	public static void delMapImage(Map<String,ImageLoadBean> map,String time){
		Iterator<Map.Entry<String,ImageLoadBean>> it = map.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,ImageLoadBean> entry = it.next();  
			if(entry.getKey().compareTo(time)<0){
				it.remove();
			}
		}
	}
	
	public static void delMap(Map<String,EventTrendBean> map,String time){
		Iterator<Map.Entry<String,EventTrendBean>> it = map.entrySet().iterator();
		while(it.hasNext()){
			 Map.Entry<String,EventTrendBean> entry = it.next();  
			if(entry.getKey().compareTo(time)<0){
				it.remove();
			}
		}
	}
	
	public static void delMapFail(Map<String,PlayFailLog> map,String time){
		Iterator<Map.Entry<String,PlayFailLog>> it = map.entrySet().iterator();
		while(it.hasNext()){
			 Map.Entry<String,PlayFailLog> entry = it.next();  
			if(entry.getKey().compareTo(time)<0){
				it.remove();
			}
		}
	}	
	
	public static void setEventBean(int eventID,EventTrendBean eb,long value){
		switch (eventID) {
		case 1:
			eb.setEvent1(value);
			eb.setCdn(eb.getCdn()+value);
			break;
		case 2:
			eb.setEvent2(value);
			eb.setOperator(eb.getOperator() + value);
			break;
		case 3:
			eb.setEvent3(value);
			eb.setStb(eb.getStb() +value);
			break;
		case 4:
			eb.setEvent4(value);
			eb.setOperator(eb.getOperator() + value);
			break;
		case 6:
			eb.setEvent6(value);
			eb.setCdn(eb.getCdn()+value);
			break;
		case 7:
			eb.setEvent7(value);
			eb.setOperator(eb.getOperator() + value);
			break;
		case 8:
			eb.setEvent8(value);
			eb.setCdn(eb.getCdn()+value);
			break;
		case 10:
			eb.setEvent10(value);
			eb.setOperator(eb.getOperator() + value);
			break;
		case 11:
			eb.setEvent11(value);
			eb.setFamily(eb.getCdn()+value);
			break;
		case 12:
			eb.setEvent12(value);
			eb.setFamily(eb.getCdn()+value);
			break;
		case 13:
			eb.setEvent13(value);
			eb.setStb(eb.getStb() + value);
			break;
		case 14:
			eb.setEvent14(value);
			eb.setStb(eb.getStb() + value);
			break;
		case 15:
			eb.setEvent15(value);
			eb.setStb(eb.getStb() + value);
			break;
		case 16:
			eb.setEvent16(value);
			eb.setCdn(eb.getCdn()+value);
			break;
		}
	}
}
