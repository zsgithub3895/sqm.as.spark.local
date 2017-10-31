package com.sihuatech.sqm.spark.offline;
/**
 * 事件趋势 
 * 1.终端问题(3、TCP低窗口包;13、分片未下载部分过大告警;14、一个tcp流多次请求相同的分片;15、未及时关闭连接)
 * 2.运营商网络问题(2、TCP建立时间(us)超长告警;4、TCP重传率过大;7、网络中断告警;10、TCP 乱序率过大;
 *            153、DNS响应过长告警;154、DNS错误响应码;155、DNS未响应率)
 * 3.CDN平台事件(1、分片HTTP响应时延(us)超长告警;6、EPG菜单HTTP响应时延(us)超长告警;8、HLS直播TS列表长时间不更新;
 *            9、OTT业务的HTTP请求无响应;16、m3u8序号回跳;151、EPG图片下载过长告警 )
 * 4.家庭网络事件(11、WIFI强度低;12、网关响应超长告警)
 */
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.EventTrendBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

public class EventTrendOffineAnalysis {

	private static Logger logger = Logger.getLogger(EventTrendOffineAnalysis.class);
	static DecimalFormat formatFour = new DecimalFormat("0.0000");
	private static Map<String, Long> eventTrendMap = new HashMap<String, Long>();
	private static String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion","eventID"}; 
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务编号*/
	private static String TASKID = "";
	

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: EventTrendOffineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.EVENT_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASKID = args[2];
		logger.info(String.format("图片加载指标离线分析执行参数 : \n period : %s \n path : %s \n task : %s", PERIOD, PATH,TASKID));
		if (! Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: EventTrendOffineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.EVENT_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.EVENT_TASK_ENUM.contains(TASKID)){
			System.err.println("Usage: EventTrendOffineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.EVENT_TASK_ENUM);
			System.exit(1);
		}
		
		if(StringUtils.isNotBlank(TASKID) && Constant.EVENT_ALL_TASK.equals(TASKID)){
			logger.info("--------------本次执行全部任务----------------");
		}
		int index = PATH.lastIndexOf("/");
		Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1),PERIOD,c);
		filePath = filePath + "*";

		SparkConf conf = new SparkConf().setAppName("EventTrendOffineAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		logger.info("文件路径：" + filePath);
		// 读取HDFS符合条件的文件
		JavaPairRDD<BytesWritable, BytesWritable> slines = sc.sequenceFile(filePath, BytesWritable.class,
				BytesWritable.class);
		JavaRDD<String> lines = slines.values().map(new Function<BytesWritable, String>() {
			@Override
			public String call(BytesWritable value) throws Exception {
				return new String(value.getBytes());
			}
		});
		// 过滤不符合规范的记录
		JavaRDD<String> filterLines = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String line) throws Exception {
				logger.info("+++[Event]日志过滤开始");
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if(lineArr.length < 16){
					return false;
				}else if(!"11".equals(lineArr[0])){
					return false;
				}else if (StringUtils.isBlank(lineArr[1].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[2].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[3].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[4].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[5].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[6].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[7].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[8].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[9].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[10].trim())) {
					return false;
				}else if (StringUtils.isBlank(lineArr[11].trim())) {
					return false;
				}
				return true;
			}
		});
		logger.info("+++[Lag]日志验证结束，抽象JavaBean开始");
		JavaRDD<EventTrendBean> rowRDD = filterLines.map(new Function<String, EventTrendBean>() {
			public EventTrendBean call(String line) {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				EventTrendBean event = new EventTrendBean();
				event.setProbeID(lineArr[2].trim());
				event.setDeviceProvider(lineArr[3].trim());
				event.setPlatform(lineArr[4].trim());
				event.setProvinceID(lineArr[5].trim());
				event.setCityID(lineArr[6].trim());
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(event.getProbeID(), event.getProvinceID());
				if (area != null) {
					event.setProvinceID(area[0]);
					event.setCityID(area[1]);
				}
				event.setFwVersion(lineArr[7].trim());
				event.setEventID(lineArr[10].trim());
				event.setCount(Long.valueOf(lineArr[13]));
				return event;
			 }
		});
		logger.debug("+++[eventTrend]抽象JavaBean结束，过滤NullObject开始");
		// 过滤对象为空
		JavaRDD<EventTrendBean> rowRDD2 = rowRDD.filter(new Function<EventTrendBean, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(EventTrendBean e) throws Exception {
					if (null == e) {
						return false;
					}
					return true;
				}
		 });
		// 注册临时表
		DataFrame eventDataFrame = sqlContext.createDataFrame(rowRDD2, EventTrendBean.class);
		eventDataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER());
		eventDataFrame.registerTempTable("eventTrend");
		
		long total = eventDataFrame.count();
		logger.info("+++[eventTrend]事件趋势日志记录数：" + total);
		if (total > 0) {
			eventAnalysis(sqlContext, c);
		}
		eventDataFrame.unpersist();
	}
	
	public static void eventAnalysis(SQLContext sqlContext,Calendar c){
		if(Constant.EVENT_ALL_TASK.equalsIgnoreCase(TASKID)){
			logger.info("开始计算事件趋势");
			eventSparkAnalysis(sqlContext);
			eventSave(c);
		}
	}
	
	public static void eventSparkAnalysis(SQLContext sqlContext) {
		String[] EVENTID = PropHelper.getProperty("EVENTID").split(",");
		String sqlEnd = "where eventID = ";
	    for (int i = 0; i < EVENTID.length - 1; i++) {
	    	sqlEnd += EVENTID[i] + " or eventID = ";
	    }
	    sqlEnd += EVENTID[EVENTID.length - 1];
		int num = 0;
		while (num < Math.pow(2, dimensions.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			for (int i = 0; i < dimensions.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				if (bit == 1) {
					selectSb.append(dimensions[i]).append(",");
					groupSb.append(dimensions[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT sum(count),").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM eventTrend ").append(sqlEnd) ;
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sqlSb.toString());
			Row[] rows = allDimensionToTimes.collect();
			if (rows == null || rows.length == 0) {
				return;
			} else {
				for (Row row : rows) {
					String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3) + "#"
							+ row.getString(4) + "#" + row.getString(5) + "#" + row.getString(6);
					eventTrendMap.put(key, Long.valueOf(row.getLong(0)));
				}
			}
		}
	}
	
	public static void eventSave(Calendar c){
		logger.info("事件趋势存储开始mapSize=" + eventTrendMap.size());
		if (eventTrendMap != null && eventTrendMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.eventTrendToMysql(eventTrendMap, time,PERIOD);
			eventTrendMap.clear();
		}
	}
	
}
