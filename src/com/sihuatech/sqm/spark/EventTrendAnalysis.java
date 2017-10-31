package com.sihuatech.sqm.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.sihuatech.sqm.spark.bean.EventTrendBean;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 事件趋势 
 * 1.终端问题(3、TCP低窗口包;13、分片未下载部分过大告警;14、一个tcp流多次请求相同的分片;15、未及时关闭连接)
 * 2.运营商网络问题(2、TCP建立时间(us)超长告警;4、TCP重传率过大;7、网络中断告警;10、TCP 乱序率过大;
 *            153、DNS响应过长告警;154、DNS错误响应码;155、DNS未响应率)
 * 3.CDN平台事件(1、分片HTTP响应时延(us)超长告警;6、EPG菜单HTTP响应时延(us)超长告警;8、HLS直播TS列表长时间不更新;
 *            9、OTT业务的HTTP请求无响应;16、m3u8序号回跳;151、EPG图片下载过长告警 )
 * 4.家庭网络事件(11、WIFI强度低;12、网关响应超长告警)
 */
public class EventTrendAnalysis {
	private static Logger logger = Logger.getLogger(EventTrendAnalysis.class);
	private static HashMap<String, Long> eventTrendMap = new HashMap<String, Long>();
	private final static int DEFAULT_NUM_PARTITIONS = 100;
	private final static long DEFAULT_BATCH_DURATION = 1;
	private static int numPartitions = 0;
	private static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
	
	private static String[] dimensions = { "provinceID","cityID","platform","deviceProvider","fwVersion"};

	public static void main(String[] args) {
		if (null == args || args.length < 2) {
			System.err.println("Usage: EventTrendAnalysis <topics> <numPartitions>\n" +
			          "  <topics> is a list of one or more kafka topics to consume from\n"
			          + " <numPartitions> 重新设置Rdd的分区数量\n\n");
			return;
		}
	   // 获取参数
		String topics = args[0];
		String _numPartitions = args[1];
		String brokers = PropHelper.getProperty("broker.quorum");;
		String _batchDuration = PropHelper.getProperty("EVENT_TREND");
		logger.info("parameters...\n"
				+ "brokers:" + brokers + "\n"
				+ "topics:" + topics + "\n"
				+ "batchDuration:" + _batchDuration + "\n"
				+ "numPartitions:" + _numPartitions + "\n");
		// 转换参数提供使用
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		long batchDuration = NumberUtils.toLong(_batchDuration, DEFAULT_BATCH_DURATION);
		numPartitions = NumberUtils.toInt(_numPartitions, DEFAULT_NUM_PARTITIONS);
		// spark任务初始化
		SparkConf sparkConf = new SparkConf().setAppName("EventTrendAnalysis");
		sparkConf.setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.minutes(batchDuration));
		logger.info("开始读取内容：");
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicsSet
		);
		
		// 获取kafka的offset
		JavaPairDStream<String, String> tmpMessages = messages.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		});
		
		JavaDStream<String> lines = tmpMessages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		// 校验日志，过滤不符合条件的记录
		JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(String line) throws Exception {
				logger.info("+++[Event]日志过滤开始");
				String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
				if (lineArr.length < 16) {
					return false;
				} else if (!"11".equals(lineArr[0])) {
					return false;
				} else if (StringUtils.isBlank(lineArr[1].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[2].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[5].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[6].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[8].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[10].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[13].trim())) {
					return false;
				}
				return true;
			}
		});
		filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
			private static final long serialVersionUID = 1L;
			public void call(JavaRDD<String> rdd, Time time) {
				logger.info("RDD分区数：" + rdd.getNumPartitions());
				logger.info("+++[Event]日志验证结束，抽象JavaBean开始");
				SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
				JavaRDD<EventTrendBean> rowRDD = rdd.map(new Function<String, EventTrendBean>() {
					private static final long serialVersionUID = 1L;
					public EventTrendBean call(String line) {
						String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
						String kpiUtcSec = lineArr[8].trim().substring(0, 12);
						EventTrendBean event = new EventTrendBean();
						event.setProbeID(lineArr[2].trim());
						event.setDeviceProvider(lineArr[3].trim());
						event.setPlatform(lineArr[4].trim());
						event.setProvinceID(lineArr[5].trim());
						event.setCityID(lineArr[6].trim());
						event.setFwVersion(lineArr[7].trim());
						event.setKpiUtcSec(kpiUtcSec);
						event.setEventID(lineArr[10].trim());
						if(StringUtils.isBlank(lineArr[13])){
							event.setCount(0);
						}else{
							event.setCount(Long.valueOf(lineArr[13]));
						}
						return event;
					}
				});
				logger.debug("+++[EventTrend]抽象JavaBean结束，过滤NullObject开始");
				// 过滤对象为空
				JavaRDD<EventTrendBean> rowRDD2 = rowRDD.filter(new Function<EventTrendBean, Boolean>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(EventTrendBean eventTrendBean) throws Exception {
						if (null == eventTrendBean) {
							return false;
						}
						return true;
					}
				});
				int tmpNumPartitions = rowRDD2.getNumPartitions();
				logger.info("合并前分区数：" + tmpNumPartitions);
				JavaRDD<EventTrendBean> resRDD = null;
				if (tmpNumPartitions < numPartitions) {
					resRDD = rowRDD2.repartition(numPartitions);
				} else if (tmpNumPartitions > numPartitions){
					resRDD = rowRDD2.coalesce(numPartitions);
				} else {
					resRDD = rowRDD2;
				}
				logger.info("合并后分区数：" + resRDD.getNumPartitions());
				String periodTime = DateUtil.getRTPeriodTime();
				DataFrame wordsDataFrame = sqlContext.createDataFrame(resRDD, EventTrendBean.class);
				wordsDataFrame.registerTempTable("EventTrend");
				long total = wordsDataFrame.count();// 计算单位时间段内记录总数
				logger.info("+++[EventTrend]事件趋势日志记录数：" + total);
				if (total > 0) {
					GroupedData data = wordsDataFrame.cube(dimensions[0], dimensions[1],dimensions[2],dimensions[3],dimensions[4],"eventID","kpiUtcSec");
					Row[] stateRow = data.sum("count").filter("eventID is not null").filter("kpiUtcSec is not null").collect();
					if (stateRow == null || stateRow.length == 0) {
						return;
					} else {
						for (Row row : stateRow) {
							if(row == null || row.size() == 0){
								continue;
							}
							String  pID = row.getString(0);
							String  cID = row.getString(1);
							String  pf = row.getString(2);
							String  dp = row.getString(3);
							String  fv = row.getString(4);
							if(null == pID){
								pID  = "ALL";
							}
							if(null == cID){
								cID  = "ALL";
							}
							if(null == pf){
								pf  = "ALL";
							}
							if(null == dp){
								dp  = "ALL";
							}
							if(null == fv){
								fv  = "ALL";
							}
							String key = pID + "#"+ cID + "#" + pf +"#"+ dp + "#" + fv + "\t"  + row.getString(5)+ "\t" + row.getString(6);
							//logger.info("+++++++++++++++++++++++++++++++"+key+"|"+row.getLong(7));
							eventTrendMap.put(key, row.getLong(7));
						}
					}
					if (eventTrendMap.size() > 0) {
						eventTrendSave(periodTime);
						eventTrendMap.clear();
					}
				}
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}

	public static void sparkEventTrendByDimension(SQLContext sqlContext) {
		String EVENTID = PropHelper.getProperty("EVENTID");
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
			String sql = "select sum(cnt)," + selectSb.substring(0, selectSb.length() - 1)+ ",eventID,kpiUtcSec from cacheEvent " ;
			if(StringUtils.isNotBlank(EVENTID)){
				sql += "where eventID in ("+EVENTID+") group by eventID,kpiUtcSec";
			}else{
				sql += " group by eventID,kpiUtcSec ";
			}
			if (StringUtils.isNotBlank(groupSb.toString())) {
				sql += ","+groupSb.substring(0, groupSb.length()-1);
			} 
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			Row[] stateRow = allDimensionToTimes.collect();
			if (stateRow == null || stateRow.length == 0) {
				return;
			} else {
				for (Row row : stateRow) {
					if(row == null || row.size() == 0){
						continue;
					}
					String key = row.getString(1) + "#" + row.getString(2) +"#"+row.getString(3) + "#" + row.getString(4)+ "#" + row.getString(5)+ "\t" + row.getString(6)+"\t"+row.getString(7);
					eventTrendMap.put(key, row.getLong(0));
				}
			}
		}
	}

	public static void eventTrendSave(String time) {
		logger.info("事件趋势保存分析数据到存储系统开始");
		logger.info("保存到Redis");
		IndexToRedis.eventTrendToRedis(eventTrendMap, time);
		logger.info("事件趋势保存分析数据到存储系统结束");
	}

}
