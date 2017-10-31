package com.sihuatech.sqm.spark;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.EPGResponseBean;
import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class LagphaseAnalasis {
	private static Logger logger = Logger.getLogger(LagphaseAnalasis.class);
	private static HashMap<String, HashMap<String, LagPhaseBehaviorLog>> lagCacheMap = new HashMap<String, HashMap<String,LagPhaseBehaviorLog>>();
	private static HashMap<String, Long> playCount = new HashMap<String, Long>();
	private static HashMap<String, Long> faultMap = new HashMap<String, Long>();
//	private static String[] dimension = { "provinceID", "platform", "deviceProvider", "fwVersion"};
	private static String[] dimension = {"provinceID","cityID","platform", "deviceProvider", "fwVersion"};;

	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 3) {
			System.err.println("Usage: LagphaseAnalasis <group> <topics> <numThreads>");
			System.exit(1);
		}
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		logger.debug("+++[Lag]parameters...\n"
				+ "group:" + args[0] + "\n"
				+ "topics:" + args[1] + "\n"
				+ "numThreads:" + args[2] + "\n"
				+ "zkQuorum:" + zkQuorum + "\n");
		SparkConf sparkConf = new SparkConf().setAppName("LagphaseAnalasis");
		String batchDuration = PropHelper.getProperty("LAG_PHASE_TIME");
		logger.debug("+++[Lag]parameters...\n"
				+ "batchDuration:" + batchDuration + "\n");
		if (null != batchDuration && !"".equals(batchDuration)) {
			JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
					Durations.minutes(Long.valueOf(batchDuration)));

			// 此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[2]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			String[] topics = args[1].split(",");
			for (String topic : topics) {
				topicMap.put(topic, numThreads);
			}
			logger.debug("+++[Lag]创建Kafka Input DStream");
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, args[0],
					topicMap);
			logger.debug("+++[Lag]接收数据");
			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
			logger.debug("+++[Lag]日志验证开始");
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
					if (lineArr.length < 16) {
						return false;
					} else if (!"4".equals(lineArr[0])) {
						return false;
					}
					return true;
				}
			});
			
			// 提取字段转为Bean
			JavaDStream<LagPhaseBehaviorLog> objs = filterLines.map(new Function<String, LagPhaseBehaviorLog>() {
				private static final long serialVersionUID = 1L;
				public LagPhaseBehaviorLog call(String line) {
					LagPhaseBehaviorLog lag = null;
					if (StringUtils.isNotBlank(line)) {
						String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
						lag = new LagPhaseBehaviorLog(
								Integer.valueOf(lineArr[0]), lineArr[1], lineArr[2],
								lineArr[3], lineArr[4], lineArr[5], lineArr[6],
								lineArr[7],lineArr[8], lineArr[10]);
					}
					return lag;
				}
			});
			
			JavaDStream<LagPhaseBehaviorLog> filterObjs = objs.filter(new Function<LagPhaseBehaviorLog, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(LagPhaseBehaviorLog log) throws Exception {
					if (null == log) {
						return false;
					}
					return true;
				}
			});
			
			filterObjs.foreachRDD(new VoidFunction2<JavaRDD<LagPhaseBehaviorLog>, Time>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void call(JavaRDD<LagPhaseBehaviorLog> rdd, Time time) {
					Map<String,String> analasisFor1R = new HashMap<String,String>();
					Map<String,String>  analasisFor15R = new HashMap<String,String>();
					Map<String,String>  analasisFor60R = new HashMap<String,String>();
					/*SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");*/
					List<LagPhaseBehaviorLog> lags = rdd.collect();
					for(LagPhaseBehaviorLog lag : lags){
						if(null == lagCacheMap){
							lagCacheMap = new HashMap<String, HashMap<String,LagPhaseBehaviorLog>>();
						}
						//时间点kPIUTCSec去除最后两位
						String kpiTime = lag.getkPIUTCSec();
						//hasID_probeID
						String timeMapKey = lag.getHasID() + "_" + lag.getProbeID();
					/*	//延时时间(相比当前时间)
						Long delayTime = ((new Date()).getTime() - df.parse(kpiTime).getTime())/60000;*/
						HashMap<String,LagPhaseBehaviorLog> timeMap =  lagCacheMap.get(kpiTime);
						if(null == timeMap){
							timeMap = new HashMap<String,LagPhaseBehaviorLog>();
						}
						//对Map进行更新
						if(null == timeMap.get(timeMapKey)){
							timeMap.put(timeMapKey, lag);
							lagCacheMap.put(kpiTime, timeMap);
							analasisFor1R.put(kpiTime,kpiTime);
							analasisFor15R.put(DateUtil.getQuarterOfHour(kpiTime),DateUtil.getQuarterOfHour(kpiTime));
							analasisFor60R.put(kpiTime.substring(0,kpiTime.length()-2)+"00",kpiTime.substring(0,kpiTime.length()-2)+"00");
						}
					}
					//处理1分钟计算
					if(null != analasisFor1R && analasisFor1R.size()>0){
						Iterator<Map.Entry<String,String>> it = analasisFor1R.entrySet().iterator();
						while(it.hasNext()){
							Map.Entry<String,String> entry = it.next();  
							String kpiTime = entry.getKey();
							JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
							JavaRDD<LagPhaseBehaviorLog> rdd_new = jsc.parallelize(new ArrayList<LagPhaseBehaviorLog>(lagCacheMap.get(kpiTime).values()));
							analasis(rdd,rdd_new,kpiTime,"");
						}
					}
					//处理15分钟计算
					if(null != analasisFor15R && analasisFor15R.size()>0){
						Iterator<Map.Entry<String,String>> it = analasisFor15R.entrySet().iterator();
						while(it.hasNext()){
							Map.Entry<String,String> entry = it.next();  
							String kpiTime = entry.getKey();
							JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
							HashMap<String,LagPhaseBehaviorLog> mapAll = new HashMap<String,LagPhaseBehaviorLog>();
							for(int i=0;i<15;i++){
								if(null != lagCacheMap.get(kpiTime+i)){
									mapAll.putAll(lagCacheMap.get(kpiTime+i));
								}
							}
							JavaRDD<LagPhaseBehaviorLog> rdd_new = jsc.parallelize(new ArrayList<LagPhaseBehaviorLog>(mapAll.values()));
							analasis(rdd,rdd_new,kpiTime,"15R");
						}
					}
					//处理60分钟计算
					if(null != analasisFor60R && analasisFor60R.size()>0){
						Iterator<Map.Entry<String,String>> it = analasisFor60R.entrySet().iterator();
						while(it.hasNext()){
							Map.Entry<String,String> entry = it.next();  
							String kpiTime = entry.getKey();
							JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
							HashMap<String,LagPhaseBehaviorLog> mapAll = new HashMap<String,LagPhaseBehaviorLog>();
							for(int i=0;i<60;i++){
								if(null != lagCacheMap.get(kpiTime+i)){
									mapAll.putAll(lagCacheMap.get(kpiTime+i));
								}
							}
							JavaRDD<LagPhaseBehaviorLog> rdd_new = jsc.parallelize(new ArrayList<LagPhaseBehaviorLog>(mapAll.values()));
							analasis(rdd,rdd_new,kpiTime,"60R");
						}
					}
				}
			});
			jssc.start();
			jssc.awaitTermination();
		} else {
			logger.info("请配置每隔多长时间取卡顿行为日志！！");
		}
	}
	
	protected static void analasis(JavaRDD<LagPhaseBehaviorLog> rdd, JavaRDD<LagPhaseBehaviorLog> rdd_new,String periodTime,String period) {
		SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
		DataFrame dataFrame = sqlContext.createDataFrame(rdd_new, LagPhaseBehaviorLog.class);
		dataFrame.registerTempTable("LagPhaseLog");
		dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER());
		long count = dataFrame.count();
		logger.info("+++[Lag]卡顿行为日志记录数" + count);
		if(count > 0){
			//总次数
			logger.debug("+++[Lag]分析开始");
			lagPhaseDimension(sqlContext);
			logger.debug("+++[Lag]分析结束，分析结果记录数如下...\n");
			lagPhaseSave(periodTime,period);
			//故障各原因次数
			lagPhaseFaultDimension(sqlContext);
			lagPhaseFaultSave(periodTime,period);
		}
		dataFrame.unpersist();
	}

	public static void lagPhaseDimension(SQLContext sqlContext){
		int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			String sql = null;
			if(StringUtils.isBlank(groupSb.toString())){
				sql="select count(distinct hasID),"+selectSb.substring(0,selectSb.length()-1)+" from LagPhaseLog ";
			}else{
				sql="select count(distinct hasID),"+selectSb.substring(0,selectSb.length()-1)+" from LagPhaseLog  group by "+groupSb.substring(0,groupSb.length()-1);
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			Row[] stateRow = allDimensionToTimes.collect();
			lagPhaseMap(stateRow);
		}
	}
	
	public static void lagPhaseMap(Row[] rowRate){
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5);
				playCount.put(key, row.getLong(0));
			}
		}
	}
	
	private static void lagPhaseSave(String time, String period) {
		logger.info("卡顿播放次数分析结束，map的大小=" + playCount.size());
		if (playCount.size() > 0) {
			/** 卡顿播放次数入Redis**/
			IndexToRedis.playCountToRedis(playCount, time,period);
			playCount.clear();
		}
	}
	
	public static void lagPhaseFaultDimension(SQLContext sqlContext){
		int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			String sql = null;
			if(StringUtils.isBlank(groupSb.toString())){
				sql="select count(1),"+selectSb.toString()+" exportId from LagPhaseLog group by exportId";
			}else{
				sql="select count(1),"+selectSb.toString()+" exportId from LagPhaseLog  group by "+groupSb.toString()+" exportId";
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			Row[] stateRow = allDimensionToTimes.collect();
			lagPhaseFaultToMap(stateRow);
		}
	}
	
	private static void lagPhaseFaultSave(String time, String period) {
		logger.info("故障趋势实时 map:"+faultMap.size());
		if (faultMap.size() > 0) {
			IndexToRedis.faultToRedis(faultMap, time,period);
			faultMap.clear();
		}
	}
	
	public static void lagPhaseFaultToMap(Row[] rowRate){
		Set<String> preKeySet = new HashSet<String>();
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(1)+"#"+ row.getString(2);
				if (preKeySet.add(key)) { // 未初始化
					for (int i = 1; i < 18; i++) {
						if(i != 16){
							faultMap.put(key + "#" + i, 0l);
						}
					}
				}
				String keyAll = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3) + "#" + row.getString(4)  + "#" + row.getString(5)  + "#" + row.getString(6);
				faultMap.put(keyAll, row.getLong(0));
			}
		}
		preKeySet.clear();
	}
}
