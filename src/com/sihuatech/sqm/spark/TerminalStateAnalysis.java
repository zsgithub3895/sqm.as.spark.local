package com.sihuatech.sqm.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.TerminalState;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class TerminalStateAnalysis {
	private static Logger logger = Logger.getLogger(TerminalStateAnalysis.class);
	
	private static Map<String, TerminalState> stateMap = new HashMap<String, TerminalState>();
	
	private static HashMap<String, Long> startUserMap = new HashMap<String, Long>();
	private static String[] dimensions = { "provinceID", "cityID" };
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 3) {
			System.err.println("Usage: TerminalStateAnalysis <zkGroup> <topic> <numStreams>");
			System.exit(1);
		}
		
		// 获取参数
		String zkGroup = args[0];
		String topic = args[1];
		String numStreamsS = args[2]; // 此参数为接收Topic的线程数，并非Spark分析的分区数
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		String batchDurationS = PropHelper.getProperty("TERMINAL_STATE_TIME");
		logger.info("+++[TerminalState]parameters...\n"
				+ "zkGroup:" + zkGroup + "\n"
				+ "topic:" + topic + "\n"
				+ "numStreams:" + numStreamsS + "\n"
				+ "zkQuorum:" + zkQuorum + "\n"
				+ "batchDuration:" + batchDurationS + "\n");
		// 转换参数提供使用
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		int numStreams = Integer.parseInt(numStreamsS);
		long batchDuration = Long.valueOf(batchDurationS);
		// spark任务初始化
		SparkConf sparkConf = new SparkConf().setAppName("TerminalStateAnalysis");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.minutes(batchDuration));
		logger.info("+++[TerminalState]开始读取内容：");
		List<JavaPairDStream<String, String>> streamsList = new ArrayList<JavaPairDStream<String, String>>(numStreams);
		for (int i = 0; i < numStreams; i++) {
			streamsList.add(KafkaUtils.createStream(jssc, zkQuorum, zkGroup, topicMap));
		}
		/* Union all the streams if there is more than 1 stream */
		JavaPairDStream<String, String> unionStreams;
		if (streamsList.size() > 1) {
			unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
		} else {
			/* Otherwise, just use the 1 stream */
			unionStreams = streamsList.get(0);
		}
		
		JavaDStream<String> lines = unionStreams.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		
		// 校验日志，过滤不符合条件的记录
		JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 7) {
					return false;
				} else if (!"2".equals(lineArr[0])) {
					return false;
				}
				return true;
			}
		});
		
		// 提取字段转为Bean
		JavaDStream<TerminalState> objs = filterLines.map(new Function<String, TerminalState>() {
			private static final long serialVersionUID = 1L;
			public TerminalState call(String line) {
				TerminalState terminalState = null;
				if (StringUtils.isNotBlank(line)) {
					String[] fields = line.split(String.valueOf((char) 0x7F), -1);
					String info = IndexToRedis.getBaseInfo(fields[1]);
					if (StringUtils.isNotBlank(info)) {
						String[] infoArr = info.split(String.valueOf((char) 0x7F), -1);
						terminalState = new TerminalState();
//						terminalState.setDeviceProvider(infoArr[3]);
//						terminalState.setPlatform(infoArr[4]);
						terminalState.setProvinceID(infoArr[5]);
						terminalState.setCityID(infoArr[6]);
//						terminalState.setFwVersion(infoArr[7]);
						terminalState.setProbeID(fields[1]);
						terminalState.setHasID(fields[2]);
						terminalState.setState(fields[3]);
					}
				}
				return terminalState;
			}
		});
		
		//logger.debug("+++[TerminalState]抽象JavaBean结束，过滤NullObject开始");
		JavaDStream<TerminalState> filterObjs = objs.filter(new Function<TerminalState, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(TerminalState terminalState) throws Exception {
				if (null == terminalState) {
					return false;
				}
				return true;
			}
		});
		
		filterObjs.foreachRDD(new VoidFunction2<JavaRDD<TerminalState>, Time>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<TerminalState> tmpRDD, Time time) {
				logger.info("+++[TerminalState]RDD分区数：" + tmpRDD.getNumPartitions());
				
//				List<TerminalState> states = rdd.collect();
//				for (TerminalState state : states) {
//					if ("0".equals(state.getState())) {
//						stateMap.remove(state.getProbeID());
//					} else {
//						stateMap.put(state.getProbeID(), state);
//					}
//				}
//				JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
//				JavaRDD<TerminalState> rdd_new = jsc.parallelize(new ArrayList<TerminalState>(stateMap.values()));
//				
				SQLContext sqlContext = SQLContext.getOrCreate(tmpRDD.context());
//				
				// rdd 合并后计算
//				JavaRDD<TerminalState> tmpRDD = rdd.union(rdd_new);
//				logger.info("tmpRDD分区数：" + tmpRDD.getNumPartitions());
				int partitionNums = 30;
				JavaRDD<TerminalState> tmpRDD2 = null;
				if (tmpRDD.getNumPartitions() < partitionNums) {
					tmpRDD2 = tmpRDD.repartition(partitionNums);
				} else {
					tmpRDD2 = tmpRDD.coalesce(partitionNums);
				}
				logger.info("tmpRDD2分区数：" + tmpRDD2.getNumPartitions());
				
				//logger.debug("+++[TerminalState]过滤NullObject结束");
				String periodTime = DateUtil.getRTPeriodTime();
				DataFrame df = sqlContext.createDataFrame(tmpRDD2, TerminalState.class);
				df.registerTempTable("TerminalState");
				long total = df.count();
				logger.info("+++[TerminalState]设备状态日志记录数：" + total);
				if (total > 0) {
					// 开机用户数
					startTableDimensions(sqlContext, periodTime);
				}
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}
	
	public static void startTableDimensions(SQLContext sqlContext,String time) {
		String sqlStart = "select count(distinct probeID) as probeID,provinceID,cityID from TerminalState where state != '0' group by provinceID,cityID ";
		DataFrame secondDataFrame = sqlContext.sql(sqlStart);
		secondDataFrame.registerTempTable("startTerminalStateTable");
		sqlContext.cacheTable("startTerminalStateTable");
		logger.debug("+++[TerminalState]-开机-分析开始");
		String result = "sum(probeID)";
		String tableName = "startTerminalStateTable";
		terminalStatePlayByDimensions(sqlContext,result,tableName);
		logger.debug("+++[TerminalState]-开机-分析结束，分析结果记录数如下...\n");
		saveStart(time);
		sqlContext.uncacheTable("startTerminalStateTable");
	}
	
	public static void terminalStatePlayByDimensions(SQLContext sqlContext,String result,String tableName) {
		int num = 0;
		while (num < Math.pow(2, dimensions.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
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
			sqlSb.append("select ").append(result).append(",").append(selectSb.substring(0, selectSb.length() - 1))
					.append(" from ").append(tableName);
			if (groupSb.length() > 0) {
				sqlSb.append(" group by ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			num++;

			DataFrame df = sqlContext.sql(sqlSb.toString());
			Row[] stateRow = df.collect();
			teminalStateStartMapRow(stateRow);
		}
	}
	
	public static void saveStart(String time) {
		logger.info("开机用户数 存储map大小："+startUserMap.size());
		if(null != startUserMap && startUserMap.size() > 0) {
			IndexToRedis.startCountToRedisOnline(startUserMap, time);
			startUserMap.clear();
		}
	}
	
	private static void teminalStateStartMapRow(Row[] rowRate) {
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(1) + "#" + row.getString(2);
					startUserMap.put(key , row.getLong(0));
			}
		}
	}
}
