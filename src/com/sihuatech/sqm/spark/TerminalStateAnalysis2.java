package com.sihuatech.sqm.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

import com.sihuatech.sqm.spark.bean.TerminalState;
import com.sihuatech.sqm.spark.util.DateUtil;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * use createDirectStream
 */
public class TerminalStateAnalysis2 {
	private static Logger logger = Logger.getLogger(TerminalStateAnalysis2.class);
	
	private static final String[] DIMENSION_2D = { "provinceID", "cityID" };
	private final static long DEFAULT_BATCH_DURATION = 5;
	private final static int DEFAULT_NUM_PARTITIONS = 100;
	
	private static int numPartitions = 0;
	private static Map<String, TerminalState> stateMap = new HashMap<String, TerminalState>();
	private static HashMap<String, Long> startUserMap = new HashMap<String, Long>();
	
	private static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 4) {
			System.err.println("Usage: TerminalStateAnalysis2 <brokers> <topics> <batchDuration> <numPartitions>\n" +
			          "  <brokers> is a list of one or more Kafka brokers\n" +
			          "  <topics> is a list of one or more kafka topics to consume from\n"
			          + " <batchDuration> 执行周期（单位：分钟）\n"
			          + " <numPartitions> 重新设置Rdd的分区数量\n\n");
			System.exit(1);
		}
		
		// 获取参数
		String brokers = args[0];
		String topics = args[1];
		String _batchDuration = args[2];
		String _numPartitions = args[3];
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
		SparkConf sparkConf = new SparkConf().setAppName("STATE-ANALYSIS");
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
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		
		// 校验日志，过滤不符合条件的记录
		JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
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
						terminalState.setProbeID(fields[1]);
						terminalState.setHasID(fields[2]);
						terminalState.setState(fields[3]);
						terminalState.setProvinceID(infoArr[5]);
						terminalState.setCityID(infoArr[6]);
					}
				}
				return terminalState;
			}
		});
		
		//logger.debug("抽象JavaBean结束，过滤NullObject开始");
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
			@Override
			public void call(JavaRDD<TerminalState> rdd, Time time) {
				logger.info("RDD分区数：" + rdd.getNumPartitions());
				
				List<TerminalState> states = rdd.collect();
				for (TerminalState state : states) {
					if ("0".equals(state.getState())) {
						stateMap.remove(state.getProbeID());
					} else {
						stateMap.put(state.getProbeID(), state);
					}
				}
				JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
				JavaRDD<TerminalState> rdd_new = jsc.parallelize(new ArrayList<TerminalState>(stateMap.values()));
				
				SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
				
				// rdd 合并后计算
				JavaRDD<TerminalState> tmpUnionRDD = rdd.union(rdd_new);
				int tmpNumPartitions = tmpUnionRDD.getNumPartitions();
				logger.info("tmpUnionRDD分区数：" + tmpNumPartitions);
				JavaRDD<TerminalState> unionRDD = null;
				if (tmpNumPartitions < numPartitions) {
					unionRDD = tmpUnionRDD.repartition(numPartitions);
				} else if (tmpNumPartitions > numPartitions){
					unionRDD = tmpUnionRDD.coalesce(numPartitions);
				} else {
					unionRDD = tmpUnionRDD;
				}
				logger.info("unionRDD分区数：" + unionRDD.getNumPartitions());
				
				//logger.debug("+++[TerminalState]过滤NullObject结束");
				String periodTime = DateUtil.getRTPeriodTime();
				DataFrame df = sqlContext.createDataFrame(unionRDD, TerminalState.class);
				df.registerTempTable("TerminalState");
				long total = df.count();
				logger.info("设备状态日志记录数：" + total);
				if (total > 0) {
					logger.info("开机用户数 分析开始");
					startUserByDimensions(sqlContext);
					logger.info("开机用户数 分析结束，分析结果记录数如下...");
					saveStart(periodTime);
				}
				
				// 打印kafka的offset
				for (OffsetRange o : offsetRanges.get()) {
					System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " "
							+ o.untilOffset());
				}
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}
	
	public static void startUserByDimensions(SQLContext sqlContext) {
		String sqlStart = "select count(distinct probeID) as probeID,provinceID,cityID from TerminalState where state != '0' group by provinceID,cityID ";
		DataFrame df = sqlContext.sql(sqlStart);
		df.registerTempTable("startTerminalStateTable");
		sqlContext.cacheTable("startTerminalStateTable");
		String result = "sum(probeID)";
		String tableName = "startTerminalStateTable";
		terminalStatePlayByDimensions(sqlContext, result, tableName);
		sqlContext.uncacheTable("startTerminalStateTable");
	}
	
	public static void terminalStatePlayByDimensions(SQLContext sqlContext, String result,
			String tableName) {
		int num = 0;
		while (num < Math.pow(2, DIMENSION_2D.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
			for (int i = 0; i < DIMENSION_2D.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				if (bit == 1) {
					selectSb.append(DIMENSION_2D[i]).append(",");
					groupSb.append(DIMENSION_2D[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("select ").append(result).append(",")
					.append(selectSb.substring(0, selectSb.length() - 1)).append(" from ")
					.append(tableName);
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
		logger.info("开机用户数 存储map大小：" + startUserMap.size());
		if (null != startUserMap && startUserMap.size() > 0) {
			IndexToRedis.startCountToRedisOnline(startUserMap, time);
			startUserMap.clear();
		}
	}
	
	private static void teminalStateStartMapRow(Row[] stateRow) {
		if (stateRow == null || stateRow.length == 0) {
			return;
		} else {
			for (Row row : stateRow) {
				String key = row.getString(1) + "#" + row.getString(2);
				startUserMap.put(key, row.getLong(0));
			}
		}
	}
}
