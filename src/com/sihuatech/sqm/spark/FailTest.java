package com.sihuatech.sqm.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

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

import com.sihuatech.sqm.spark.bean.PlayFailLog;
import com.sihuatech.sqm.spark.util.PropHelper;
import com.sihuatech.sqm.spark.util.SplitCommon;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class FailTest {

	private static Logger logger = Logger.getLogger(FailTest.class);
	private static HashMap<String, Long> failMap = new HashMap<String, Long>();
	private static final Pattern TAB = Pattern.compile("\t");
	private static final String PERIOD = "1";
	private static final String[] dimension = { "provinceID", "cityID"};
	private final static int DEFAULT_NUM_PARTITIONS = 100;
	private final static long DEFAULT_BATCH_DURATION = 1;
	private static int numPartitions = 0;
	private static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 2) {
			System.err.println("Usage: PlaySuccRateAnalysis <topics> <numPartitions>\n" +
			          "  <topics> is a list of one or more kafka topics to consume from\n"
			          + " <numPartitions> 重新设置Rdd的分区数量\n\n");
			return;
		}
	   // 获取参数
		String topics = args[0];
		String _numPartitions = args[1];
		String brokers = PropHelper.getProperty("broker.quorum");;
		String _batchDuration = PropHelper.getProperty("PLAY_SUCC_TIME");
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
		SparkConf sparkConf = new SparkConf().setAppName("FailTest");
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
					String[] lineArr = SplitCommon.split(line);
					String msg = "";
					for(int i=0;i<lineArr.length;i++){
						msg += lineArr[i]+"|";
					}
					logger.info("++++++++++++++++++++失败日志++"+msg);
					if (lineArr.length < 7) {
						return false;
					} else if (!"5".equals(lineArr[0])) {
						return false;
					}
					return true;
				}
			});

			filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void call(JavaRDD<String> rdd, Time time) {
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
					JavaRDD<PlayFailLog> rowRDD = rdd.map(new Function<String, PlayFailLog>() {
						private static final long serialVersionUID = 1L;

						public PlayFailLog call(String word) throws Exception {
							PlayFailLog playFailLog = null;
							if (StringUtils.isNotBlank(word)) {
								String[] fields = word.split(String.valueOf((char) 0x7F), -1);
								playFailLog = new PlayFailLog();
								playFailLog.setDeviceProvider(fields[2]);
								playFailLog.setPlatform(fields[3]);
								playFailLog.setProvinceID(fields[4]);
								playFailLog.setCityID(fields[5]);
								playFailLog.setFwVersion(fields[6]);
								playFailLog.setKpiUtcSec(fields[7]);
								playFailLog.setHasType(fields[11]);
							}
							return playFailLog;
						}
					});
					int tmpNumPartitions = rowRDD.getNumPartitions();
					logger.info("rowRDD分区数：" + tmpNumPartitions);
					JavaRDD<PlayFailLog> resRDD = null;
					if (tmpNumPartitions < numPartitions) {
						resRDD = rowRDD.repartition(numPartitions);
					} else if (tmpNumPartitions > numPartitions){
						resRDD = rowRDD.coalesce(numPartitions);
					} else {
						resRDD = rowRDD;
					}
					logger.info("resRDD分区数：" + resRDD.getNumPartitions());
					DataFrame wordsDataFrame = sqlContext.createDataFrame(resRDD, PlayFailLog.class);
					wordsDataFrame.registerTempTable("PlayFailLog");

					Long total = wordsDataFrame.count();
					logger.info("+++[PlayFail]播放失败日志记录数：" + total);
					if (total > 0) {
						GroupedData data = wordsDataFrame.cube(dimension[0], dimension[1],"kpiUtcSec");
						data.count().filter("kpiUtcSec is not null").show();
						Row[] stateRow = data.count().filter("kpiUtcSec is not null").collect();
						if (stateRow == null || stateRow.length == 0) {
							return;
						} else {
							for (Row row : stateRow) {
								if(row == null || row.size() == 0){
									continue;
								}
								String  pID = row.getString(0);
								String  cID = row.getString(1);
								if(null == pID){
									pID  = "ALL";
								}
								if(null == cID){
									cID  = "ALL";
								}
								String key = pID + "#"+ cID+"\t"  + row.getString(2);
								logger.info("+++++++++++++++++++++++++++++++"+key+"|"+row.getLong(3));
							}
						}
					}
				}
			});
			jssc.start();
			jssc.awaitTermination();
	}
}
