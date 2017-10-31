package com.sihuatech.sqm.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.HttpErrorCodeLog;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class HttpErrorCodeAnalysis {

	private static Logger logger = Logger.getLogger(HttpErrorCodeAnalysis.class);
	private static Map<String, Long> errorCodeMap = new HashMap<String, Long>();

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: HttpErrorCodeAnalysis <group> <topics> <numThreads>");
			System.exit(1);
		}
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		String httpErrorCodeTime = PropHelper.getProperty("HTTP_ERRORCODE_TIME");
		if(null != httpErrorCodeTime && !"".equals(httpErrorCodeTime)){
			logger.info(String.format("HTTP错误指标执行参数 : \n zk.quorum : %s \n group : %s \n topic : %s \n numThread : %s \n duration : %s minute",zkQuorum,
					args[0],args[1],args[2],httpErrorCodeTime));
			SparkConf conf = new SparkConf().setAppName("EPGResponseAnalysis");
			JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(Long.valueOf(httpErrorCodeTime)));
			
			//此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[2]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			String[] topics = args[1].split(",");
			for (String topic : topics) {
				topicMap.put(topic, numThreads);
			}
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, args[0],
					topicMap);

			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String,Boolean>(){
				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
					if (lineArr.length < 13) {
						return false;
					} else if (!"10".equals(lineArr[0])) {
						return false;
					} else if (StringUtils.isBlank(lineArr[3].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[4].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[5].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[6].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[7].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[10].trim())) {
						return false;
					} else if (StringUtils.isBlank(lineArr[12].trim())) {
						return false;
					}
					return true;
				}
			});
			
			filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
				public void call(JavaRDD<String> rdd, Time time) {
//					String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion" };
					String[] dimensions = { "provinceID", "cityID" };
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

					JavaRDD<HttpErrorCodeLog> rowRDD = rdd.map(new Function<String, HttpErrorCodeLog>() {
						public HttpErrorCodeLog call(String line) throws Exception {
							String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
							HttpErrorCodeLog record = new HttpErrorCodeLog();
							record.setProvinceID(lineArr[5].trim());
							record.setCityID(lineArr[6].trim());
//							record.setPlatform(lineArr[4].trim());
//							record.setDeviceProvider(lineArr[3].trim());
//							record.setFwVersion(lineArr[7].trim());
							record.setHttpRspCode(lineArr[10].trim());
							record.setCount(Long.valueOf(lineArr[13].trim()));
							return record;
						}
					});
					String periodTime = DateUtil.getRTPeriodTime();
					// 注册临时表
					DataFrame errorCodeFrame = sqlContext.createDataFrame(rowRDD, HttpErrorCodeLog.class);
					errorCodeFrame.registerTempTable("errorcode");
					
					long total = errorCodeFrame.count();//计算单位时间段内记录总数
					logger.info("+++错误码录日志记录数：" + total);
					if(total > 0){
						logger.info("计算HTTP错误");
						DataFrame tempFrame = sqlContext.sql("select sum(count) as cnt,httpRspCode,"
								+ dimensions[0] + "," + dimensions[1] + " from errorcode group by "
								+ dimensions[0] + "," + dimensions[1] +",httpRspCode");
						tempFrame.registerTempTable("cacheErrorCode");
						sqlContext.cacheTable("cacheErrorCode");
						httpErrorCodeByDimension(sqlContext,dimensions);
						sqlContext.uncacheTable("cacheErrorCode");
						
						saveHttpErrorCode(periodTime);
					}
				}
			});
			jssc.start();
			jssc.awaitTermination();
			
		}else{
			logger.info("请配置每隔多长时间取错误码日志！！");
		}
	}
	
	private static void saveHttpErrorCode(String time){
		logger.info("http错误 保存分析数据到存储系统开始 mapSize=" + errorCodeMap.size());  
		if (errorCodeMap != null && errorCodeMap.size() > 0) {
			logger.info("保存到Redis");
			IndexToRedis.errorCodeRedisOnLong(errorCodeMap, time);
			errorCodeMap.clear();
			logger.info("保存分析数据到存储系统结束");
		}
	}
	
	
	private static void httpErrorCodeByDimension(SQLContext sqlContext,String[] dimensions) {
		int num = 0;
		while (num < Math.pow(2, dimensions.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();

			for (int i = 0; i < dimensions.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				// numStr += bit;
				if (bit == 1) {
					selectSb.append(dimensions[i]).append(",");
					groupSb.append(dimensions[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("select sum(cnt),").append(selectSb.substring(0, selectSb.length() - 1))
					.append(" ,httpRspCode from cacheErrorCode");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1)).append(",httpRspCode");
			}else{
				sqlSb.append(" GROUP BY httpRspCode");
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());
			Row[] stateRow = df.collect();
			if (stateRow == null || stateRow.length == 0) {
				return;
			} else {
				for (Row row : stateRow) {
					String key = Constant.HTTP_ERRORCODE + "#" + row.getString(1) + "#"
							+ row.getString(2) + "#" + row.getString(3);
					errorCodeMap.put(key, row.getLong(0));
				}
			}
		}
	}

}
