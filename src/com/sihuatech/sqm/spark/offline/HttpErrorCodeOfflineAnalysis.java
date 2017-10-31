package com.sihuatech.sqm.spark.offline;

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

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.HttpErrorCodeLog;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;

/**
 * 错误码日志 指标:HTTP错误码-统计各种错误码的发生次数
 * 
 * @author chuql 2017年3月13日
 */
public class HttpErrorCodeOfflineAnalysis {

	private static Logger logger = Logger.getLogger(HttpErrorCodeOfflineAnalysis.class);
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务编号 */
	private static String TASKID = "";

	private static Map<String, Long> errorCodeMap = new HashMap<String, Long>();

	private static String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion" };

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: HttpErrorCodeOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is " + Constant.PERIOD_ENUM + " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task: value is " + Constant.ERRORCODE_TASK_ENUM);
			System.exit(1);
		}

		PERIOD = args[0];
		PATH = args[1];
		TASKID = args[2];

		logger.info(String.format("机顶盒登录日志离线分析执行参数 : \n period : %s \n path : %s \n task : %s", PERIOD, PATH, TASKID));
		if (!Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: HttpErrorCodeOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is " + Constant.PERIOD_ENUM + " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task: value is " + Constant.ERRORCODE_TASK_ENUM);
			System.exit(1);
		}

		if (!Constant.ERRORCODE_TASK_ENUM.contains(TASKID)) {
			System.err.println("Usage: HttpErrorCodeOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is " + Constant.PERIOD_ENUM + " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task: value is " + Constant.ERRORCODE_TASK_ENUM);
			System.exit(1);
		}

		if (StringUtils.isNotBlank(TASKID) && Constant.ERRORCODE_ALL_TASK.equals(TASKID)) {
			logger.info("--------------本次执行全部任务----------------");
		}
		if (StringUtils.isNotBlank(TASKID) && Constant.ERRORCODE_DISTRIBUTION_TASK.equals(TASKID)) {
			logger.info("--------------本次执行HTTP错误码分析任务----------------");
		}

		int index = PATH.lastIndexOf("/");
		Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index) + DateUtil.getPathPattern(PATH.substring(index + 1), PERIOD, c);
		filePath = filePath + "*";

		SparkConf conf = new SparkConf().setAppName("HttpErrorCodeOfflineAnalysis");
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

		// 提取字段转为Bean
		JavaRDD<HttpErrorCodeLog> tranLines = filterLines.map(new Function<String, HttpErrorCodeLog>() {
			@Override
			public HttpErrorCodeLog call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char) 0x7F), -1);
				HttpErrorCodeLog record = new HttpErrorCodeLog();
				record.setPlatform(lineArr[4].trim());
				record.setProvinceID(lineArr[5].trim());
				record.setCityID(lineArr[6].trim());
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(lineArr[2].trim(), record.getProvinceID());
				if (area != null) {
					record.setProvinceID(area[0]);
					record.setCityID(area[1]);
				}
				record.setDeviceProvider(lineArr[3].trim());
				record.setFwVersion(lineArr[7].trim());
				record.setHttpRspCode(lineArr[10].trim());
				record.setCount(Long.valueOf(lineArr[13].trim()));
				return record;
			}
		});

		// 注册临时表
		DataFrame errorCodeFrame = sqlContext.createDataFrame(tranLines, HttpErrorCodeLog.class);
		errorCodeFrame.registerTempTable("errorcode");

		long total = errorCodeFrame.count();// 计算单位时间段内记录总数
		logger.info("+++错误码录日志记录数：" + total);
		if (total > 0) {
			logger.info("计算HTTP错误码分布");
			DataFrame tempFrame = sqlContext.sql("select sum(count) as cnt,httpRspCode,"
					+ dimensions[0] + "," + dimensions[1] + "," + dimensions[2]+ "," + dimensions[3]+ "," + dimensions[4] + " from errorcode group by "
					+ dimensions[0] + "," + dimensions[1] + "," + dimensions[2]+ "," + dimensions[3]+ "," + dimensions[4]+",httpRspCode");
			tempFrame.registerTempTable("cacheErrorCode");
			sqlContext.cacheTable("cacheErrorCode");
			httpErrorCodeByDimension(sqlContext);
			saveHttpErrorCode(c);
			sqlContext.uncacheTable("cacheErrorCode");
		}
	}

	private static void httpErrorCodeByDimension(SQLContext sqlContext) {
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
					String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3)+ "#" + row.getString(4)+ "#" + row.getString(5)+ "#" + row.getString(6);
					errorCodeMap.put(key, row.getLong(0));
				}
			}
		}
	}

	private static void saveHttpErrorCode(Calendar c) {
		logger.info("http错误码分布  存储开始mapSize=" + errorCodeMap.size());
		if (errorCodeMap != null && errorCodeMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnHttpErrorCode(errorCodeMap, time, PERIOD);
			errorCodeMap.clear();
		}
	}

}
