package com.sihuatech.sqm.spark.offline;

import java.util.Calendar;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.StateBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.common.Constants;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

/**
 * 设备状态日志离线分析 时间维度：
 * 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（
 * 年） TASK_NUMBER:201表示总播放次数、流用户数，202开机用户数,200表示所有
 * 201、202包含PEAK（峰时）的时间维度
 */
public class TerminalStateSumOffLine {
	private static Logger logger = Logger.getLogger(TerminalStateSumOffLine.class);
	private static final int PARTITIONS_NUM = 100;
	private static HashMap<String, Long> play_count = new HashMap<String, Long>();
	private static HashMap<String, Long> play_user = new HashMap<String, Long>();
	private static HashMap<String, Long> start_hm = new HashMap<String, Long>();
	private static String[] dimension = { "provinceID","cityID", "platform", "deviceProvider", "fwVersion" };
	//redis失效时间
	private static  int REDIS_TIME_LENGTH = 7*24*60*60;
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务号 */
	private static String TASK_NUMBER = "";

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: TerminalStateSumOffLine <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task:  value is "+Constant.STATE_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASK_NUMBER = args[2];
		
		logger.info(String.format("设备状态日志离线分析执行参数 : \n period : %s \n path : %s task: %s", PERIOD, PATH, TASK_NUMBER));
		if (!Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: TerminalStateSumOffLine <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task:  value is "+Constant.STATE_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.STATE_TASK_ENUM.contains(TASK_NUMBER)){
			System.err.println("Usage: TerminalStateSumOffLine <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ " \n"
					+ "  path: root folder + file prefix \n" 
					+ "  task:  value is "+Constant.STATE_TASK_ENUM);
			System.exit(1);
		}
		Calendar c = null;
		if (args.length > 3) {
			String time = args[3];
			c = DateUtil.getAppointedTime(time);
		} else {
			c = DateUtil.getBackdatedTime(PERIOD);
		}
		int partitionNums = PARTITIONS_NUM;
		if (args.length > 4) {
			partitionNums = Integer.parseInt(args[4]);
		}
		int index = PATH.lastIndexOf("/");
		String filePath = PATH.substring(0, index) + DateUtil.getPathPattern(PATH.substring(index + 1), PERIOD,c);
		filePath = filePath + "*";
		
		SparkConf sparkConf = new SparkConf().setAppName("TerminalStateSumOffLine");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx);
		logger.info("文件路径：" + filePath);
		
		String REDIS_LOSE_TIME = PropHelper.getProperty("REDIS_LOSE_TIME");
		
		if (REDIS_LOSE_TIME != null && !REDIS_LOSE_TIME.equals("")) {
			REDIS_TIME_LENGTH = Integer.valueOf(REDIS_LOSE_TIME) * 24*60*60; 
		}
		//广播变量
		final Broadcast<Integer> REDIS_LOSE_TIME_LENGTH = ctx.broadcast(REDIS_TIME_LENGTH);
		
		// 读取HDFS符合条件的文件
		JavaPairRDD<BytesWritable, BytesWritable> slines = ctx.sequenceFile(filePath, BytesWritable.class, BytesWritable.class);
		JavaRDD<String> lines = slines.values().map(new Function<BytesWritable, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(BytesWritable value) throws Exception {
				return new String(value.getBytes());
			}
		});
		
		// 校验日志，过滤不符合条件的记录
		JavaRDD<String> filterLines = lines.filter(new Function<String, Boolean>() {
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
//		logger.info("获取上次设备最终状态数据...");
//		String filePathPrefix = PATH.substring(0, index);
//		Calendar lastc = DateUtil.getBackdatedTime(c, PERIOD);
//		String lastFilePath = filePathPrefix + Constants.DIRECTORY_DELIMITER
//				+ Constants.LOG_STATE + "_" + PERIOD + Constants.DIRECTORY_DELIMITER
//				+ DateUtil.getFormedTime(PERIOD, lastc);
//		logger.info("上次设备状态数据文件路径：" + lastFilePath);
//		JavaRDD<String> unionRDD = null;
//		FileSystem fs = FileSystem.newInstance(new Configuration());
//		boolean unionFlag = fs.exists(new Path(lastFilePath));
//		logger.info("文件" + (unionFlag ? "存在" : "不存在"));
//		// 为了计算时间段数据而非点数据，本次设备状态的多条记录需要和最终状态合并后计算
//		// 如果之前无状态记录文件，则不需要合并计算，直接计算本次的
//		if (unionFlag) {
//			JavaRDD<String> lastRDD = ctx.textFile(lastFilePath);
//			unionRDD = lastRDD.union(filterLines);
//		} else {
//			unionRDD = filterLines;
//		}
//		
//		unionRDD = unionRDD.filter(new Function<String, Boolean>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public Boolean call(String line) throws Exception {
//				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
//				if (lineArr.length < 7) {
//					return false;
//				} else if (!"2".equals(lineArr[0])) {
//					return false;
//				}
//				return true;
//			}
//		});
//		
//		JavaRDD<String> tmpRDD = null;
//		if (unionRDD.getNumPartitions() < partitionNums) {
//			tmpRDD = unionRDD.repartition(partitionNums);
//		} else {
//			tmpRDD = unionRDD.coalesce(partitionNums);
//		}
//		tmpRDD.cache();
//		// 上次设备最终状态和本次设备状态日志合并后去重取最后状态
//		JavaRDD<String> currentRDD = tmpRDD.mapToPair(new PairFunction<String, String, String>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(String line) throws Exception {
//				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
//				return new Tuple2<String, String>(lineArr[1], line);
//			}
//		}).reduceByKey(new Function2<String, String, String>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String call(String v1, String v2) throws Exception {
//				String[] v1Arr = v1.split(String.valueOf((char)0x7F), -1);
//				String[] v2Arr = v2.split(String.valueOf((char)0x7F), -1);
//				if (v1Arr[4].compareTo(v2Arr[4]) > 0) {
//					return v1;
//				} else {
//					return v2;
//				}
//			}
//		}).values();
//		logger.info("写入本次设备最终状态数据...");
//		String currentFilePath = filePathPrefix + Constants.DIRECTORY_DELIMITER
//				+ Constants.LOG_STATE + "_" + PERIOD + Constants.DIRECTORY_DELIMITER
//				+ DateUtil.getFormedTime(PERIOD, c);
//		logger.info("本次设备状态数据文件路径：" + currentFilePath);
//		Path path = new Path(currentFilePath);
//		if (fs.exists(path)) {
//			fs.delete(path, true);
//		}
//		currentRDD.repartition(1).saveAsTextFile(currentFilePath);
//		logger.info("分区数：" + tmpRDD.getNumPartitions());
//		
		// 提取字段转为Bean
		JavaRDD<StateBean> rowRDDD = filterLines.map(new Function<String, StateBean>() {
			private static final long serialVersionUID = 1L;

			public StateBean call(String line) {
				StateBean terminalState = null;
				if (StringUtils.isNotBlank(line)) {
					String[] fields = line.split(String.valueOf((char)0x7F), -1);
					if ("0".equals(fields[3])) { // 状态为关机的不计算
						return null;
					}
					String info = IndexToRedis.getBaseInfo(fields[1]);
					if (StringUtils.isNotBlank(info)) {
						String[] infoArr = info.split(String.valueOf((char)0x7F), -1);
						terminalState = new StateBean();
						terminalState.setProbeID(fields[1]);
						terminalState.setHasID(fields[2]);
						try {
							terminalState.setState(Integer.parseInt(fields[3]));
						} catch(Exception e) {
							logger.error("设备状态有误，probeId：" + fields[1] + "，state：" + fields[3]);
						}
						terminalState.setDeviceProvider(infoArr[3]);
						terminalState.setPlatform(infoArr[4]);
						terminalState.setProvinceID(infoArr[5]);
						terminalState.setCityID(infoArr[6]);
						// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
						String[] area = IndexToRedis.getStbArea(terminalState.getProbeID(), terminalState.getProvinceID());
						if (area != null) {
							terminalState.setProvinceID(area[0]);
							terminalState.setCityID(area[1]);
						}
						terminalState.setFwVersion(infoArr[7]);
					} else {
						logger.info("redis中不存在设备ID=" + fields[1] + " 对应的信息");
					}
				}
				return terminalState;
			}
		});
		
		// 过滤对象为空
		JavaRDD<StateBean> rowRDD = rowRDDD.filter(new Function<StateBean, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(StateBean terminalState) throws Exception {
				if (null == terminalState) {
					return false;
				}
				return true;
			}
		});
		
		// 注册临时表
		DataFrame imageDataFrame = sqlContext.createDataFrame(rowRDD, StateBean.class);
		imageDataFrame.registerTempTable("TerminalStateOffline");
		long total = imageDataFrame.count();
		logger.info("+++[TerminalState]设备状态日志记录数：" + total);
		if(total > 0){
			if(TASK_NUMBER.equals(Constant.PLAY_NUM)) {
//				playCountDimensions(sqlContext,c,REDIS_LOSE_TIME_LENGTH);
			}else if (TASK_NUMBER.equals(Constant.START_NUM)) {
				startUserDimensions(sqlContext,c);
			}else if (TASK_NUMBER.equals(Constant.STATE_NUM)) {
//				playCountDimensions(sqlContext,c,REDIS_LOSE_TIME_LENGTH);
				startUserDimensions(sqlContext,c);
			}
		}
		
//		tmpRDD.unpersist();
	}

	public static void saveStartUser(Calendar c) {
		String time = DateUtil.getIndexTime(PERIOD, c);
		logger.info("开机用户数大小：" + start_hm.size());
		if (start_hm != null && start_hm.size() > 0) {
			String insertSql = "INSERT INTO T_STARTUSER (provinceID,platform,deviceProvider,fwVersion,cityID,parseTime,period,startUserCount)"
						+ " VALUES(?,?,?,?,?,?,?,?)";
			IndexToMysql.userToMysqlOffline(start_hm, time, insertSql, PERIOD);
			logger.info("开机用户数写入MySQL结束");
			start_hm.clear();
		}
	}
	
	public static void startUserDimensions(SQLContext sqlContext, Calendar c) {
		String sql = "select count(distinct probeID) as probeID,provinceID,platform,deviceProvider,fwVersion,cityID from TerminalStateOffline  group by provinceID,platform,deviceProvider,fwVersion,cityID ";
		DataFrame secondDataFrame = sqlContext.sql(sql);
		secondDataFrame.registerTempTable("cacheTerminalStartUserTable");
		sqlContext.cacheTable("cacheTerminalStartUserTable");
		allDimensionsStartAndUser(sqlContext);
		saveStartUser(c);
		sqlContext.uncacheTable("cacheTerminalStartUserTable");
	}

	public static void allCountDimensions(SQLContext sqlContext) {
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
			if (StringUtils.isBlank(groupSb.toString())) {
				sql = "select sum(hasID),sum(probeID)," + selectSb.substring(0,selectSb.length()-1) + " from cacheTerminalStateTable ";
			} else {
				sql = "select sum(hasID),sum(probeID)," + selectSb.substring(0,selectSb.length()-1) + " from cacheTerminalStateTable group by "
						+ groupSb.substring(0,groupSb.length()-1);
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			Row[] row = allDimensionToTimes.collect();
			toMapCount(row);
		}
	}

	public static void allDimensionsStartAndUser(SQLContext sqlContext) {
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
			StringBuffer sql = new StringBuffer();
			sql.append("select sum(probeID),").append(selectSb.substring(0,selectSb.length()-1));
			sql.append(" from cacheTerminalStartUserTable ");
			if (groupSb.length() > 0) {
				sql.append(" group by ").append(groupSb.substring(0, groupSb.length() - 1));
			} 
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql.toString());
			Row[] stateRow = allDimensionToTimes.collect();
			toMapUser(stateRow);
		}
	}
	

	public static void toMapCount(Row[] rowRate) {
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(2) + "#" + row.getString(3) + "#"+ row.getString(4) + "#" + row.getString(5)+"#" + row.getString(6);
				play_count.put(key, row.getLong(0));
				play_user.put(key, row.getLong(1));
			}
		}
	}

	public static void toMapUser(Row[] rowRate) {
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(1) + "#" + row.getString(2) + "#"+ row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5);
				start_hm.put(key , row.getLong(0));
			}
		}
	}
}