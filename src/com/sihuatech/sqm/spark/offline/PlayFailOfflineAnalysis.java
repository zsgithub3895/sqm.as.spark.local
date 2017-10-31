package com.sihuatech.sqm.spark.offline;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.PlayFailLog;
import com.sihuatech.sqm.spark.bean.PlayRequestBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;
import com.sihuatech.sqm.spark.util.SplitCommon;

/**
 * 播放失败日志离线分析
 * 时间维度： 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
 * 500：全部任务都跑     501：执行播放成功率任务
 */
public class PlayFailOfflineAnalysis {
	private static Logger logger = Logger.getLogger(PlayFailOfflineAnalysis.class);
	private static final Pattern TAB = Pattern.compile("\t");
	private static final int PARTITIONS_NUM = 100;
	private static final String[] dimension = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion"};
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/*任务选择 all:全部执行  playSuccRate:播放成功率 httpStatusCode:http错误码 */
	private static String TASK ="";
	// 平均响应时长单位微秒
	private static Map<String, Long> playSuccRateMap = new HashMap<String, Long>();
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: PlaySuccRateOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ "\n"
					+ "  path: root folder + file prefix \n"
					+ "  task:  value is "+Constant.FAIL_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASK = args[2];
		
		logger.info(String.format("PERIOD : \n period : %s \n path : %s task: %s", PERIOD, PATH, TASK));
		if(! Constant.PERIOD_ENUM.contains(PERIOD)){
			System.err.println("Usage: PlaySuccRateOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ "\n"
					+ "  path: root folder + file prefix \n"
					+ "  task:  value is "+Constant.FAIL_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.FAIL_TASK_ENUM.contains(TASK)){
			System.err.println("Usage: PlaySuccRateOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+ "\n"
					+ "  path: root folder + file prefix \n"
					+ "  task:  value is "+Constant.FAIL_TASK_ENUM);
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
		
		if(StringUtils.isNotBlank(TASK) && Constant.PLAY_FIAL_ALL_TASK.equals(TASK)){
			logger.info("--------------本次执行全部任务----------------");
		}
		if(StringUtils.isNotBlank(TASK) && Constant.PLAY_SUCC_TASK.equals(TASK)){
			logger.info("--------------本次执行播放成功率任务----------------");
		}
		int index = PATH.lastIndexOf("/");
		//Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1), PERIOD, c);
		filePath = filePath +"*";
		
		SparkConf conf = new SparkConf().setAppName("PlayFailOfflineAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		logger.info("文件路径："+filePath);
		// 读取HDFS符合条件的文件
		JavaPairRDD<BytesWritable,BytesWritable> slines = sc.sequenceFile(filePath,BytesWritable.class,BytesWritable.class);
		JavaRDD<String> lines = slines.values().map(new Function<BytesWritable,String>(){
			@Override
			public String call(BytesWritable value) throws Exception {
				return new String(value.getBytes());
			}
		});
		// 过滤不符合规范的记录
		JavaRDD<String> filterLines = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String line) throws Exception {
				String[] lineArr = SplitCommon.split(line);
				if (lineArr.length < 14) {
					return false;
				} else if (!"5".equals(lineArr[0])) {
					return false;
				}/* else if ("".equals(lineArr[1])) {
					return false;
				} else if ("".equals(lineArr[2])) {
					return false;
				} else if ("".equals(lineArr[3])) {
					return false;
				} else if ("".equals(lineArr[4])) {
					return false;
				} else if ("".equals(lineArr[5])) {
					return false;
				} else if ("".equals(lineArr[6])) {
					return false;
				} else if ("".equals(lineArr[7])) {
					return false;
				} else if ("".equals(lineArr[8])) {
					return false;
				} else if ("".equals(lineArr[9])) {
					return false;
				} else if ("".equals(lineArr[10])) {
					return false;
				} else if ("".equals(lineArr[11])) {
					return false;
				}*/
				return true;
			}
		});
		// 提取字段转为Bean
		JavaRDD<PlayFailLog> tranLines = filterLines.map(new Function<String, PlayFailLog>() {
			@Override
			public PlayFailLog call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				PlayFailLog record = new PlayFailLog();
				record.setDeviceProvider(lineArr[2].trim());
				record.setPlatform(lineArr[3].trim());
				record.setProvinceID(lineArr[4].trim());
				record.setCityID(lineArr[5].trim());
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(lineArr[1].trim(), record.getProvinceID());
				if (area != null) {
					record.setProvinceID(area[0]);
					record.setCityID(area[1]);
				}
				record.setFwVersion(lineArr[6].trim());
				record.setStartSecond(lineArr[7].trim());
				record.setHasType(lineArr[11].trim());
				record.setStatusCode(lineArr[13].trim());
				return record;
			}
		});
		
		JavaRDD<PlayFailLog> tmpRDD = null;
		if (tranLines.getNumPartitions() < partitionNums) {
			tmpRDD = tranLines.repartition(partitionNums);
		} else {
			tmpRDD = tranLines.coalesce(partitionNums);
		}
		logger.info("分区数：" + tmpRDD.getNumPartitions());
		// 注册临时表
		DataFrame playSuccDataFrame = sqlContext.createDataFrame(tmpRDD, PlayFailLog.class);
		playSuccDataFrame.registerTempTable("playSuccResponse");
		
		long total = playSuccDataFrame.count();// 计算单位时间段内记录总数
		logger.info("+++[PlayFail]播放失败日志记录数：" + total);
		if (total > 0) {
			if(StringUtils.isNotBlank(TASK) && (Constant.PLAY_FIAL_ALL_TASK.equals(TASK) 
					|| Constant.PLAY_SUCC_TASK.equals(TASK))){
				playSuccesRate(sqlContext, c);
			}
		}
	}
	
	private static void playSuccesRate(SQLContext sqlContext,Calendar c) {
		String[] HASTYPE = PropHelper.getProperty("HASTYPE").split(",");
		String sqlEnd = "where hasType = ";
	    for (int i = 0; i < HASTYPE.length - 1; i++) {
	    	sqlEnd += HASTYPE[i] + " or hasType = ";
	    }
	    sqlEnd += HASTYPE[HASTYPE.length - 1];
		//这里添加中间表，因为是四个维度，和下面的五个维度分开，等以后添加了地市后再合并在一起
		String middleSql = "select count(*) as total,"+ dimension[0] + "," + dimension[1] + "," + dimension[2]
				+ "," + dimension[3] + "," + dimension[4] + " from playSuccResponse " + sqlEnd + " group by " + dimension[0]
				+ "," + dimension[1] + "," + dimension[2] + "," + dimension[3] + "," + dimension[4];
		DataFrame middleDataFrame = sqlContext.sql(middleSql);
		middleDataFrame.registerTempTable("cacheTable");
		sqlContext.cacheTable("cacheTable");
		int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			String numStr = "";
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				numStr += bit;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			String sql = null;
			if(StringUtils.isBlank(groupSb.toString())){
				sql="select sum(total),"+selectSb.substring(0,selectSb.length()-1)+" from cacheTable ";
			}else{
				sql="select sum(total),"+selectSb.substring(0,selectSb.length()-1)+" from cacheTable  group by "+groupSb.substring(0,groupSb.length()-1);
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String call(Row row) {
					String kk = row.getString(1) + "#" + row.getString(2)
							+ "#" + row.getString(3) + "#"
							+ row.getString(4) +  "#" + row.getString(5) + "\t" + row.getLong(0);;
					return kk;
				}
			});
			List<String> stateRow = avgRDD.collect();
			if (stateRow == null || stateRow.size() == 0) {
			} else {
				for (String row : stateRow) {
					String[] rowArr = TAB.split(row,-1);
					playSuccRateMap.put(rowArr[0], Long.valueOf(rowArr[1]));
				}
			}
		}
		if(null != playSuccRateMap && playSuccRateMap.size() >0){
			String time = DateUtil.getIndexTime(PERIOD, c);
			logger.info("map的大小："+playSuccRateMap.size()+"    time="+time);
			IndexToRedis.playSuccToRedisOffline(playSuccRateMap,PERIOD,time);
			playSuccRateMap.clear();
		}
		sqlContext.uncacheTable("cacheTable");
	}
}
