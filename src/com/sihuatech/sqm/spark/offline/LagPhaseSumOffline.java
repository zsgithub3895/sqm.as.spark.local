package com.sihuatech.sqm.spark.offline;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
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

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
/**
 * 卡顿指标离线分析
 * 时间维度： 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
 */

/**离线分析卡顿原因报表
 * 运营商网络问题 (2：带宽不足 5：网络延迟过大 6：网络丢包严重 8：网络连接中断 11 网络路由不稳定)
 * 家庭网络问题  (15 家庭网络问题)
 * CDN平台问题  (1：服务器性能问题 9：播放列表服务器问题 10：CDN调度服务器问题 12 直播M3U8服务异常)
 * 终端问题 (3：OTT终端问题 13:终端硬件性能问题 14:终端分片调度问题)
 */
public class LagPhaseSumOffline {
	private static Logger logger = Logger.getLogger(LagPhaseSumOffline.class);
	private static final int PARTITIONS_NUM = 100;
	private static HashMap<String, Long> naturalMap = new HashMap<String, Long>();
	private static HashMap<String, String> causeMap = new HashMap<String, String>();
	private static final Pattern TAB = Pattern.compile("\t");
	
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务号 */
	private static String TASK_NUMBER = "";

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: LagPhaseSumOffline <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.LAG_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASK_NUMBER = args[2];
		
		logger.info(String.format("卡顿指标离线分析执行参数 : \n period : %s \n path : %s \n task : %s", PERIOD, PATH, TASK_NUMBER));
		if (! Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: LagPhaseSumOffline <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.LAG_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.LAG_TASK_ENUM.contains(TASK_NUMBER)){
			System.err.println("Usage: LagPhaseSumOffline <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.LAG_TASK_ENUM);
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

		boolean taskNumber2 = false;
		boolean taskNumber3 = false;
		boolean taskNumber4 = false;
		if (TASK_NUMBER.isEmpty()){
			logger.info("任务号参数错误：" + args[2]);
			System.exit(1);
		}else if(TASK_NUMBER.equals(Constant.LAG_NUM)){
				taskNumber2 = true;
				taskNumber3 = true;
				taskNumber4 = true;
		}else if(TASK_NUMBER.equals(Constant.FAULT_NUM)){
					taskNumber2 = true;
		}else if(TASK_NUMBER.equals(Constant.CAUSE_NUM)){
					taskNumber3 = true;
		}else if(TASK_NUMBER.equals(Constant.NATURAL_NUM)){
					taskNumber4 = true;
		}
		
		if((taskNumber2 == false) && (taskNumber3 == false) && (taskNumber4 == false) ){
			logger.info("任务号参数错误："+args[2]);
			System.exit(1);
		}
		int index = PATH.lastIndexOf("/");
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1), PERIOD, c);
		filePath = filePath + "*";

		SparkConf sparkConf = new SparkConf().setAppName("LagPhaseSumOffline");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx);
		logger.info("文件路径：" + filePath);
 
		// 读取HDFS符合条件的文件
		JavaPairRDD<BytesWritable, BytesWritable> slines = ctx.sequenceFile(filePath, BytesWritable.class,
				BytesWritable.class);
		JavaRDD<String> lines = slines.values().map(new Function<BytesWritable, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(BytesWritable value) throws Exception {
				return new String(value.getBytes());
			}
		});
		logger.debug("+++[Lag]日志验证开始");
		// 校验日志，过滤不符合条件的记录
		JavaRDD<String> filterLines = lines.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 16) {
					return false;
				} else if (!"4".equals(lineArr[0])) {
					return false;
				}
				return true;
			}
		});
		logger.debug("+++[Lag]日志验证结束，抽象JavaBean开始");
		JavaRDD<LagPhaseBehaviorLog> rowRDDD = filterLines.map(new Function<String, LagPhaseBehaviorLog>() {
			private static final long serialVersionUID = 1L;

			public LagPhaseBehaviorLog call(String word) {
				LagPhaseBehaviorLog lagLog = null;
				if (StringUtils.isNotBlank(word)) {
					String[] lineArr = word.split(String.valueOf((char)0x7F), -1);
					try {
						lagLog = new LagPhaseBehaviorLog(
								Integer.valueOf(lineArr[0]), lineArr[1], lineArr[2],
								lineArr[3], lineArr[4], lineArr[5], lineArr[6],
								lineArr[7],lineArr[8], lineArr[10]);
						// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
						String[] area = IndexToRedis.getStbArea(lagLog.getProbeID(), lagLog.getProvinceID());
						if (area != null) {
							lagLog.setProvinceID(area[0]);
							lagLog.setCityID(area[1]);
						}
					} catch (ArrayIndexOutOfBoundsException e) {
						logger.error("*********************", e);
					}
				}
				return lagLog;
			}
		});
		logger.debug("+++[Lag]抽象JavaBean结束，过滤NullObject开始");
		// 过滤对象为空
		JavaRDD<LagPhaseBehaviorLog> rowRDD = rowRDDD.filter(new Function<LagPhaseBehaviorLog, Boolean>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(LagPhaseBehaviorLog lag) throws Exception {
						if (null == lag) {
							return false;
						}
						return true;
					}
				});
		
		String[] dimension = { "provinceID","cityID", "platform", "deviceProvider","fwVersion" };
		
		String[] dimensionExportId = { "provinceID","cityID", "platform", "deviceProvider","fwVersion","exportId" };
		JavaRDD<LagPhaseBehaviorLog> tmpRDD = null;
		if (rowRDD.getNumPartitions() < partitionNums) {
			tmpRDD = rowRDD.repartition(partitionNums);
		} else {
			tmpRDD = rowRDD.coalesce(partitionNums);
		}
		DataFrame lagDataFrame = sqlContext.createDataFrame(tmpRDD, LagPhaseBehaviorLog.class);
		long total = lagDataFrame.count();
		logger.info("+++[Lag]卡顿日志记录数：" + total);
		lagDataFrame.registerTempTable("LagPhaseLogUserRate");
		
		//故障原因次数分布
		if(taskNumber2 == true && rowRDD.count() != 0){
			String sql = "select count(*) as faultCount,provinceID,cityID,platform,deviceProvider,fwVersion,exportId from LagPhaseLogUserRate group by provinceID,cityID,platform,deviceProvider,fwVersion,exportId ";
			logger.debug("+++[Lag]故障原因次数分布预分析开始，SQL：\n" + sql);
			DataFrame secondDataFrame = sqlContext.sql(sql);
			if (logger.isDebugEnabled()) {
				long count = secondDataFrame.count();
				logger.debug("+++[Lag]故障原因次数分布预分析结束，预分析结果记录数：" + count);
			}
			secondDataFrame.registerTempTable("cacheFaultLagPhaseTable");
			sqlContext.cacheTable("cacheFaultLagPhaseTable");
			logger.debug("+++[Lag]故障原因次数分布分析开始");
			allDimensionsFault(sqlContext, dimensionExportId,c);
			sqlContext.uncacheTable("cacheFaultLagPhaseTable");
		}
		
		//故障原因分布
		if(taskNumber3 == true && rowRDD.count() != 0){
			int causeNum = 0;
				String userSql = " select  ff.c1,ff.c2,ff.c3,ff.c4,ff.provinceID,ff.cityID,ff.platform,ff.deviceProvider,ff.fwVersion  from " +
					" (select case when t.exportId in (2,5,6,8,11) then 1 else 0 end as c1, " +
					" case when t.exportId in (15)  then 1 else 0  end as c2, " +
					" case when t.exportId in (1,9,10,12) then 1 else 0  end as c3, " +
					" case when t.exportId in (3,13,14) then 1 else 0  end as c4, " +
					" t.provinceID,t.cityID,t.platform,t.deviceProvider,t.fwVersion " +
					" from LagPhaseLogUserRate t ) ff ";
			logger.debug("+++[Lag]故障原因分布预分析开始，SQL：\n" + userSql);
			DataFrame allDimensionToUser = sqlContext.sql(userSql);
			if (logger.isDebugEnabled()) {
				long count = allDimensionToUser.count();
				logger.debug("+++[Lag]故障原因分布预分析结束，预分析结果记录数：" + count);
			}
//			allDimensionToUser.show();
			allDimensionToUser.registerTempTable("cacheTable");
			sqlContext.cacheTable("cacheTable");
			logger.debug("+++[Lag]故障原因分布分析开始");
			while (causeNum < Math.pow(2, dimension.length)) {
				StringBuffer groupSb = new StringBuffer();
				StringBuffer selectSb = new StringBuffer();
				for (int i = 0; i < dimension.length; i++) {
					// 二进制从低位到高位取值
					int bit = (causeNum >> i) & 1;
					if (bit == 1) {
						selectSb.append(dimension[i]).append(",");
						groupSb.append(dimension[i]).append(",");
					} else {
						selectSb.append("'ALL'").append(",");
					}
				}
				String sql = null;
				if(StringUtils.isBlank(groupSb.toString())){
					sql="select sum(c1+c2+c3+c4),sum(c1),sum(c2),sum(c3),sum(c4),"+selectSb.toString()+" from cacheTable ";
				}else{
					sql="select sum(c1+c2+c3+c4),sum(c1),sum(c2),sum(c3),sum(c4),"+selectSb.toString()+" from cacheTable  group by "+groupSb;
					sql=sql.substring(0, sql.length()-1);
				}
				String sql2 = sql.replace(", from", " from");
				causeNum++;
				DataFrame allDimensionToTimes = sqlContext.sql(sql2);
//				allDimensionToTimes.show();
				JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public String call(Row row) {
						String kk = "KEY" + "#" + row.getString(5) + "#" + row.getString(6) + "#"
								+ row.getString(7) + "#" + row.getString(8) + "#" + row.getString(9) 
								+ "\t" + "VALUE" +"#"+ row.getLong(0) + "#" +row.getLong(1) 
								+ "#" +row.getLong(2) + "#" +row.getLong(3) + "#" +row.getLong(4);
						return kk;
					}
				});
				List<String> stateRow = avgRDD.collect();
				causeToMap(stateRow);
			}
			logger.debug("+++[Lag]故障原因分布分析结束，分析结果记录数如下...\n"
					+ "故障原因分布：" + causeMap.size() + "\n");
			if (causeMap.size() > 0) {
				logger.info("卡顿原因 map的大小:"+causeMap.size());
				causeSave(c);
				// 清空本次分析数据
				causeMap.clear();
			}else{
				logger.info("卡顿原因 map 为空");
			}
			sqlContext.uncacheTable("cacheTable");
		}
		
		//自然播放率
		if(taskNumber4 == true && rowRDD.count() != 0){
			String sql = "select count(distinct hasID) as hasID,provinceID,cityID,platform,deviceProvider,fwVersion from LagPhaseLogUserRate group by provinceID,cityID,platform,deviceProvider,fwVersion ";
			logger.debug("+++[Lag]自然播放率预分析开始，SQL：\n" + sql);
			DataFrame fourDataFrame = sqlContext.sql(sql);
			if (logger.isDebugEnabled()) {
				long count = fourDataFrame.count();
				logger.debug("+++[Lag]自然播放率预分析结束，预分析结果记录数：" + count);
			}
			fourDataFrame.registerTempTable("cacheNaturalLagPhaseTable");
			sqlContext.cacheTable("cacheNaturalLagPhaseTable");
			logger.debug("+++[Lag]自然播放率分布分析开始");
			allDimensionsUserNatural(sqlContext,dimension);
			logger.debug("+++[Lag]自然播放率分析结束，分析结果记录数如下...\n"
					+ "自然播放率：" + naturalMap.size() + "\n");
			saveNatural(c);
			sqlContext.uncacheTable("cacheNaturalLagPhaseTable");
		 }
	}

	private static void saveNatural(Calendar c) {
	  if(naturalMap.size() > 0) {
		logger.info("自然缓冲率 map的大小:"+naturalMap.size());
		String time = DateUtil.getIndexTime(PERIOD, c);
		/** 自然播放率写入MySQL */
		IndexToRedis.playCountToRedisOffline(naturalMap, time,PERIOD);
		naturalMap.clear();
	  	}else{
			logger.info("自然缓冲率 map为空");
		}
	}
	
	private static void causeSave(Calendar c) {
		logger.info("卡顿原因 保存分析数据到存储系统开始");
		String time = DateUtil.getIndexTime(PERIOD, c);
		logger.info("卡顿原因 保存到Mysql");
		/** 卡頓原因写入MySQL */
		IndexToMysql.causeToMysqlOffline(causeMap, time ,PERIOD);
		logger.info("卡顿原因 保存分析数据到存储系统结束");
	}
	
	public static void naturalMap(List<String> rowRate){
		if (rowRate == null || rowRate.size() == 0) {
			return;
		} else {
			for (String row : rowRate) {
				String[] rowArr = TAB.split(row,-1);
				naturalMap.put(rowArr[0], Math.round(Double.valueOf(rowArr[1])));
			}
		}
	}
	
	
	public static void causeToMap(List<String> rowRate){
		if (rowRate == null || rowRate.size() == 0) {
			return;
		} else {
			for (String row : rowRate) {
				String[] rowArr = TAB.split(row,-1);
				causeMap.put(rowArr[0] ,rowArr[1]);
//				logger.info("map值: "+row);
			}
		}
	}
	
	public static void allDimensionsUserNatural(SQLContext sqlContext, String[] dimension) {
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
					sql.append("select sum(hasID),").append(selectSb.toString().substring(0, selectSb.length()-1)).append(" from cacheNaturalLagPhaseTable ");
					if(StringUtils.isNotBlank(groupSb.toString())){
						sql.append(" group by ").append(groupSb.substring(0, groupSb.length()-1));
					}
					num++;
					DataFrame allDimensionToTimes = sqlContext.sql(sql.toString());
//					allDimensionToTimes.show();
					JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
						private static final long serialVersionUID = 1L;
						@Override
						public String call(Row row) {
							String kk = Constant.LAG_PLAY + "#" + row.getString(1) + "#" + row.getString(2) + "#"
									+ row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5) + "\t" + row.getLong(0);
							logger.info("维度中的key：" + kk + "    卡顿播放次数：" + row.getLong(0));
							return kk;
						}
					});
					List<String> stateRow = avgRDD.collect();
					naturalMap(stateRow);
				}
	}
	
	public static void allDimensionsFault(SQLContext sqlContext, String[] dimensionExportId, Calendar c){
		int faultNum = 0;
		while (faultNum < Math.pow(2, dimensionExportId.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			for (int i = 0; i < dimensionExportId.length; i++) {
				// 二进制从低位到高位取值
				int bit = (faultNum >> i) & 1;
				if (bit == 1) {
					selectSb.append(dimensionExportId[i]).append(",");
					groupSb.append(dimensionExportId[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT sum(faultCount),").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM cacheFaultLagPhaseTable");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			faultNum++;
			DataFrame allDimensionToTimes = sqlContext.sql(sqlSb.toString());
			
			Row[] rows = allDimensionToTimes.collect();
			if(null != rows && rows.length>0 ){
				logger.debug("+++[Lag]故障原因次数分布分析结束，分析结果记录数如下...\n" + "故障原因次数分布：" + rows.length+ "\n");
				String time = DateUtil.getIndexTime(PERIOD, c);
				logger.info("故障次数分布 保存到Mysql");
				/** 故障次数分布写入MySQL */
				IndexToMysql.faultToMysqlOffline(rows, time,PERIOD);
			}else{
				logger.debug("+++[Lag]故障原因次数分布分析结束，分析结果记录数如下...\n" + "故障原因次数分布：0 \n");
			}
		}
	}
}
