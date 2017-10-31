/*package com.sihuatech.sqm.spark.offline;

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.PlayRequestBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

*//**
 * 首帧指标离线分析   
 * 时间维度： 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
 * @author yan
 *
 *//*
public class PlayRequestOfflineAnalysis {
	//redis失效时间
	private static  int REDIS_TIME_LENGTH = 7*24*60*60;
	private static Logger logger = Logger.getLogger(PlayRequestOfflineAnalysis.class);
	private static final int PARTITIONS_NUM = 100;
	private static final Pattern TAB = Pattern.compile("\t");
	static DecimalFormat nf = new DecimalFormat("0.0000");
	private static String[] columns = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion"};
	private static String[] playColumns = { "provinceID", "platform", "deviceProvider", "fwVersion", "cityID"};
	日志目录
	private static String PATH = "";
	时间维度 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
	private static String PERIOD = "";
	 任务编号
	private static String TASKID = "";
	
	//首帧指标
	private static Map<String,Double> firstFrameMap = new HashMap<String,Double>();
	private static long maxLatency = 0;
	//http流量指标
	private static Map<String,Long> downBytesMap = new HashMap<String, Long>();
	//http流量-临时Map
	private static Map<String,String> tempByteMap = new HashMap<String, String>();
	//离线分析业务在线数据指标
	private static HashMap<String, Long> hm = new HashMap<String, Long>();
	//业务在线-临时Map
	private static HashMap<String, Long> tempBizMap = new HashMap<String, Long>();
	//用户卡顿分布报表离线分析
	private static HashMap<String, String> nbMap = new HashMap<String, String>();
	//总体业务质量分布指标
	private static Map<String,String> businessMap = new HashMap<String, String>();
	private static HashMap<String, Long> play_count = new HashMap<String, Long>();
	private static HashMap<String, Long> play_user = new HashMap<String, Long>();
	public static void main(String[] args){
		if(args.length < 3){
			System.err.println("Usage: PlayRequestOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is " + Constant.PLAYREQUEST_TASK_ENUM);
			System.exit(1);
		}
		
		PERIOD = args[0];
		PATH = args[1];	
		TASKID = args[2];
		logger.info(String.format("播放请求日志离线分析执行参数 : \n period : %s \n path : %s \n task : %s", PERIOD, PATH, TASKID));
		if (! Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: PlayRequestOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is " + Constant.PLAYREQUEST_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.PLAYREQUEST_TASK_ENUM.contains(TASKID)){
			System.err.println("Usage: PlayRequestOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is " + Constant.PLAYREQUEST_TASK_ENUM);
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
		if(StringUtils.isNotBlank(TASKID) && Constant.PLAYREQUEST_ALL_TASK.equals(TASKID)){
			logger.info("--------------本次执行全部任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.PLAYREQUEST_FIRSTFRAME_TASK.equals(TASKID)){
			logger.info("--------------本次执行首帧分析任务任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.PLAYREQUEST_LAG_PHASE.equals(TASKID)){
			logger.info("--------------本次执行卡顿分布分析任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.PLAYREQUEST_FREEZE.equals(TASKID)){
			logger.info("--------------本次执行HTTP流量、总体业务质量分布、业务在线分析任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.STREAM_AND_PLAY_COUNT.equals(TASKID)){
			logger.info("--------------本次执行流用户数、播放次数分析任务----------------");
		}
		
		int index = PATH.lastIndexOf("/");
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1), PERIOD,c);
		filePath = filePath+"*";
		SparkConf conf = new SparkConf().setAppName("PlayRequestOfflineAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		final Broadcast<Integer> REDIS_LOSE_TIME_LENGTH = sc.broadcast(REDIS_TIME_LENGTH);
		logger.info("文件路径：" + filePath);
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
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 40) {
					return false;
				}else if (!"3".equals(lineArr[0])) {
					return false;
				}else if (StringUtils.isBlank(lineArr[12].trim())){
					return false;
				}else if (StringUtils.isBlank(lineArr[13].trim())){
					return false;
				}else if (StringUtils.isBlank(lineArr[14].trim())){
					return false;
				}else if (StringUtils.isBlank(lineArr[16].trim())){
					return false;
				}else if (StringUtils.isBlank(lineArr[17].trim())){
					return false;
				}
				return true;
			}
		});
		//提取字段转化为Bean
		JavaRDD<PlayRequestBean> tranLines = filterLines.map(new Function<String,PlayRequestBean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public PlayRequestBean call(String line) throws Exception{
				String[] lineArr = line.split(String.valueOf((char)0x7F),-1);
				PlayRequestBean record = new PlayRequestBean();
				// 防止不同机器的HasID重复
				record.setHasID(lineArr[4].trim() + lineArr[1].trim());
				record.setProbeID(lineArr[4].trim());
				record.setDeviceProvider(lineArr[5].trim());
				record.setPlatform(lineArr[6].trim());
				record.setProvinceID(lineArr[7].trim());
				record.setCityID(lineArr[8].trim());
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(record.getProbeID(), record.getProvinceID());
				if (area != null) {
					record.setProvinceID(area[0]);
					record.setCityID(area[1]);
				}
				record.setFwVersion(lineArr[9].trim());
				record.setHasType(lineArr[10].trim());
				record.setPlaySeconds(Long.valueOf(lineArr[24].trim()));
				record.setFreezeCount(Long.valueOf(lineArr[13].trim()));
				record.setFreezeTime(Long.valueOf(lineArr[14].trim()));
				record.setLatency(Long.valueOf(lineArr[16].trim()));
				record.setDownBytes(Long.valueOf(lineArr[17].trim()));
				record.setExpertID(lineArr[18].trim());
				record.setDownSeconds(Long.valueOf(lineArr[24].trim()));
				record.setHttpRspTime(lineArr[28].trim());
				record.setM3u8HttpRspTime(lineArr[29].trim());
				record.setTcpSynTime(lineArr[32].trim());
				record.setTcpLowWinPkts(lineArr[38].trim());
				record.setTcpRetrasRate(lineArr[40].trim());
				record.setKPIUTCSec(lineArr[23].trim());
				return record;
			}
		});
		
		JavaRDD<PlayRequestBean> tmpRDD = null;
		if (tranLines.getNumPartitions() < partitionNums) {
			tmpRDD = tranLines.repartition(partitionNums);
		} else {
			tmpRDD = tranLines.coalesce(partitionNums);
		}
		logger.info("分区数：" + tmpRDD.getNumPartitions());
		DataFrame dataFrame = sqlContext.createDataFrame(tmpRDD, PlayRequestBean.class);
		long total0 = dataFrame.count();
		logger.info("+++[PlayReq]播放请求日志 -过滤前-记录数：" + total0);
		dataFrame.registerTempTable("PlayResponseLogs");
		// 过滤播放请求日志
		String filterSql = "select p.* from PlayResponseLogs p,(select hasID,max(KPIUTCSec) KPIUTCSec from  PlayResponseLogs group by hasID ) b where p.hasID=b.hasID and p.KPIUTCSec=b.KPIUTCSec ";
		DataFrame filterDataFrame = sqlContext.sql(filterSql);
		filterDataFrame.registerTempTable("fristFrameResponse");
		
		long total = filterDataFrame.count();
		logger.info("+++[PlayReq]播放请求日志-过滤后-记录数：" + total);
		if(total > 0){
			sqlContext.cacheTable("fristFrameResponse");
			firstFrameDimensions(sqlContext,c,REDIS_LOSE_TIME_LENGTH);
			sqlContext.uncacheTable("fristFrameResponse");
		}
	}
	
	public static void firstFrameDimensions(SQLContext sqlContext,Calendar c, Broadcast<Integer> rEDIS_LOSE_TIME_LENGTH){
		if(Constant.STREAM_AND_PLAY_COUNT.equals(TASKID) || Constant.PLAYREQUEST_ALL_TASK.equals(TASKID)){
			playCountDimensions(sqlContext,c,rEDIS_LOSE_TIME_LENGTH);
		}
		if(Constant.PLAYREQUEST_FIRSTFRAME_TASK.equals(TASKID) || Constant.PLAYREQUEST_ALL_TASK.equals(TASKID)){
			logger.info("计算首帧响应");
			String maxLatency = PropHelper.getProperty("LATENCY_MAX_VALUE");
			PlayRequestOfflineAnalysis.maxLatency = maxLatency == null ? 50000000 : Long.valueOf(maxLatency);
			sparkSqlFirstFrameByDimension(sqlContext);
			saveFirst(c);
		}
		if(Constant.PLAYREQUEST_LAG_PHASE.equals(TASKID) || Constant.PLAYREQUEST_ALL_TASK.equals(TASKID)){
			logger.info("计算卡顿分布");
			sparkLagPhaseByDimension(sqlContext,c);
		}
		if(Constant.PLAYREQUEST_FREEZE.equals(TASKID) || Constant.PLAYREQUEST_ALL_TASK.equals(TASKID)){
			logger.info("计算HTTP流量、总体业务质量分布");
			sparkSqlDownBytesAndFreezeByDimension(sqlContext,c);
			
			logger.info("计算业务在线");
			sparkSqlBizByDimension(sqlContext);
			if("HOUR".equals(PERIOD)){
				bizByCache(sqlContext);
			}
			saveBiz(c);
		}
};
		public static void playCountDimensions(SQLContext sqlContext,Calendar c,Broadcast<Integer> rEDIS_LOSE_TIME_LENGTH) {
			String sql = "select count(distinct hasID) as hasID,count(distinct probeID) as probeID,provinceID,cityID,platform,deviceProvider,fwVersion from fristFrameResponse group by provinceID,cityID,platform,deviceProvider,fwVersion ";
			DataFrame secondDataFrame = sqlContext.sql(sql);
			secondDataFrame.registerTempTable("cacheTerminalStateTable");
			sqlContext.cacheTable("cacheTerminalStateTable");
			allCountDimensions(sqlContext);
			saveCount(c,rEDIS_LOSE_TIME_LENGTH.value());
			sqlContext.uncacheTable("cacheTerminalStateTable");
		}
		
		public static void saveCount(Calendar c, int redisLoseTime) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			logger.info("总播放次数大小：" + play_count.size());
			if (play_count != null && play_count.size() > 0 && !PERIOD.equals("PEAK")) {
				IndexToRedis.allPlayToRedisOffline(play_count, time, redisLoseTime);
				logger.info("总播放次数写入Redis结束");
				play_count.clear();
			}
			
			logger.info("流用户数大小：" + play_user.size());
			if (play_user != null && play_user.size() > 0) {
				String insertSql = "INSERT INTO T_STREAMUSER (provinceID,platform,deviceProvider,fwVersion,cityID,parseTime,period,playUserCount)"
						+ " VALUES(?,?,?,?,?,?,?,?)";
				IndexToMysql.userToMysqlOffline(play_user, time, insertSql, PERIOD);
				logger.info("流用户数写入MySQL结束");
				play_user.clear();
			}
		}
		
		public static void allCountDimensions(SQLContext sqlContext) {
			int num = 0;
			while (num < Math.pow(2, columns.length)) {
				StringBuffer groupSb = new StringBuffer();
				StringBuffer selectSb = new StringBuffer();
				for (int i = 0; i < columns.length; i++) {
					// 二进制从低位到高位取值
					int bit = (num >> i) & 1;
					if (bit == 1) {
						selectSb.append(columns[i]).append(",");
						groupSb.append(columns[i]).append(",");
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

	private static void sparkSqlFirstFrameByDimension(SQLContext sqlContext) {
		String[] HASTYPE = PropHelper.getProperty("HASTYPE").split(",");
		String sqlEnd = "where hasType = ";
	    for (int i = 0; i < HASTYPE.length - 1; i++) {
	    	sqlEnd += HASTYPE[i] + " or hasType = ";
	    }
	    sqlEnd += HASTYPE[HASTYPE.length - 1];
	    String sqlCache = "select latency, provinceID, cityID, platform, deviceProvider, fwVersion from fristFrameResponse " + sqlEnd;
	    DataFrame filter = sqlContext.sql(sqlCache);
		filter.registerTempTable("fristFrameResponseCache");
		sqlContext.cacheTable("fristFrameResponseCache");
		int num = 0;
		while (num < Math.pow(2, columns.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();

			for (int i = 0; i < columns.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				// numStr += bit;
				if (bit == 1) {
					selectSb.append(columns[i]).append(",");
					groupSb.append(columns[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT avg(latency),")
					.append(selectSb.substring(0, selectSb.length() - 1))
					.append(" from fristFrameResponse where latency > 0 and latency <= "+PlayRequestOfflineAnalysis.maxLatency+" ");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());

			JavaRDD<String> rdd = df.toJavaRDD().map(new Function<Row, String>() {
				@Override
				public String call(Row row) {
					String kk = "#" + row.getString(1) + "#" + row.getString(2) + "#"
							+ row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5) + "\t" + row.getDouble(0);
					return kk;
				}
			});

			List<String> stateRow = rdd.collect();
			if (CollectionUtils.isNotEmpty(stateRow)) {
				for (String row : stateRow) {
					String[] rowArr = TAB.split(row,-1);
					String keyFirstFrame = Constant.FIRST_FRAME + rowArr[0];
					firstFrameMap.put(keyFirstFrame, Double.valueOf(nf.format(Double.valueOf(rowArr[1]))));
				}
			}
		}
		sqlContext.uncacheTable("fristFrameResponseCache");
	}
	
	private static void httpDownBytesAndFreezeByDimension(SQLContext sqlContext) {
		int num = 0;
		while (num < Math.pow(2, columns.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
			
			for (int i = 0; i < columns.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				if (bit == 1) {
					selectSb.append(columns[i]).append(",");
					groupSb.append(columns[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT sum(maxByte),sum(maxFreezeTime),sum(maxFreezeCount),sum(maxPlaySeconds),count(distinct hasID),").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM httpDownBytesTemp");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());
			
			Row[] rows = df.collect();
			if(rows != null && rows.length > 0){
				for(Row row : rows){
					String keyHttp = row.getString(5) + "#" + row.getString(6) + "#"
							+ row.getString(7) + "#" + row.getString(8) + "#" + row.getString(9);
					downBytesMap.put(keyHttp, Long.valueOf(row.getLong(0)));
					businessMap.put(keyHttp, row.getLong(1)+"#"+row.getLong(2)+"#"+row.getLong(3)+"#"+row.getLong(4));
				}
			}
		}
	}
	
	private static void httpDownBytesAndFreezeByCache(SQLContext sqlContext){
		DataFrame dFrame = sqlContext.sql("select maxByte,maxFreezeTime,maxFreezeCount,maxPlaySeconds,hasID,"+ columns[0]
				+ "," + columns[1] + "," + columns[2] + "," + columns[3] + "," + columns[4]+",hasType from httpDownBytesTemp");
		Row[] rows = dFrame.collect();
		if(rows != null && rows.length > 0){
			for (Row row : rows) {
				String key = row.getString(4)+"#"+row.getString(10)+"#"+PERIOD;
				//取出前一个维度最大量 格式：最大下载流量#卡顿时长#卡顿次数#播放时长
				String maxHis = IndexToRedis.getByKey(key);
				if(maxHis != null){
					String dimensionKey = row.getString(5) + "#" + row.getString(6) + "#" + row.getString(7) + "#" + row.getString(8) + "#" + row.getString(9);
					String[] maxHisArr = maxHis.split("\\#",-1);
					long maxPlaySecondsHis = Long.valueOf(maxHisArr[3]);
					if(tempByteMap.containsKey(dimensionKey)){
						long maxDownBytesHis = Long.valueOf(maxHisArr[0]);
						long maxFreezeTimeHis = Long.valueOf(maxHisArr[1]);
						long maxFreezeCountHis = Long.valueOf(maxHisArr[2]);
						
						String[] dimensionArr = tempByteMap.get(dimensionKey).split("\\#",-1);
						long dimensionDownBytes = Long.valueOf(dimensionArr[0]) + maxDownBytesHis;
						long dimensionFreezeTime = Long.valueOf(dimensionArr[1])+ maxFreezeTimeHis;
						long dimensionFreezeCount = Long.valueOf(dimensionArr[2]) + maxFreezeCountHis;
						long dimensionPlaySeconds = Long.valueOf(dimensionArr[3]) + maxPlaySecondsHis;
						
						tempByteMap.put(dimensionKey, dimensionDownBytes + "#" +dimensionFreezeTime + "#" +dimensionFreezeCount + "#" + dimensionPlaySeconds);
					}else{
						tempByteMap.put(dimensionKey,maxHis);
					}
					
					String bizDimensionKey = row.getString(5) + "#" + row.getString(6) + "#" + row.getString(7) + "#" + row.getString(8) + "#" + row.getString(9) + "#" + row.getString(10);
					if(tempBizMap.containsKey(bizDimensionKey)){
						tempBizMap.put(bizDimensionKey, tempBizMap.get(bizDimensionKey)+maxPlaySecondsHis);
					}else{
						tempBizMap.put(bizDimensionKey,maxPlaySecondsHis);
					}
				}
				//本次最新HasId的最大流量写入Redis
				IndexToRedis.setAndExpireTime(key, row.getLong(0) +"#"+row.getLong(1) +"#"+row.getLong(2) +"#"+row.getLong(3), DateUtil.getRedisExpireTime(PERIOD));
			}
		}
		
		if(tempByteMap.size() > 0){
			IndexToMysql.toDownBytesMysqlTemp(tempByteMap, columns);
		}
		
		//Key=省份#地市#牌照方#设备厂商#框架版本
		if(downBytesMap.size() > 0 && tempByteMap.size() > 0){
			for(Entry<String,String> entry : tempByteMap.entrySet()){
				String key = entry.getKey();
				String[] valueArr = entry.getValue().split("\\#",-1);
				downBytesMap.put(key, downBytesMap.get(key)- Long.valueOf(valueArr[0]));
				
				String[] businessArr = businessMap.get(key).split("\\#",-1);
				long businessFreezeTime = Long.valueOf(businessArr[0]) - Long.valueOf(valueArr[1]);
				long businessFreezeCount = Long.valueOf(businessArr[1]) - Long.valueOf(valueArr[2]);
				long businessPlaySeconds = Long.valueOf(businessArr[2]) - Long.valueOf(valueArr[3]);
				businessMap.put(key, businessFreezeTime + "#"+businessFreezeCount+ "#"+businessPlaySeconds+ "#"+businessArr[3]);
			}
		}
		tempByteMap.clear();
	}
	
	private static void sparkLagPhaseByDimension(SQLContext sqlContext,Calendar c) {
		String[] HASTYPE = PropHelper.getProperty("HASTYPE").split(",");
		String sqlEnd = "where t.hasType = ";
	    for (int i = 0; i < HASTYPE.length - 1; i++) {
	    	sqlEnd += HASTYPE[i] + " or t.hasType = ";
	    }
	    sqlEnd += HASTYPE[HASTYPE.length - 1];
		//获取数据中所有的设备id
		String userSql = "select ff.c1,ff.c2,ff.c3,ff.c4,ff.provinceID,ff.platform,ff.deviceProvider,ff.fwVersion,ff.cityID,ff.probeID from\n" +
				"(select case when SUM(t.freezeTime)/SUM(t.downSeconds) > 50000 then 1 else 0 end as c4,\n" +
				"case when SUM(t.freezeTime)/SUM(t.downSeconds) > 10000 and SUM(t.freezeTime)/SUM(t.downSeconds) <= 50000 then 1 else 0  end as c3,\n" +
				"case when SUM(t.freezeTime)/SUM(t.downSeconds)  > 0 and SUM(t.freezeTime)/SUM(t.downSeconds) <= 10000  then 1 else 0  end as c2,\n" +
				"case when SUM(t.freezeTime) = 0  then 1 else 0  end as c1,\n" +
				"t.provinceID as provinceID,t.platform as platform,t.deviceProvider as deviceProvider,t.fwVersion as fwVersion,t.cityID as cityID,\n" +
				"t.probeID as probeID\n"+
				" from fristFrameResponse t " + sqlEnd + " GROUP BY t.probeID,t.provinceID,t.platform,t.deviceProvider,t.fwVersion,t.cityID) ff";
				DataFrame allDimensionToUser = sqlContext.sql(userSql);
				allDimensionToUser.registerTempTable("cacheTable");
				sqlContext.cacheTable("cacheTable");		
				int num = 0;
				while (num < Math.pow(2, playColumns.length)) {
					StringBuffer groupSb = new StringBuffer();
					StringBuffer selectSb = new StringBuffer();
					String numStr = "";
					for (int i = 0; i < playColumns.length; i++) {
						// 二进制从低位到高位取值
						int bit = (num >> i) & 1;
						numStr += bit;
						if (bit == 1) {
							selectSb.append(playColumns[i]).append(",");
							groupSb.append(playColumns[i]).append(",");
						} else {
							selectSb.append("'ALL'").append(",");
						}
					}
					String sql = null;
					if(StringUtils.isBlank(groupSb.toString())){
						sql="select sum(c1),sum(c2),sum(c3),sum(c4),"+selectSb.substring(0,selectSb.length()-1)+" from cacheTable ";
					}else{
						sql="select sum(c1),sum(c2),sum(c3),sum(c4),"+selectSb.substring(0,selectSb.length()-1)+" from cacheTable  group by "+groupSb.substring(0, groupSb.length()-1);
					}
					num++;
					DataFrame allDimensionToTimes = sqlContext.sql(sql);
					JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
						private static final long serialVersionUID = 1L;
						@Override
						public String call(Row row) {
							String kk = Constant.LAG_PHASE + "#" + row.getString(4) + "#" + row.getString(5) + "#"
									+ row.getString(6) + "#" + row.getString(7) + "#" + row.getString(8) 
									+ "\t" + "VALUE" +"#"+ row.getLong(0) + "#" +row.getLong(1) + "#" +row.getLong(2) + "#" +row.getLong(3);
							return kk;
						}
					});
					List<String> stateRow = avgRDD.collect();
					toMap(stateRow);
				}
				saveLagPhase(c);
				sqlContext.uncacheTable("cacheTable");
		
	}
	
	public static void FirstFrameTemp(SQLContext sqlContext,Calendar c){
		DataFrame tempFrame =sqlContext.sql("select sum(latency) as latency,count(*) as quantity," + playColumns[0] + "," + playColumns[1] + "," + playColumns[2] + ","
				+ playColumns[3] + "," + playColumns[4] + " from fristFrameResponse where latency > 0 and latency <= 5000000 group by " + playColumns[0]
				+ "," + playColumns[1] + "," + playColumns[2] + "," + playColumns[3] + "," + playColumns[4]) ;
		tempFrame.registerTempTable("fristFrameResponseTemp");
		sqlContext.cacheTable("fristFrameResponseTemp");
		sparkSqlFirstFrameByDimension(sqlContext);
		saveFirst(c);
		sqlContext.uncacheTable("fristFrameResponseTemp");
	}
	
	public static void sparkSqlDownBytesAndFreezeByDimension(SQLContext sqlContext,Calendar c){
		DataFrame tempFrame =sqlContext.sql("select MAX(downBytes) as maxByte,MAX(freezeTime) as maxFreezeTime,MAX(freezeCount) as maxFreezeCount,MAX(playSeconds) as maxPlaySeconds," + columns[0] + "," + columns[1] + "," + columns[2] + ","
				+ columns[3] + "," + columns[4] + ",hasID,hasType from fristFrameResponse group by " + columns[0]
				+ "," + columns[1] + "," + columns[2] + "," + columns[3] + "," + columns[4] + ",hasID,hasType") ;
		tempFrame.registerTempTable("httpDownBytesTemp");
		sqlContext.cacheTable("httpDownBytesTemp");
		httpDownBytesAndFreezeByDimension(sqlContext);
		if("HOUR".equals(PERIOD)){
			httpDownBytesAndFreezeByCache(sqlContext);
		}
		saveHttp(c);
		saveFreeze(c);
		sqlContext.uncacheTable("httpDownBytesTemp");
	}
	
	public static void toMap(List<String> rowRate){
		if (rowRate == null || rowRate.size() == 0) {
			return;
		} else {
			for (String row : rowRate) {
				String[] rowArr = TAB.split(row,-1);
				nbMap.put(rowArr[0], rowArr[1]);
			}
		}
	}
	
	private static void saveLagPhase(Calendar c) {
		String time = DateUtil.getIndexTime(PERIOD,c);
		logger.info("用户卡顿分布 存储开始mapSize=" + nbMap.size());
		if (nbMap.size() > 0) {
			*//** 用户卡顿分布报表写入MySQL *//*
			IndexToMysql.distributeToMysqlOffline(nbMap, time, PERIOD);
			// 清空本次分析数据
			nbMap.clear();
		}
	}

	private static void saveFirst(Calendar c) {
		String time = DateUtil.getIndexTime(PERIOD, c);
		logger.info("首帧响应 存储开始mapSize=" + firstFrameMap.size());
		if (firstFrameMap != null && firstFrameMap.size() > 0) {
			*//** 首帧响应指标入mysql **//*
			IndexToMysql.toMysqlOnFirstFramIndex(firstFrameMap, time, PERIOD);
			firstFrameMap.clear();
		}
	}
	
	private static void saveHttp(Calendar c) {
		String time = DateUtil.getIndexTime(PERIOD, c);
		logger.info("HTTP流量 存储开始mapSize=" + downBytesMap.size());
		if (downBytesMap != null && downBytesMap.size() > 0) {
			*//** http流量指标入mysql **//*
			IndexToMysql.toMysqlDownBytesIndex(downBytesMap, time, PERIOD);
			downBytesMap.clear();
		}
	}
	
	private static void saveBiz(Calendar c) {
		logger.info("业务在线 存储开始mapSize=" + hm.size());
		if (hm != null && hm.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnOffLineData(hm, time, PERIOD);
			hm.clear();
		}
	}
	public static void saveFreeze(Calendar c) {
		String insertSql = null;
		String time = DateUtil.getIndexTime(PERIOD, c);
		logger.info("总体业务分布 存储开始Size=" + businessMap.size());
		if (businessMap != null && businessMap.size() > 0) {
			insertSql = "INSERT INTO T_BUSINESS_QUALITY (provinceID,cityID,platform,deviceProvider,fwVersion,parseTime,period,freezeTime,freezeCount,playSeconds,playCount)"
					+ " VALUES(?,?,?,?,?,?,?,?,?,?,?)";
			IndexToMysql.playFreezeToMysqlOffline(businessMap,PERIOD,insertSql,time);
			businessMap.clear();
		}
	}
	
	*//**
	 * @param sqlContext
	 *//*
	
	private static void sparkSqlBizByDimension(SQLContext sqlContext) {
		int num = 0;
		while (num < Math.pow(2, columns.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
			
			for (int i = 0; i < columns.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				// numStr += bit;
				if (bit == 1) {
					selectSb.append(columns[i]).append(",");
					groupSb.append(columns[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT sum(playSeconds),").append(selectSb.substring(0, selectSb.length() - 1))
				.append(",hasType").append(" FROM fristFrameResponse");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1)).append(",hasType");
			}else{
				sqlSb.append(" GROUP BY ").append("hasType");
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());
			JavaRDD<String> rdd = df.toJavaRDD().map(new Function<Row, String>() {
				@Override
				public String call(Row row) {
					String kk = row.getString(1) + "#" + row.getString(2) + "#"
							+ row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5) + "#" + row.getString(6) + "\t" + row.getLong(0);
					return kk;
				}
			});
			
			List<String> stateRow = rdd.collect();
			if (CollectionUtils.isNotEmpty(stateRow)) {
				for (String row : stateRow) {
					String[] rowArr = TAB.split(row,-1);
					hm.put(rowArr[0], Long.valueOf(rowArr[1]));
				}
			}
			
		}
	}
	
	private static void bizByCache(SQLContext sqlContext){
		if(tempBizMap.size() > 0){
			IndexToMysql.toBizMysqlTemp(tempBizMap, columns);
		}
		
		//Key=省份#地市#牌照方#设备厂商#框架版本#节目类型
		if(hm.size() > 0 && tempBizMap.size() > 0){
			for(Entry<String,Long> entry : tempBizMap.entrySet()){
				String key = entry.getKey();
				hm.put(key,hm.get(key) - entry.getValue() );
			}
		}
		tempBizMap.clear();
	}
}
*/