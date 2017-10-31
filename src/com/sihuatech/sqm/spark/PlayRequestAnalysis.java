package com.sihuatech.sqm.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.PlayResponseLog;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class PlayRequestAnalysis {
	//redis失效时间
	private static  int REDIS_TIME_LENGTH = 7*24*60*60;
	private static Logger logger = Logger.getLogger(PlayRequestAnalysis.class);
	private static final String PERIOD = "RT"; // 代表实时RealTime
	private static final Pattern TAB = Pattern.compile("\t");
	//业务在线数据指标
	private static HashMap<String,String> hm = new HashMap<String,String>();
	private static HashMap<String, Long> hmOnline = new HashMap<String, Long>();
	private static HashMap<String, Long> downBytesMap = new HashMap<String, Long>();
	//用户卡顿分布分析
	private static HashMap<String, String> lagPhaseMap = new HashMap<String, String>();
	//http流量-临时Map
	private static Map<String,Long> tempByteMap = new HashMap<String, Long>();
	//业务在线-临时Map
	private static Map<String,Long> tempBizMap = new HashMap<String, Long>();
	//流用户
	private static Map<String,Long> streamUserCountMap = new HashMap<String, Long>();
	//播放次数
	private static Map<String,Long> playCountMap = new HashMap<String, Long>();
	private static String[] dimension = {"provinceID", "cityID"};
	private static final String[] DIMENSION_5D = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion" };
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 3) {
			System.err.println("Usage: PlayRequestAnalysis <zkGroup> <topic> <numStreams>");
			System.exit(1);
		}
		// 获取参数
		String zkGroup = args[0];
		String topic = args[1];
		String numStreamsS = args[2]; // 此参数为接收Topic的线程数，并非Spark分析的分区数
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		final String batchDurationS = PropHelper.getProperty("FIRST_FRAME_TIME");
		long maxLatency = Long.valueOf(PropHelper.getProperty("LATENCY_MAX_VALUE"));
		maxLatency = maxLatency == 0 ? 50000000 : maxLatency;
		logger.info("+++[PlayReq]parameters...\n"
				+ "zkGroup:" + zkGroup + "\n"
				+ "topic:" + topic + "\n"
				+ "numStreams:" + numStreamsS + "\n"
				+ "zkQuorum:" + zkQuorum + "\n"
				+ "batchDuration:" + batchDurationS + "\n"
				+ "maxLatency:" + maxLatency + "\n");
		// 转换参数提供使用
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		int numStreams = Integer.parseInt(numStreamsS);
		long batchDuration = Long.valueOf(batchDurationS);
		// spark任务初始化
		SparkConf sparkConf = new SparkConf().setAppName("PlayRequestAnalysis");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.minutes(batchDuration));
		//广播变量
		final Broadcast<Long> LATENCY_MAX_VALUE = ctx.broadcast(maxLatency);
		final Broadcast<Integer> REDIS_LOSE_TIME_LENGTH = ctx.broadcast(REDIS_TIME_LENGTH);
		logger.info("+++[PlayReq]开始读取内容：");
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
		// 过滤不符合规范的记录
		JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 40) {
					return false;
				}else if (!"3".equals(lineArr[0])) {
					return false;
				}
				return true;
			}
		});
		filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd, Time time) {
				logger.info("+++[PlayReq]播放请求开始!");
				logger.info("+++[PlayReq]RDD分区数：" + rdd.getNumPartitions());
				SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
				JavaRDD<PlayResponseLog> rowRDD = rdd.map(new Function<String, PlayResponseLog>() {
					private static final long serialVersionUID = 1L;
					
					public PlayResponseLog call(String word) {
						PlayResponseLog playResponseLog = null;
						if (StringUtils.isNotBlank(word)) {
							String[] fields = word.split(String.valueOf((char)0x7F),-1);
							try {
								if (fields.length > 40) {
									playResponseLog = new PlayResponseLog(fields[5], fields[6],
											fields[7], fields[9], fields[10],
											NumberUtils.toLong(fields[16], 0),
											NumberUtils.toLong(fields[17], 0), fields[18],
											fields[4], fields[4] + fields[1], fields[23], fields[8],
											NumberUtils.toLong(fields[12], 0),
											NumberUtils.toLong(fields[24], 0),
											NumberUtils.toLong(fields[14], 0),
											fields[23].substring(0, 12));
								}
							} catch (ArrayIndexOutOfBoundsException e) {
								logger.error("=====================首帧响应指标日志中latency或downBytes字段为空，舍弃", e);
							}
						}
						return playResponseLog;
					}
				});
				// 校验日志，过滤不符合条件的记录
				JavaRDD<PlayResponseLog> playResponseLogs = rowRDD.filter(new Function<PlayResponseLog, Boolean>() {
					@Override
					public Boolean call(PlayResponseLog play) throws Exception {
						if (null==play) {
							return false;
						}
						return true;
					}
				});
				String periodTime = DateUtil.getRTPeriodTime();
				DataFrame wordsDataFrame = sqlContext.createDataFrame(playResponseLogs, PlayResponseLog.class);
				wordsDataFrame.registerTempTable("PlayResponseLogs");
				long total0 = wordsDataFrame.count();
				logger.info("+++[PlayReq]播放请求日志 -过滤前-记录数：" + total0);
				String filterSql = "select distinct p.* from PlayResponseLogs p,(select hasID,max(KPIUTCSec) KPIUTCSec from  PlayResponseLogs group by hasID ) b where p.hasID=b.hasID and p.KPIUTCSec=b.KPIUTCSec ";
				  
				DataFrame filterDataFrame = sqlContext.sql(filterSql);
				filterDataFrame.registerTempTable("PlayResponseLog");
				filterDataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER());
				long total = filterDataFrame.count();
				logger.info("+++[PlayReq]播放请求日志-过滤后-记录数：" + total);
				if(total > 0){
					//流用户数 和 播放次数
					streamAndPlayByDimension(sqlContext);
					saveStreamAndPlay(periodTime);
				    //首帧响应指标，暂时没有首帧的实时数据展现，隐藏此指标的实时计算
					indexByDimension(sqlContext, LATENCY_MAX_VALUE.value());
					saveIndex(periodTime);
					
					//HTTP流量DownBytes字段累积值
					downBytesByDimension(sqlContext);
					saveDownBytes(periodTime);
					
					// 处理节目类型各维度总播放时长
					bizOnlineByDimension(sqlContext);
					bizByCache(sqlContext);
					saveLong(periodTime);
					
					//计算卡顿分布
					sparkLagPhaseByDimension(sqlContext);
					saveLagPhase(periodTime, REDIS_LOSE_TIME_LENGTH);
				}
				filterDataFrame.unpersist();
			}

		});
		jssc.start();
		jssc.awaitTermination();
	}
	

	protected static void saveStreamAndPlay(String time) {
		logger.info("流用户数 存储开始mapSize=" + streamUserCountMap.size());
		if (streamUserCountMap != null && streamUserCountMap.size() > 0) {
			IndexToRedis.streamUserCountMap(streamUserCountMap,time);
			streamUserCountMap.clear();
		}
		logger.info("播放次数 存储开始mapSize=" + playCountMap.size());
		if (playCountMap != null && playCountMap.size() > 0) {
			IndexToRedis.playCountMapToRedis(playCountMap,time);
			playCountMap.clear();
		}
		
	}


	protected static void saveLagPhase(String time,Broadcast<Integer> rEDIS_LOSE_TIME_LENGTH) {
		logger.info("用户卡顿分布 存储开始mapSize=" + lagPhaseMap.size());
		if (lagPhaseMap != null && lagPhaseMap.size() > 0) {
			/** 用户卡顿分布 入Redis **/
			IndexToRedis.lagPhaseMapToRedis(lagPhaseMap,time,rEDIS_LOSE_TIME_LENGTH.value());
			lagPhaseMap.clear();
		}
		
	}
	
	private static void saveDownBytes(String time){
		logger.info("HTTP流量 存储开始mapSize=" + downBytesMap.size());
		if (downBytesMap != null && downBytesMap.size() > 0) {
			/**HTTP流量指标入mysql **/
			IndexToRedis.downBytesToRedis(downBytesMap,time,PERIOD);
			downBytesMap.clear();
		}

	}
	
	private static void saveLong(String time) {
		logger.info("业务在线 存储开始mapSize=" + hmOnline.size());
		if (hmOnline != null && hmOnline.size() > 0) {
			IndexToRedis.toRedisOnLineData(hmOnline, time, PERIOD);
			hmOnline.clear();
		}
	}
	//流用户数 和 播放次数
	protected static void streamAndPlayByDimension(SQLContext sqlContext) {
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
				sql = "select count(distinct probeID), count(distinct hasID)," + selectSb.substring(0, selectSb.length()-1) + "  from PlayResponseLog ";
			} else {
				sql = "select count(distinct probeID), count(distinct hasID)," + selectSb.substring(0, selectSb.length()-1) + "  from PlayResponseLog  group by " + groupSb.substring(0, groupSb.length() - 1);
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String call(Row row) {
					String kk = row.getString(2) + "#" + row.getString(3)
							+ "\t" + row.getLong(0) + "#" + row.getLong(1);
					return kk;
				}
			});
			List<String> rowRate = avgRDD.collect();
			
			if (rowRate == null || rowRate.size() == 0) {
				return;
			} else {
				for (String row : rowRate) {
					String[] rowArr = TAB.split(row,-1);
					String[] data = rowArr[1].split("\\#",-1);
					streamUserCountMap.put(Constant.ALL_USER + "#" + rowArr[0], Long.valueOf(data[0]));
					playCountMap.put(Constant.ALL_PLAY + "#" + rowArr[0], Long.valueOf(data[1]));
				}
			}
		}
	}
	//计算卡顿分布维度（地市）
	private static void sparkLagPhaseByDimension(SQLContext sqlContext) {
		String[] HASTYPE = PropHelper.getProperty("HASTYPE").split(",");
		String sqlEnd = "where t.hasType = ";
	    for (int i = 0; i < HASTYPE.length - 1; i++) {
	    	sqlEnd += HASTYPE[i] + " or t.hasType = ";
	    }
	    sqlEnd += HASTYPE[HASTYPE.length - 1];
		//获取数据中所有的设备id
		String userSql = "select ff.c1,ff.c2,ff.c3,ff.c4,ff.provinceID,ff.cityID,ff.probeID from\n" +
				"(select case when SUM(t.freezeTime)/SUM(t.downSeconds) > 50000 then 1 else 0 end as c4,\n" +
				"case when SUM(t.freezeTime)/SUM(t.downSeconds) > 10000 and SUM(t.freezeTime)/SUM(t.downSeconds) <= 50000 then 1 else 0  end as c3,\n" +
				"case when SUM(t.freezeTime)/SUM(t.downSeconds)  > 0 and SUM(t.freezeTime)/SUM(t.downSeconds) <= 10000  then 1 else 0  end as c2,\n" +
				"case when SUM(t.freezeTime) = 0  then 1 else 0  end as c1,\n" +
				"t.provinceID as provinceID,t.cityID as cityID,\n" +
				"t.probeID as probeID\n"+
				" from PlayResponseLog t " + sqlEnd + " GROUP BY t.probeID,t.provinceID,t.cityID) ff";
				DataFrame allDimensionToUser = sqlContext.sql(userSql);
				allDimensionToUser.registerTempTable("cacheTable");
				sqlContext.cacheTable("cacheTable");
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
						sql="select sum(c1),sum(c2),sum(c3),sum(c4),count(probeID)," +selectSb.substring(0,selectSb.length()-1)+" from cacheTable ";
					}else{
						sql="select sum(c1),sum(c2),sum(c3),sum(c4),count(probeID),"+selectSb.substring(0,selectSb.length()-1)+" from cacheTable  group by "+groupSb.substring(0, groupSb.length()-1);
					}
					num++;
					DataFrame allDimensionToTimes = sqlContext.sql(sql);
					JavaRDD<String> avgRDD = allDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
						private static final long serialVersionUID = 1L;
						@Override
						public String call(Row row) {
							String kk = Constant.LAG_PHASE + "#" + row.getString(5) + "#" + row.getString(6)
									+ "\t" + row.getLong(0) + "#" +row.getLong(1) + "#" +row.getLong(2) + "#" +row.getLong(3) + "#" + row.getLong(4);
							return kk;
						}
					});
					List<String> rowRate = avgRDD.collect();
					
					if (rowRate == null || rowRate.size() == 0) {
						return;
					} else {
						for (String row : rowRate) {
							String[] rowArr = TAB.split(row,-1);
							lagPhaseMap.put(rowArr[0], rowArr[1]);
						}
					}
				}
				sqlContext.uncacheTable("cacheTable");
	}
	
	
	
	private static void bizOnlineByDimension(SQLContext sqlContext) {
		int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
			
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				// numStr += bit;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("select sum(playSeconds),").append(selectSb.substring(0, selectSb.length() - 1))
				.append(",hasType").append(" FROM PlayResponseLog ");
			if (groupSb.length() > 0) {
				sqlSb.append(" group by ").append(groupSb.substring(0, groupSb.length() - 1)).append(",hasType");
			}else{
				sqlSb.append(" group by ").append("hasType");
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());
			Row[] stateRow = df.collect();
			if (null == stateRow || stateRow.length == 0) {
				return;
			} else {
				for (Row row : stateRow) {
					String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3);
					hmOnline.put(key,row.getLong(0));
				}
			}
		}
	}
	
	private static void bizByCache(SQLContext sqlContext){
		if(tempBizMap.size() > 0){
			IndexToMysql.toBizMysqlTempRT(tempBizMap, dimension);
		}
		//Key=省份#地市#节目类型
		if(hmOnline.size() > 0 && tempBizMap.size() > 0){
			for(Entry<String,Long> entry : tempBizMap.entrySet()){
				String key = entry.getKey();
				hmOnline.put(key, hmOnline.get(key)- entry.getValue());
			}
		}
		tempBizMap.clear();
	}
	
	private static void indexByDimension(SQLContext sqlContext, long maxLatency) {
		String[] HASTYPE = PropHelper.getProperty("HASTYPE").split(",");
		String sqlEnd = "where hasType = ";
	    for (int i = 0; i < HASTYPE.length - 1; i++) {
	    	sqlEnd += HASTYPE[i] + " or hasType = ";
	    }
	    sqlEnd += HASTYPE[HASTYPE.length - 1];
	    String sqlCache = "select latency, provinceID, cityID, KPIUTCSec12 from PlayResponseLog " + sqlEnd;
	    DataFrame filter = sqlContext.sql(sqlCache);
		filter.registerTempTable("PlayResponseLogCache");
		sqlContext.cacheTable("PlayResponseLogCache");
		int num = 0;
		while (num < Math.pow(2, DIMENSION_5D.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			String numStr = "";
			for (int i = 0; i < DIMENSION_5D.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				numStr += bit;
				if (bit == 1) {
					selectSb.append(DIMENSION_5D[i]).append(",");
					groupSb.append(DIMENSION_5D[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			String sql = null;
			if (StringUtils.isBlank(groupSb.toString())) {
				sql = "select count(*),sum(latency)," + selectSb.substring(0, selectSb.length()-1) + ",KPIUTCSec12  from PlayResponseLogCache where latency > 0  and latency <= " + maxLatency + " group by KPIUTCSec12";
			} else {
				sql = "select count(*),sum(latency)," + selectSb.substring(0, selectSb.length()-1) + ",KPIUTCSec12  from PlayResponseLogCache where latency > 0  and latency <= " + maxLatency + " group by KPIUTCSec12," + groupSb.substring(0, groupSb.length() - 1);
			}
			num++;
			DataFrame allDimensionToTimes = sqlContext.sql(sql);
			Row[] row = allDimensionToTimes.collect();
			indexMap(row);
		}
		sqlContext.uncacheTable("PlayResponseLogCache");
	}
	
	public static void indexMap(Row[] rowRate) {
		if (rowRate == null || rowRate.length == 0) {
			return;
		} else {
			for (Row row : rowRate) {
				String key = row.getString(2) + "#" + row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5) + "#" + row.getString(6) + "\t" + row.getString(7);
				String value = row.getLong(0) + "#" + row.getLong(1);
				hm.put(key,value);
			}
		}
	}
	
	private static void saveIndex(String time) {
		logger.info("首帧响应 存储开始mapSize=" + hm.size());
		if (hm != null && hm.size() > 0) {
			/** 首帧响应指标入Redis **/
			IndexToRedis.hmToRedis(hm);
			hm.clear();
		}
	}
	
	private static void downBytesByDimension(SQLContext sqlContext) {
		DataFrame tempFrame = sqlContext.sql("select " + dimension[0] + "," + dimension[1]
				+ ",hasID,hasType,MAX(downBytes) as maxDownBytes,MAX(playSeconds) as maxPlaySeconds from PlayResponseLog group by "
				+ dimension[0] + "," + dimension[1] + ",hasID,hasType");
		tempFrame.registerTempTable("httpDownBytesTemp");
		sqlContext.cacheTable("httpDownBytesTemp");
		httpDownBytesByDimension(sqlContext);
		httpDownBytesByCache(sqlContext, PERIOD);
		sqlContext.uncacheTable("httpDownBytesTemp");
	}
	private static void httpDownBytesByDimension(SQLContext sqlContext) {
		int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer selectSb = new StringBuffer();
			StringBuffer groupSb = new StringBuffer();
			
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				// numStr += bit;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			StringBuffer sqlSb = new StringBuffer();
			sqlSb.append("SELECT sum(maxDownBytes),").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM httpDownBytesTemp");
			if (groupSb.length() > 0) {
				sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
			}
			num++;
			DataFrame df = sqlContext.sql(sqlSb.toString());
			
			Row[] rows = df.collect();
			if(rows != null && rows.length > 0){
				for (Row row : rows) {
					String key = Constant.DOWN_BYTES + "#" + row.getString(1) + "#" + row.getString(2);
					downBytesMap.put(key, row.getLong(0));
				}
			}
		}
	}
	
	private static void httpDownBytesByCache(SQLContext sqlContext,String period){
		DataFrame dFrame = sqlContext.sql("select " + dimension[0] + "," + dimension[1]
				+ ",hasID,hasType,maxDownBytes,maxPlaySeconds from httpDownBytesTemp");
		Row[] rows = dFrame.collect();
		if(rows != null && rows.length > 0){
			for (Row row : rows) {
				// 不同的hasid+hastype
				String key = row.getString(2) + "#" + row.getString(3) + "#" + period;
				//取出前一个维度最大量 格式：最大下载流量#播放时长
				// 计算本次出现的hasid+hastype在上一个时间周期的数据总和
				String maxHis = IndexToRedis.getByKey(key);
				if(maxHis != null){
					String[] maxHisArr = maxHis.split("\\#",-1);
					long maxDownBytesHis = Long.valueOf(maxHisArr[0]);
					long maxPlaySecondsHis = Long.valueOf(maxHisArr[1]);
					
					String dimensionKey = row.getString(0) + "#" + row.getString(1);
					if(tempByteMap.containsKey(dimensionKey)){
						tempByteMap.put(dimensionKey, tempByteMap.get(dimensionKey).longValue() + maxDownBytesHis);
					}else{
						tempByteMap.put(dimensionKey, maxDownBytesHis);
					}
					
					String bizDimensionKey = row.getString(0) + "#" + row.getString(1) + "#" + row.getString(3);
					if(tempBizMap.containsKey(bizDimensionKey)){
						tempBizMap.put(bizDimensionKey, tempBizMap.get(bizDimensionKey) + maxPlaySecondsHis);
					}else{
						tempBizMap.put(bizDimensionKey, maxPlaySecondsHis);
					}
				}
				IndexToRedis.setAndExpireTime(key, row.getLong(4)+"#"+row.getLong(5), DateUtil.getRedisExpireTime("R"));
			}
		}
		if(tempByteMap.size() > 0){
			IndexToMysql.toDownBytesMysqlTempRT(tempByteMap, dimension);
		}
		
		//Key=省份#地市
		if(downBytesMap.size() > 0 && tempByteMap.size() > 0){
			for(Entry<String,Long> entry : tempByteMap.entrySet()){
				String key = Constant.DOWN_BYTES + "#" + entry.getKey();
				Long value = entry.getValue();
				downBytesMap.put(key, downBytesMap.get(key)-value);
			}
		}
		tempByteMap.clear();
	}
}