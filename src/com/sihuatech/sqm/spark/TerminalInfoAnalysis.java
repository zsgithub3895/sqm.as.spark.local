package com.sihuatech.sqm.spark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.InfoCountBean;
import com.sihuatech.sqm.spark.bean.TerminalInfo;
import com.sihuatech.sqm.spark.strategy.InfoStrategy;
import com.sihuatech.sqm.spark.util.DBConnection;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class TerminalInfoAnalysis {
	private static Logger logger = Logger.getLogger(TerminalInfoAnalysis.class);
	
	private static Map<String, TerminalInfo> infoMap = new HashMap<String, TerminalInfo>();
	private static List<InfoCountBean> infoCountList = new ArrayList<InfoCountBean>();
	
	static {
		if (!initInfoMap()) {
			System.exit(-1);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 3) {
			System.err.println("Usage: TerminalInfoAnalysis <zkGroup> <topic> <numStreams> <batchDuration>");
			System.exit(1);
		}
		
		// 获取参数
		String zkGroup = args[0];
		String topic = args[1];
		int numStreams = Integer.parseInt(args[2]); // 此参数为接收Topic的线程数，并非Spark分析的分区数
		final long batchDuration = Long.valueOf(args[3]);
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		logger.info("parameters...\n"
				+ "zkGroup:" + zkGroup + "\n"
				+ "topic:" + topic + "\n"
				+ "numStreams:" + numStreams + "\n"
				+ "zkQuorum:" + zkQuorum + "\n"
				+ "batchDuration:" + batchDuration + "\n");
		// 转换参数提供使用
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		// spark任务初始化
		SparkConf sparkConf = new SparkConf();
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.minutes(batchDuration));
		
		logger.info("创建 DStream");
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
				return InfoStrategy.validate(line);
			}
		});
		
		// 抽象 Bean
		JavaDStream<TerminalInfo> objs = filterLines.map(new Function<String, TerminalInfo>() {
			private static final long serialVersionUID = 1L;
			public TerminalInfo call(String line) {
				return InfoStrategy.buildBean(line);
			}
		});
		
		// 过滤 Null Object
		JavaDStream<TerminalInfo> filterObjs = objs.filter(new Function<TerminalInfo, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(TerminalInfo info) throws Exception {
				if (info == null) {
					return false;
				} else {
					return true;
				}
			}
		});
		
		filterObjs.foreachRDD(new VoidFunction<JavaRDD<TerminalInfo>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<TerminalInfo> rdd) {
				logger.info("RDD分区数：" + rdd.getNumPartitions());
				List<TerminalInfo> infos = rdd.collect();
				storeRedis(infos);
				
				JavaSparkContext jsc = JavaSparkContext.fromSparkContext(rdd.context());
				JavaRDD<TerminalInfo> rdd_new = jsc.parallelize(new ArrayList<TerminalInfo>(infoMap.values()));
				SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
				DataFrame df = sqlContext.createDataFrame(rdd_new, TerminalInfo.class);
				df.registerTempTable("TerminalInfo");
				df.persist(StorageLevel.MEMORY_AND_DISK_SER());
				long total = df.count();
				logger.info("设备信息记录数：" + total);
				if (total > 0) {
					// 总机顶盒数，包含时间的话入MySQL，存小时数据，只在整点入MySQL
					terminalCountDimensions(sqlContext);
				}
				df.unpersist();
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}
	
	private static void terminalCountDimensions(SQLContext sqlContext) {
		String sql = "select count(distinct deviceID) as probeCount, provinceID, cityID, platform,"
				+ "deviceProvider, fwVersion from TerminalInfo group by provinceID, cityID, platform,"
				+ "deviceProvider, fwVersion";
		DataFrame secondDataFrame = sqlContext.sql(sql);
		String tableName = "TerminalCountTmp";
		secondDataFrame.registerTempTable(tableName);
		sqlContext.cacheTable(tableName);
		logger.debug("分析开始");
		String result = "sum(probeCount)";
		int num = 0;
		String[] dimensions = InfoStrategy.RT_DIMENSIONS;
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
			Row[] rows = df.collect();
			for (Row row : rows) {
				InfoCountBean infoCount = new InfoCountBean();
				infoCount.setCount(row.getLong(0));
				infoCount.setProvinceID(row.getString(1));
				infoCount.setCityID(row.getString(2));
				infoCount.setPlatform(row.getString(3));
				infoCount.setDeviceProvider(row.getString(4));
				infoCount.setFwVersion(row.getString(5));
				infoCountList.add(infoCount);
			}
		}
		logger.info("分析结束，分析结果记录数：" + infoCountList.size());
		saveRedis();
		infoCountList.clear();
		sqlContext.uncacheTable(tableName);
	}
	
	public static void saveRedis() {
		if (infoCountList.size() > 0) {
			IndexToRedis.saveInfoCount(infoCountList);
		}
	}
	
	public static void saveMySQL(String time) {
		if (infoCountList.size() > 0) {
			IndexToMysql.saveInfoCount(infoCountList, time, "HOUR");
		}
	}
	
	private static boolean initInfoMap() {
		boolean flag = true;
		logger.info("启动时初始化设备信息数据，目前从MySQL获取数据");
		try {
			Connection conn = DBConnection.getConnection();
			String sql = "select '1',probeID,deviceID,deviceProvider,platform,provinceID,"
					+ "cityID,fwVersion,deviceModelID,deviceVersion,mode,userID,probeIP,mac,mac2,"
					+ "evVersion,managerName from TERMINAL_INFO";
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				TerminalInfo info = new TerminalInfo();
				info = new TerminalInfo();
				info.setLogType(rs.getString(1));
				info.setProbeID(rs.getString(2));
				info.setDeviceID(rs.getString(3));
				info.setDeviceProvider(rs.getString(4));
				info.setPlatform(rs.getString(5));
				info.setProvinceID(rs.getString(6));
				info.setCityID(rs.getString(7));
				info.setFwVersion(rs.getString(8));
				info.setDeviceModelID(rs.getString(9));
				info.setDeviceVersion(rs.getString(10));
				info.setMode(rs.getString(11));
				info.setUserID(rs.getString(12));
				info.setProbeIP(rs.getString(13));
				info.setMac(rs.getString(14));
				info.setMac2(rs.getString(15));
				info.setEvVersion(rs.getString(16));
				info.setManagerName(rs.getString(17));
				if (!info.equals(infoMap.get(info.getDeviceID()))) {
					infoMap.put(info.getDeviceID(), info);
				}
			}
		} catch (SQLException e) {
			logger.error("初始化设备信息失败！");
			flag = false;
		}
		return flag;
	}
	
	private static void storeRedisAndMySQL(List<TerminalInfo> infos) {
		List<TerminalInfo> list_after = new ArrayList<TerminalInfo>();
		for (TerminalInfo info : infos) {
			if (!infoMap.containsKey(info.getDeviceID())
					|| !StringUtils.equals(info.toString(),
							infoMap.get(info.getDeviceID()).toString())) {
				infoMap.put(info.getDeviceID(), info);
				// 写入Redis暂时保留以probeID为key
				IndexToRedis.set(info.getProbeID(), info.toString());
				list_after.add(info);
			}
		}
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			conn.setAutoCommit(false);
			String insertOrUpdate = new StringBuilder("INSERT INTO TERMINAL_INFO")
					.append("(logType,probeID,deviceID,deviceProvider,platform,provinceID,cityID,fwVersion,")
					.append("deviceModelID,deviceVersion,mode,userID,probeIP,mac,mac2,evVersion,managerName)")
					.append(" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ")
					.append("logType=VALUES(logType),probeID=VALUES(probeID),deviceID=VALUES(deviceID),deviceProvider=VALUES(deviceProvider),")
					.append("platform=VALUES(platform),provinceID=VALUES(provinceID),cityID=VALUES(cityID),fwVersion=VALUES(fwVersion),")
					.append("deviceModelID=VALUES(deviceModelID),deviceVersion=VALUES(deviceVersion),mode=VALUES(mode),userID=VALUES(userID),")
					.append("probeIP=VALUES(probeIP),mac=VALUES(mac),mac2=VALUES(mac2),evVersion=VALUES(evVersion),managerName=VALUES(managerName)")
					.toString();
			stmt = conn.prepareStatement(insertOrUpdate);
			int count = 0;
			for(TerminalInfo info : list_after) {
				stmt.setInt(1, Integer.parseInt(info.getLogType()));
				stmt.setString(2, info.getProbeID());
				stmt.setString(3, info.getDeviceID());
				stmt.setString(4, info.getDeviceProvider());
				stmt.setString(5, info.getPlatform());
				stmt.setString(6, info.getProvinceID());
				stmt.setString(7, info.getCityID());
				stmt.setString(8, info.getFwVersion());
				stmt.setString(9, info.getDeviceModelID());
				stmt.setString(10, info.getDeviceVersion());
				stmt.setString(11, info.getMode());
				stmt.setString(12, info.getUserID());
				stmt.setString(13, info.getProbeIP());
				stmt.setString(14, info.getMac());
				stmt.setString(15, info.getMac2());
				stmt.setString(16, info.getEvVersion());
				stmt.setString(17, info.getManagerName());
				stmt.addBatch();
				count++;
				if (count % 1000 == 0) {
					stmt.executeBatch();
					conn.commit();
					count = 0;
				}
			}
			if (list_after.size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
				count = 0;
			}
		} catch(Exception e) {
			
		}
	}
	
	private static void storeRedis(List<TerminalInfo> infos) {
		for (TerminalInfo info : infos) {
			if (!infoMap.containsKey(info.getDeviceID())
					|| !StringUtils.equals(info.toString(),
							infoMap.get(info.getDeviceID()).toString())) {
				infoMap.put(info.getDeviceID(), info);
				// 写入Redis暂时保留以probeID为key
				IndexToRedis.set(info.getProbeID(), info.toString());
			}
		}
	}
}
