package com.sihuatech.sqm.spark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.sihuatech.sqm.spark.bean.InfoCountBean;
import com.sihuatech.sqm.spark.util.DBConnection;

public class IndexToMysql {
	static DecimalFormat df = new DecimalFormat("0.0000");
	private static Logger logger = Logger.getLogger(IndexToMysql.class);

	public static void toMysqlOnImageIntex(Map<String, String> hm, String time,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append(
					"INSERT INTO T_IMAGE_INDEX(id,provinceid,cityid,platform,deviceProvider,fwVersion,fastrate,succRate,parseTime,period)" + " VALUES(null,?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setDouble(6, Double.valueOf(vArr[0]));
					stmt.setDouble(7, Double.valueOf(vArr[1]));
					stmt.setString(8, time);
					stmt.setString(9, period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void toMysqlDownBytesIndex(Map<String, Long> downBytesMap, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_DOWN_BYTES_INDEX(provinceID,cityID,platform,deviceProvider,fwVersion,parseTime,downBytes,period)"
					+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : downBytesMap.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, time);
					stmt.setDouble(7, en.getValue());
					stmt.setString(8,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (downBytesMap.entrySet() != null && downBytesMap.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	
	public static void toMysqlOnFirstFramIndex(Map<String, Double> firstFrameMap, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_FIRST_FRAME_INDEX(provinceID,cityID,platform,deviceProvider,fwVersion,parseTime,latency,period)"
					+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : firstFrameMap.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setString(6, time);
					stmt.setDouble(7, Double.parseDouble(df.format(en.getValue())));
					stmt.setString(8,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (firstFrameMap.entrySet() != null && firstFrameMap.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}

	public static void playSuccRateToMysqlOffline(HashMap<String, Double> hm, String period, String time) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_PLAY_SUCC_RATE(provinceID,cityID,platform,deviceProvider,fwVersion,parseTime,playSuccRate,period)"
							+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, time);
					stmt.setDouble(7, en.getValue());
					stmt.setString(8,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	

	/**离线自然缓冲率******************************************************/
	public static void playCountToMysqlOffline(HashMap<String, Double> hm,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_PLAY_LAG(provinceID,platform,deviceProvider,fwVersion,parseTime,playCountRate,period,playCount,cityID)"
							+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[3]);
					stmt.setString(3, sArr[4]);
					stmt.setString(4, sArr[5]);
					stmt.setString(5, sArr[6]);
					stmt.setDouble(6, en.getValue());
					stmt.setString(7, period);
					if (StringUtils.isNotBlank(sArr[7])) {
						stmt.setLong(8, Long.valueOf(sArr[7]));
					}else{
						stmt.setLong(8, 0);
					}
					stmt.setString(9, sArr[2]);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	/**离线卡顿用户率******************************************************/
	public static void userToMysqlOffline(HashMap<String, Double> hm,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_USER_LAG(provinceID,platform,deviceProvider,fwVersion,parseTime,userRate,period,faultUser,cityID)"
							+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[3]);
					stmt.setString(3, sArr[4]);
					stmt.setString(4, sArr[5]);
					stmt.setString(5, sArr[6]);
					stmt.setDouble(6, en.getValue());
					stmt.setString(7, period);
					if (StringUtils.isNotBlank(sArr[7])) {
						stmt.setLong(8, Long.valueOf(sArr[7]));
					}else{
						stmt.setLong(8, 0);
					}
					stmt.setString(9, sArr[2]);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}	

	public static void toMysqlOnOffLineData(Map<String, Long> hm, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_BUSINESS_ONLINE_DATA(provinceID,cityId,platform,deviceProvider,fwVersion,hasType,playSum,parseTime,period)"
					+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, sArr[5]);
					stmt.setLong(7, en.getValue());
					stmt.setString(8,time);
					stmt.setString(9,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	
	public static void userToMysqlOffline(HashMap<String, Long> hm,String time,String insertSql,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[1]);
					stmt.setString(6, time);
					stmt.setString(7, period);
					stmt.setDouble(8, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 1000 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 1000 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	
	public static void faultToMysqlOffline(Row[] rows,String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_FAULT(provinceID,cityID,platform,deviceProvider,fwVersion,exportId,parseTime,period,faultCount)"
							+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Row row : rows) {
				stmt.setString(1, row.getString(1));
				stmt.setString(2, row.getString(2));
				stmt.setString(3, row.getString(3));
				stmt.setString(4, row.getString(4));
				stmt.setString(5, row.getString(5));
				stmt.setString(6,row.getString(6));
				stmt.setString(7,time);
				stmt.setString(8, period);
				stmt.setLong(9, row.getLong(0));
				stmt.addBatch();
				count++;
				if (count % 100 == 0) {
					stmt.executeBatch();
					conn.commit();
					count = 0;
				}
			}
			if (rows != null && rows.length > 0 && rows.length % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void faultToMysql(HashMap<String, Long> hm,String time) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_FAULT(provinceID,platform,deviceProvider,fwVersion,exportId,parseTime,period,faultCount)"
							+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setString(6,time);
					stmt.setString(7, "1");
					stmt.setLong(8,en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void distributeToMysqlOffline(HashMap<String, String> hm,String time,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_USER_LAG_DISTRIBUTION(provinceID,platform,deviceProvider,fwVersion,cityID,parseTime,period,"
							+ "greenUser,blueUser,yellowUser,redUser) VALUES(?,?,?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setString(6, time);
					stmt.setString(7, period);
					stmt.setLong(8, Long.valueOf(vArr[1]));
					stmt.setLong(9, Long.valueOf(vArr[2]));
					stmt.setLong(10, Long.valueOf(vArr[3]));
					stmt.setLong(11, Long.valueOf(vArr[4]));
					stmt.addBatch();
					count++;
					if (count % 1000 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void playFreezeToMysqlOffline(Map<String,String> hm, String period, String insertSql,String time) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			if(hm != null && hm.size() > 0){
				for (Entry<String,String> en: hm.entrySet()) {
					String[] sArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, time);
					stmt.setString(7, period);
					stmt.setLong(8, Long.valueOf(vArr[0])/1000/1000);
					stmt.setLong(9, Long.valueOf(vArr[1]));
					stmt.setLong(10, Long.valueOf(vArr[2]));
					stmt.setLong(11, Long.valueOf(vArr[3]));
					stmt.addBatch();
					count++;
					if (count % 1000 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
				if (hm.size() % 1000 != 0) {
					stmt.executeBatch();
					conn.commit();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void causeToMysqlOffline(HashMap<String, String> hm,String time, String period ) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_PLAYUSER_CAUSE(provinceID,platform,deviceProvider,cityId,fwVersion,parseTime,period,causeType1,causeType2,causeType3,causeType4,causeSum)"
							+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] kArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, kArr[1]);
					stmt.setString(2, kArr[3]);
					stmt.setString(3, kArr[4]);
					stmt.setString(4, kArr[2]);
					stmt.setString(5, kArr[5]);
					stmt.setString(6,time);
					stmt.setString(7, period);
					stmt.setLong(8, Long.parseLong(vArr[2]));
					stmt.setLong(9, Long.parseLong(vArr[3]));
					stmt.setLong(10, Long.parseLong(vArr[4]));
					stmt.setLong(11, Long.parseLong(vArr[5]));
					stmt.setLong(12, Long.parseLong(vArr[1]));
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void toMysqlOnEPGDistribution(Map<String, String> hm, String time,String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_EPG_DISTRIBUTION(id,provinceid,cityid,platform,deviceProvider,fwVersion,pagetype,range1,range2,range3,range4,parseTime,period)"
					+ " VALUES(null,?,?,?,?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] kArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, kArr[0]);
					stmt.setString(2, kArr[1]);
					stmt.setString(3, kArr[2]);
					stmt.setString(4, kArr[3]);
					stmt.setString(5, kArr[4]);
					stmt.setString(6, kArr[5]);
					stmt.setLong(7, Long.valueOf(vArr[0]));
					stmt.setLong(8, Long.valueOf(vArr[1]));
					stmt.setLong(9, Long.valueOf(vArr[2]));
					stmt.setLong(10, Long.valueOf(vArr[3]));
					stmt.setString(11, time);
					stmt.setString(12, period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void spHealthToMysql(HashMap<String, Long> map,String parseTime, String period) {
		if (map == null || map.size() == 0) {
			return;
		}
		
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_SLICE_HEALTH(worst,bad,normal,great,parseTime,period)"
							+ " VALUES(?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			stmt.setLong(1, map.get("worst"));
			stmt.setLong(2, map.get("bad"));
			stmt.setLong(3, map.get("normal"));
			stmt.setLong(4, map.get("great"));
			stmt.setString(5, parseTime);
			stmt.setString(6, period);
			stmt.addBatch();
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	public static void toDownBytesMysqlTemp(Map<String, String> hm,String[] columns) {
		Connection conn = null;
		PreparedStatement stmt = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder createSql = new StringBuilder();
			createSql.append("CREATE TEMPORARY TABLE `temp_byte` (");
			createSql.append("`provinceID` varchar(30),`cityID` varchar(30),");
			createSql.append("`platform` varchar(30),`deviceProvider` varchar(30),  ");
			createSql.append("`fwVersion` varchar(30),`downBytes` double(18,4), ");
			createSql.append("`freezeTime` bigint(20),`freezeCount` bigint(20), ");
			createSql.append("`playSeconds` bigint(20))");
			st = conn.createStatement();
			st.execute(createSql.toString());
			
			//Key=省份#地市#牌照方#设备厂商#框架版本,非ALL
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO temp_byte(provinceID,cityID,platform,deviceProvider,fwVersion,downBytes,freezeTime,freezeCount,playSeconds)"
					+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, String> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					String[] vArr = en.getValue().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setDouble(6, Double.valueOf(vArr[0]));
					stmt.setLong(7, Long.valueOf(vArr[1]));
					stmt.setLong(8, Long.valueOf(vArr[2]));
					stmt.setLong(9, Long.valueOf(vArr[3]));
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
			//计算各维度数据流量总和
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
				sqlSb.append("SELECT sum(downBytes),sum(freezeTime),sum(freezeCount),sum(playSeconds),").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM temp_byte");
				if (groupSb.length() > 0) {
					sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
				}
				num++;
				
				rs = st.executeQuery(sqlSb.toString());
				String key = "";
				while (rs.next()) {
					key = rs.getString(5)+ "#" +rs.getString(6)+ "#" +rs.getString(7)+ "#" +rs.getString(8)+ "#" +rs.getString(9);
					hm.put(key, rs.getLong(1)+ "#" +rs.getLong(2)+ "#" +rs.getLong(3)+ "#" +rs.getLong(4));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, st, rs);
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	
	/**
	 * 离线比实时多了地市维度，以后实时增加及地市维度后此方法可以删除，使用toDownBytesMysqlTemp
	 * 2017-08-10 实时后面只有省份和地市维度了，和离线分开，方法名加后缀RT
	 * @param hm
	 * @param columns
	 */
	public static void toDownBytesMysqlTempRT(Map<String, Long> hm,String[] columns) {
		Connection conn = null;
		PreparedStatement stmt = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder createSql = new StringBuilder();
			createSql.append("CREATE TEMPORARY TABLE `temp_byte` (");
			createSql.append("`provinceID` varchar(30),`cityID` varchar(30),");
			createSql.append("`downBytes` double(18,4))");
			st = conn.createStatement();
			st.execute(createSql.toString());
			
			//Key=省份#地市#牌照方#设备厂商#框架版本,非ALL
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO temp_byte(provinceID,cityID,downBytes)"
					+ " VALUES(?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setDouble(3, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
			//计算各维度数据流量总和
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
				sqlSb.append("SELECT sum(downBytes) as sumByte,").append(selectSb.substring(0, selectSb.length() - 1)).append(" FROM temp_byte");
				if (groupSb.length() > 0) {
					sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
				}
				num++;
				
				rs = st.executeQuery(sqlSb.toString());
				String key = "";
				while (rs.next()) {
					key = rs.getString(2)+ "#" +rs.getString(3);
					hm.put(key, rs.getLong(1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, st, rs);
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	public static void toBizMysqlTemp(Map<String, Long> hm,String[] columns) {
		Connection conn = null;
		PreparedStatement stmt = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder createSql = new StringBuilder();
			createSql.append("CREATE TEMPORARY TABLE `temp_biz` (");
			createSql.append("`provinceID` varchar(30),`cityID` varchar(30),");
			createSql.append("`platform` varchar(30),`deviceProvider` varchar(30),  ");
			createSql.append("`fwVersion` varchar(30),`hasType` varchar(30), ");
			createSql.append("`playSum` bigint(20))");
			st = conn.createStatement();
			st.execute(createSql.toString());
			
			//Key=省份#地市#牌照方#设备厂商#框架版本#节目类型,非ALL
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO temp_biz VALUES(?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, sArr[5]);
					stmt.setLong(7, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
			//计算各维度数据流量总和
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
				sqlSb.append("SELECT sum(playSum),").append(selectSb.substring(0, selectSb.length() - 1)).append(",hasType FROM temp_biz");
				if (groupSb.length() > 0) {
					sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1)).append(",hasType");
				} else{
					sqlSb.append(" GROUP BY hasType");
				}
				num++;
				
				rs = st.executeQuery(sqlSb.toString());
				String key = "";
				while (rs.next()) {
					key = rs.getString(2)+ "#" +rs.getString(3)+ "#" +rs.getString(4)+ "#" +rs.getString(5)+ "#" +rs.getString(6)+ "#" +rs.getString(7);
					hm.put(key, rs.getLong(1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, st, rs);
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	/**
	 * 离线比实时多了地市维度，以后实时增加及地市维度后此方法可以删除，使用toBizMysqlTemp
	 * 2017-08-10 实时后面只有省份和地市维度了，和离线分开，方法名加后缀RT
	 * @param hm
	 * @param columns
	 */
	public static void toBizMysqlTempRT(Map<String, Long> hm,String[] columns) {
		Connection conn = null;
		PreparedStatement stmt = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder createSql = new StringBuilder();
			createSql.append("CREATE TEMPORARY TABLE `temp_biz` (");
			createSql.append("`provinceID` varchar(30),`cityID` varchar(30),");
			createSql.append("`hasType` varchar(30), ");
			createSql.append("`playSum` bigint(20))");
			st = conn.createStatement();
			st.execute(createSql.toString());
			
			//Key=省份#地市#牌照方#设备厂商#框架版本#节目类型,非ALL
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO temp_biz VALUES(?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setLong(4, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
			//计算各维度数据流量总和
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
				sqlSb.append("SELECT sum(playSum),").append(selectSb.substring(0, selectSb.length() - 1)).append(",hasType FROM temp_biz");
				if (groupSb.length() > 0) {
					sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1)).append(",hasType");
				} else {
					sqlSb.append(" GROUP BY hasType");
				}
				num++;
				
				rs = st.executeQuery(sqlSb.toString());
				String key = "";
				while (rs.next()) {
					key = rs.getString(2)+ "#" +rs.getString(3)+ "#" +rs.getString(4);
					hm.put(key, rs.getLong(1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, st, rs);
			DBConnection.closeDB(conn, stmt, null);
		}
		
	}
	
	public static void saveInfoCount(List<InfoCountBean> list, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			String insertSql = "INSERT INTO T_TERMINAL_COUNT(provinceID,cityID,platform,"
					+ "deviceProvider,fwVersion,userCount,parseTime,period)"
					+ " VALUES(?,?,?,?,?,?,?,?)";
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql);
			int count = 0;
			for (InfoCountBean infoCount : list) {
				stmt.setString(1, infoCount.getProvinceID());
				stmt.setString(2, infoCount.getCityID());
				stmt.setString(3, infoCount.getPlatform());
				stmt.setString(4, infoCount.getDeviceProvider());
				stmt.setString(5, infoCount.getFwVersion());
				stmt.setLong(6, infoCount.getCount());
				stmt.setString(7, time);
				stmt.setString(8, period);
				stmt.addBatch();
				count++;
				if (count % 100 == 0) {
					stmt.executeBatch();
					conn.commit();
					count = 0;
				}
			}
			if (count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	public static void toMysqlOnLoginSucc(Map<String, Double> hm, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_LOGIN_SUCC_INDEX(provinceID,cityId,platform,deviceProvider,fwVersion,succRate,parseTime,period)"
					+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setDouble(6, en.getValue());
					stmt.setString(7,time);
					stmt.setString(8,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void toMysqlOnDetailLoadSucc(Map<String, Double> hm, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_DETAIL_LOAD_SUCC(provinceID,cityId,platform,deviceProvider,fwVersion,loadSuccRate,parseTime,period)"
					+ " VALUES(?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setDouble(6, en.getValue());
					stmt.setString(7,time);
					stmt.setString(8,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	//事件趋势离线入mysql;
	public static void eventTrendToMysql(Map<String, Long> hm, String time, String period){
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_EVENT_TREND(provinceID,cityId,platform,deviceProvider,fwVersion,eventID,countSum,parseTime,period)"
					+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6, sArr[5]);
					stmt.setLong(7, en.getValue());
					stmt.setString(8,time);
					stmt.setString(9,period);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void toMysqlOnHttpErrorCode(Map<String, Long> hm, String time, String period) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_HTTP_ERROR_CODE(provinceID,cityId,platform,deviceProvider,fwVersion,parseTime,period,errorCode,total)"
					+ " VALUES(?,?,?,?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setString(4, sArr[3]);
					stmt.setString(5, sArr[4]);
					stmt.setString(6,time);
					stmt.setString(7,period);
					stmt.setString(8,sArr[5]);
					stmt.setLong(9,en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
}
