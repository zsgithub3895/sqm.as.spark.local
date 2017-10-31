package com.sihuatech.sqm.spark.offline;

import java.text.DecimalFormat;
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
import com.sihuatech.sqm.spark.bean.TerminalLoginInfo;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;

/**
 * 机顶盒登录日志，计算机顶盒登录成功率(%),时间维度：天
 * @author chuql
 * 2017年2月21日
 */
public class TerminalLoginOfflineAnalysis {
	private static Logger logger = Logger.getLogger(TerminalLoginOfflineAnalysis.class);
	static DecimalFormat formatFour = new DecimalFormat("0.0000");
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务编号*/
	private static String TASKID = "";
	
	private static Map<String, Double> sucLoginMap = new HashMap<String, Double>();
	
	private static String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion"};
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: TerminalLoginOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.LOGIN_TASK_ENUM);
			System.exit(1);
		}
		
		PERIOD = args[0];
		PATH = args[1];
		TASKID = args[2];
		
		logger.info(String.format("机顶盒登录日志离线分析执行参数 : \n period : %s \n path : %s \n task : %s",PERIOD,PATH,TASKID));
		if(! Constant.PERIOD_ENUM.contains(PERIOD)){
			System.err.println("Usage: TerminalLoginOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is "+Constant.LOGIN_TASK_ENUM);
			System.exit(1);
		}
		
		if(!Constant.LOGIN_TASK_ENUM.contains(TASKID)){
			System.err.println("Usage: TerminalLoginOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is "+Constant.LOGIN_TASK_ENUM);
			System.exit(1);
		}
		
		if(StringUtils.isNotBlank(TASKID) && Constant.LOGIN_ALL_TASK.equals(TASKID)){
			logger.info("--------------本次执行全部任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.LOGIN_SUCC_TASK.equals(TASKID)){
			logger.info("--------------本次执行登录成功率分析任务----------------");
		}
		
		int index = PATH.lastIndexOf("/");
		Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1),PERIOD,c);
		filePath = filePath +"*";
		
		SparkConf conf = new SparkConf().setAppName("TerminalLoginOfflineAnalysis");
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
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 14) {
					return false;
				} else if (!"9".equals(lineArr[0])) {
					return false;
				} else if (StringUtils.isBlank(lineArr[2].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[10].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[11].trim())) {
					return false;
				}
				return true;
			}
		});
		
		// 提取字段转为Bean
		JavaRDD<TerminalLoginInfo> tranLines = filterLines.map(new Function<String, TerminalLoginInfo>() {
			@Override
			public TerminalLoginInfo call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				TerminalLoginInfo record = new TerminalLoginInfo();
				record.setPlatform(lineArr[2].trim());
				record.setProvinceID(lineArr[3].trim());
				record.setCityID(lineArr[4].trim());
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(lineArr[1].trim(), record.getProvinceID());
				if (area != null) {
					record.setProvinceID(area[0]);
					record.setCityID(area[1]);
				}
				record.setDeviceProvider(lineArr[5].trim());
				record.setFwVersion(lineArr[6].trim());
				record.setPages(lineArr[9].trim());
				record.setSucPages(lineArr[10].trim());
				return record;
			}
		});
		
		
		// 注册临时表
		DataFrame loginDataFrame = sqlContext.createDataFrame(tranLines, TerminalLoginInfo.class);
		loginDataFrame.registerTempTable("terminalLogin");
		
		long total = loginDataFrame.count();// 计算单位时间段内记录总数
		logger.info("+++机顶盒登录日志记录数：" + total);
		if (total > 0) {
			logger.info("计算登录成功率");
			String arrStr = StringUtils.join(dimensions, ",");
			DataFrame detailFrame = sqlContext.sql("select sum(sucPages) as succCnt,sum(pages) as total,"
					+ arrStr + " from terminalLogin group by " + arrStr);
			detailFrame.registerTempTable("cacheTerminalLogin");
			sqlContext.cacheTable("cacheTerminalLogin");
			succLoginByDimension(sqlContext);
			saveLoginSucc(c);
			sqlContext.uncacheTable("cacheTerminalLogin");
		}
	}
	
	private static void succLoginByDimension(SQLContext sqlContext) {
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
            sqlSb.append("select sum(succCnt)/sum(total),").append(selectSb.substring(0, selectSb.length() - 1)).append(" from cacheTerminalLogin");
            if (groupSb.length() > 0) {
                sqlSb.append(" GROUP BY ").append(groupSb.substring(0, groupSb.length() - 1));
            } 
            num++;
            DataFrame df = sqlContext.sql(sqlSb.toString());
            Row[] stateRow = df.collect();
            if (stateRow == null || stateRow.length == 0) {
    			return;
    		} else {
    			for (Row row : stateRow) {
    				 String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3)+ "#"
    				 		+ row.getString(4) + "#" + row.getString(5);
    				 sucLoginMap.put(key, Double.valueOf(formatFour.format(row.getDouble(0))));
    			}
    		}
        }
	}
	private static void saveLoginSucc(Calendar c) {
		logger.info("登录成功率 存储开始mapSize=" + sucLoginMap.size());
		if (sucLoginMap != null && sucLoginMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnLoginSucc(sucLoginMap, time, PERIOD);
			sucLoginMap.clear();
		}
	}
}
