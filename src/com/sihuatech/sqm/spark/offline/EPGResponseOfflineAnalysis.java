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
import org.apache.spark.storage.StorageLevel;

import com.sihuatech.sqm.spark.IndexToMysql;
import com.sihuatech.sqm.spark.IndexToRedis;
import com.sihuatech.sqm.spark.bean.EPGResponseBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;

/**
 * EPG响应指标离线分析-平均响应时长，单位微秒 ; EPG时延分布，单位秒;详情页加载成功率(%) 
 * 时间维度： 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
 * 
 * @author chuql 2016年7月20日
 */
public class EPGResponseOfflineAnalysis {
	private static Logger logger = Logger.getLogger(EPGResponseOfflineAnalysis.class);
	static DecimalFormat formatFour = new DecimalFormat("0.0000");
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度 */
	private static String PERIOD = "";
	/* 任务编号*/
	private static String TASKID = "";
	
	// 时延分布，单位秒
	private static Map<String, String> epgDistributionMap = new HashMap<String, String>();
	// 详情页加载成功率(%) 
    private static Map<String, Double> detailSuccMap = new HashMap<String, Double>();
	
	private static String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion"};
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: EPGResponseOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.EPG_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASKID = args[2];
		
		logger.info(String.format("EPG响应指标离线分析执行参数 : \n period : %s \n path : %s \n task : %s",PERIOD,PATH,TASKID));
		if(! Constant.PERIOD_ENUM.contains(PERIOD)){
			System.err.println("Usage: EPGResponseOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is "+Constant.EPG_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.EPG_TASK_ENUM.contains(TASKID)){
			System.err.println("Usage: EPGResponseOfflineAnalysis <period> <path> <task>\n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix"
					+ "  task: value is "+Constant.EPG_TASK_ENUM);
			System.exit(1);
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.EPG_RESPONSE_ALL_TASK.equals(TASKID)){
			logger.info("--------------本次执行全部任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.EPG_RESPONSE_DISTRIBUTION_TASK.equals(TASKID)){
			logger.info("--------------本次执行EPG时延分布分析任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.DETAIL_LOAD_SUCC_TASK.equals(TASKID)){
			logger.info("--------------本次执行详情页加载成功率分析任务----------------");
		}
		int index = PATH.lastIndexOf("/");
		Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1),PERIOD,c);
		filePath = filePath +"*";
		
		SparkConf conf = new SparkConf().setAppName("EPGResponseOfflineAnalysis");
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
				if (lineArr.length < 16) {
					return false;
				} else if (!"6".equals(lineArr[0])) {
					return false;
				} else if (StringUtils.isBlank(lineArr[2].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[3].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[4].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[5].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[6].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[8].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[11].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[12].trim())) {
					return false;
				} else if (StringUtils.isBlank(lineArr[14].trim())) {
					return false;
				} 
				return true;
			}
		});
		// 提取字段转为Bean
		JavaRDD<EPGResponseBean> tranLines = filterLines.map(new Function<String, EPGResponseBean>() {
			@Override
			public EPGResponseBean call(String line) throws Exception {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				EPGResponseBean record = new EPGResponseBean();
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
				record.setPageType(lineArr[8].trim());
				record.setRequests(Long.valueOf(lineArr[11].trim()));
				record.setHttpRspTime(Long.valueOf(lineArr[12].trim()));
				record.setPageDurSucCnt(Long.valueOf(lineArr[14]));
				return record;
			}
		});
		
		// 注册临时表
		DataFrame epgDataFrame = sqlContext.createDataFrame(tranLines, EPGResponseBean.class);
		epgDataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER());
		epgDataFrame.registerTempTable("epgresponse");

		long total = epgDataFrame.count();// 计算单位时间段内记录总数
		logger.info("+++[EPG]EPG响应日志记录数：" + total);
		if (total > 0) {
			epgAnalysis(sqlContext,c);
		}
		epgDataFrame.unpersist();
	}
	
	private static void epgAnalysis(SQLContext sqlContext,Calendar c){
		if(Constant.EPG_RESPONSE_ALL_TASK.equalsIgnoreCase(TASKID) || Constant.EPG_RESPONSE_DISTRIBUTION_TASK.equalsIgnoreCase(TASKID)){
			logger.info("开始计算EPG时延分布");
			sparkEPGDistributionByDimension(sqlContext);
			saveEPGDistribution(c);
		}
		if(Constant.EPG_RESPONSE_ALL_TASK.equalsIgnoreCase(TASKID) || Constant.DETAIL_LOAD_SUCC_TASK.equalsIgnoreCase(TASKID)){
			logger.info("开始计算详情页加载成功率");
			sparkDetailLoadSucc(sqlContext);
			saveDetailLoadSucc(c);
		}
	}

	/**
	 *  EPG时延分布
	 * @param sqlContext
	 */
	private static void sparkEPGDistributionByDimension(SQLContext sqlContext) {
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
            sqlSb.append("select pageType,sum(case when httpRspTime >= 0 and httpRspTime < 100000 then requests else 0 end) as range1,"
            		+ "sum(case when httpRspTime >= 100000 and httpRspTime < 300000 then requests else 0 end) as range2,"
            		+ "sum(case when httpRspTime >= 300000 and httpRspTime < 2000000 then requests else 0 end) as range3,"
            		+ "sum(case when httpRspTime >= 2000000 then requests else 0 end) as range4,").append(selectSb.substring(0, selectSb.length() - 1)).append(" from epgresponse");
            if (groupSb.length() > 0) {
                sqlSb.append(" GROUP BY pageType,").append(groupSb.substring(0, groupSb.length() - 1));
            } else {
            	sqlSb.append(" GROUP BY pageType");
            }
            num++;
            DataFrame df = sqlContext.sql(sqlSb.toString());
            Row[] stateRow = df.collect();
            if (stateRow == null || stateRow.length == 0) {
    			return;
    		} else {
    			for (Row row : stateRow) {
    				 String key = row.getString(5) + "#" + row.getString(6) + "#" + row.getString(7)+ "#" + row.getString(8)+ "#" + row.getString(9) + "#" + row.getString(0);
    				 String value = row.getLong(1)+ "#" + row.getLong(2)+ "#" + row.getLong(3)+ "#" + row.getLong(4);
    				 epgDistributionMap.put(key, value);
    			}
    		}
        }
    }
	
	private static void saveEPGDistribution(Calendar c) {
		logger.info("EPG时延分布 存储开始mapSize=" + epgDistributionMap.size());
		if (epgDistributionMap != null && epgDistributionMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnEPGDistribution(epgDistributionMap, time,PERIOD);
			epgDistributionMap.clear();
		}
	}
	/**
	 * 详情页加载成功率
	 * @param sqlContext
	 */
	private static void sparkDetailLoadSucc(SQLContext sqlContext) {
		DataFrame detailFrame = sqlContext
				.sql("select sum(pageDurSucCnt) as succCnt,sum(requests) as total," + dimensions[0] + ","
						+ dimensions[1] + "," + dimensions[2]+ "," + dimensions[3]+ "," + dimensions[4] + " from epgresponse where pageType = 3 group by "
						+ dimensions[0] + "," + dimensions[1] + "," + dimensions[2]+ "," + dimensions[3]+ "," + dimensions[4]);
		detailFrame.registerTempTable("detailEpg");
		sqlContext.cacheTable("detailEpg");
		sparkDetailLoadSuccByDimension(sqlContext);
		sqlContext.uncacheTable("detailEpg");
	}
	private static void sparkDetailLoadSuccByDimension(SQLContext sqlContext) {
        int num = 0;
        while (num < Math.pow(2,dimensions.length)) {
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
            sqlSb.append("select sum(succCnt)/sum(total),").append(selectSb.substring(0, selectSb.length()-1)).append(" from detailEpg ");
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
    				 String key = row.getString(1) + "#" + row.getString(2) + "#" + row.getString(3)+ "#" + row.getString(4)+ "#" + row.getString(5);
    				 detailSuccMap.put(key, Double.valueOf(formatFour.format(row.getDouble(0))));
    			}
    		}
        }
    }
	
	private static void saveDetailLoadSucc(Calendar c) {
		logger.info("详情页加载成功率存储开始mapSize=" + detailSuccMap.size());
		if (detailSuccMap != null && detailSuccMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnDetailLoadSucc(detailSuccMap, time,PERIOD);
			detailSuccMap.clear();
		}
	}
}
