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
import com.sihuatech.sqm.spark.bean.ImageLoadBean;
import com.sihuatech.sqm.spark.common.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;

/**
 * 图片加载指标分析-及时率，单位微秒 
 * 图片加载指标分析-成功率
 * 时间维度： 15MIN（15分钟），HOUR（60分钟），DAY（天），WEEK（周），MONTH（月），QUARTER（季），HALFYEAR（半年），YEAR（年）
 * 
 * @author chuql 2016年7月20日
 */
public class ImageLoadOfflineAnalysis {
	private static Logger logger = Logger.getLogger(ImageLoadOfflineAnalysis.class);
	static DecimalFormat formatFour = new DecimalFormat("0.0000");
	
	private static Map<String, String> fastRateMap = new HashMap<String, String>();
	
	/* 日志目录 */
	private static String PATH = "";
	/* 时间维度  */
	private static String PERIOD = "";
	/* 任务编号*/
	private static String TASKID = "";
	
	private static String[] dimensions = { "provinceID", "cityID", "platform", "deviceProvider", "fwVersion"};

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: ImageLoadOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.IMAGE_TASK_ENUM);
			System.exit(1);
		}
		PERIOD = args[0];
		PATH = args[1];
		TASKID = args[2];
		logger.info(String.format("图片加载指标离线分析执行参数 : \n period : %s \n path : %s \n task : %s", PERIOD, PATH,TASKID));
		if (! Constant.PERIOD_ENUM.contains(PERIOD)) {
			System.err.println("Usage: ImageLoadOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.IMAGE_TASK_ENUM);
			System.exit(1);
		}
		if(!Constant.IMAGE_TASK_ENUM.contains(TASKID)){
			System.err.println("Usage: ImageLoadOfflineAnalysis <period> <path> <task> \n"
					+ "  period : time dimension,value is "+Constant.PERIOD_ENUM+" \n"
					+ "  path: root folder + file prefix \n"
					+ "  task: value is "+Constant.IMAGE_TASK_ENUM);
			System.exit(1);
		}
		
		if(StringUtils.isNotBlank(TASKID) && Constant.IMAGE_ALL_TASK.equals(TASKID)){
			logger.info("--------------本次执行全部任务----------------");
		}
		if(StringUtils.isNotBlank(TASKID) && Constant.IMAGE_FAST_LOAD_RATE_TASK.equals(TASKID)){
			logger.info("--------------本次执行图片加载及时率和成功率分析任务----------------");
		}
		int index = PATH.lastIndexOf("/");
		Calendar c = DateUtil.getBackdatedTime(PERIOD);
		String filePath = PATH.substring(0, index)
				+ DateUtil.getPathPattern(PATH.substring(index + 1),PERIOD,c);
		filePath = filePath + "*";

		SparkConf conf = new SparkConf().setAppName("ImageLoadOfflineAnalysis");
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
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				if (lineArr.length < 12) {
					return false;
				} else if (!"7".equals(lineArr[0])) {
					return false;
				} else if ("".equals(lineArr[12].trim())) {
					return false;
				} else if ("".equals(lineArr[13].trim())) {
					return false;
				}
				return true;
			}
		});
		// 提取字段转为Bean
		JavaRDD<ImageLoadBean> rowRDD = filterLines.map(new Function<String, ImageLoadBean>() {
			public ImageLoadBean call(String line) {
				String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
				ImageLoadBean record =  new ImageLoadBean();
				record.setPlatform(lineArr[2].trim());
				record.setProvinceId(lineArr[3].trim());
				record.setCityId(lineArr[4].trim());
				record.setDeviceProvider(lineArr[5]);
				record.setFwVersion(lineArr[6]);
				// 降低位置区域对统计数据的影响，离线分析是对于未知区域从Redis重新获取
				String[] area = IndexToRedis.getStbArea(lineArr[1].trim(), record.getProvinceId());
				if (area != null) {
					record.setProvinceId(area[0]);
					record.setCityId(area[1]);
				}
				record.setRequests(Long.valueOf(lineArr[10].trim()));
				record.setPageDurSucCnt(Long.valueOf(lineArr[12].trim()));
				record.setPageDurLE3sCnt(Long.valueOf(lineArr[13].trim()));
				return record;
			}
		});
		// 注册临时表
		DataFrame imageDataFrame = sqlContext.createDataFrame(rowRDD, ImageLoadBean.class);
		imageDataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER());
		imageDataFrame.registerTempTable("imageload");
		
		long total = imageDataFrame.count();
		logger.info("+++[ImageLoad]图片加载日志记录数：" + total);
		if (total > 0) {
			DataFrame fastFrame = sqlContext
					.sql("select sum(pageDurLE3sCnt) as cnt,sum(requests) as total,sum(pageDurSucCnt) as succCnt ,"+ dimensions[0] + "," + dimensions[1] + "," + dimensions[2]+ "," + dimensions[3]+ "," + dimensions[4]+" from imageload group by "+dimensions[0] + "," +dimensions[1] + "," + dimensions[2] + "," +dimensions[3] + "," + dimensions[4]);
			fastFrame.registerTempTable("cacheImageLoad");
			sqlContext.cacheTable("cacheImageLoad");
			imageAnalysis(sqlContext, c);
			sqlContext.uncacheTable("cacheImageLoad");
		}
		imageDataFrame.unpersist();
	}
	
	private static void imageAnalysis(SQLContext sqlContext,Calendar c){
		if(Constant.IMAGE_ALL_TASK.equalsIgnoreCase(TASKID) || Constant.IMAGE_FAST_LOAD_RATE_TASK.equalsIgnoreCase(TASKID)){
			logger.info("开始计算图片加载及时率及成功率");
			sparkImageFastRateByDimension(sqlContext);
			saveImageFastRate(c);
		}
	}

	/**
	 * 图片加载及时率
	 * @param sqlContext
	 */
	private static void sparkImageFastRateByDimension(SQLContext sqlContext) {
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
            sqlSb.append("select sum(cnt),sum(succCnt),sum(total),").append(selectSb.substring(0,selectSb.length()-1)).append(" from cacheImageLoad");
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
    				if(0 != row.getLong(2)){
	    				 String key = row.getString(3) + "#" + row.getString(4) + "#" + row.getString(5) + "#" + row.getString(6) + "#" + row.getString(7) ;
	    				 Double fastRate = Double.valueOf(formatFour.format((double)row.getLong(0)/row.getLong(2)));
	    				 Double succRate = Double.valueOf(formatFour.format((double)row.getLong(1)/row.getLong(2)));
	    				 String value = fastRate +"#"+ succRate;
	    				 fastRateMap.put(key, value);
    				}
    			}
    		}
        }
    }
	
	private static void saveImageFastRate(Calendar c) {
		logger.info("图片加载及时率和成功率存储开始mapSize=" + fastRateMap.size());
		if (fastRateMap != null && fastRateMap.size() > 0) {
			String time = DateUtil.getIndexTime(PERIOD, c);
			IndexToMysql.toMysqlOnImageIntex(fastRateMap, time,PERIOD);
			fastRateMap.clear();
		}
	}

}
