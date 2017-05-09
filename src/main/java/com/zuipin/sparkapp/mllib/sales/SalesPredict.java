package com.zuipin.sparkapp.mllib.sales;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.zuipin.sparkapp.util.DateUtil;
import com.zuipin.sparkapp.util.MllibUtil;

import scala.Tuple2;
import scala.Tuple3;

/**
 * @Title: SalesPredict.java
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午9:19:29
 * @Description: 销售预测 main入参：步长、跌代次数、平均系数、预测天数、shell任务id
 */
public class SalesPredict {
	/** 缩放前测试特征 */
	public static List<LabeledPoint>	testlistBeforeScaling	= new ArrayList<LabeledPoint>();
	/** 缩放前测试预测点 */
	public static Vector				testpointBeforeScaling	= null;
	/** 测试 */
	public static boolean				testflag				= true;
	
	public static void main(String[] args) throws Exception {
		String taskStart = DateUtil.getCurrentDateStr();
		Double stepSize = Double.parseDouble(args[0]);// 步长
		Integer iterations = Integer.parseInt(args[1]);// 迭代次数
		Double avgCoefficient = Double.parseDouble(args[2]);// 平均系数
		Integer predictDays = Integer.parseInt(args[3]);// 预测天数
		String taskid = args[4];// 任务id
		
		SparkConf conf = new SparkConf().setAppName("spark_sales_predict");
		conf.setMaster("yarn-client");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		HiveContext hctx = new HiveContext(ctx);
		
		/** 查询所有商品 30天内的有消费记录的天数 和 14天内的总销量 */
		Calendar calendar = DateUtil.getCalendarWithoutTime();
		String end = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -14);// 两周前
		String date14 = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -16);// 一个月前
		String date30 = DateUtil.getDateStr(calendar);
		StringBuffer proSql = new StringBuffer();
		proSql.append(" select p.id,p.pro_sku,p.pt,COALESCE(d.counter,0),COALESCE(d1.sum,0) from cra.product p ");
		proSql.append(" left join( ");
		proSql.append(" 	select s.product_id,count(1) as counter from cra.sales_statistic s ");
		proSql.append(" 	where s.count>0 and s.create_date>='").append(date30).append("' and s.create_date<'").append(end).append("' ");
		proSql.append("		group by s.product_id ");
		proSql.append(" ) d on d.product_id=p.id ");
		proSql.append(" left join( ");
		proSql.append(" 	select s.product_id,sum(s.count) as sum  from cra.sales_statistic s ");
		proSql.append(" 	where s.count>0 and s.create_date>='").append(date14).append("' and s.create_date<'").append(end).append("' ");
		proSql.append(" 	group by s.product_id");
		proSql.append(" ) d1 on d1.product_id=p.id ");
		proSql.append(" where p.pro_sku is not null and p.pro_sku!='' ");
		JavaRDD<Row> products = hctx.sql(proSql.toString()).javaRDD();
		
		List<Row> avgList = new ArrayList<Row>();// 平均
		List<Row> regressionList = new ArrayList<Row>();// 线性回归
		for (Row r : products.collect()) {
			if (r.getLong(3) < 20) {
				avgList.add(r);
			} else {
				regressionList.add(r);
			}
		}
		
		List<PredictResult> resultList = new ArrayList<PredictResult>();
		
		/** 回归预测 */
		StringBuffer condition = new StringBuffer();
		for (Row row : regressionList) {
			condition.append(" or(s.product_id=").append(row.getLong(0)).append(" and s.pt=").append(row.getInt(2)).append(") ");
		}
		if (!condition.toString().equals("")) {
			StringBuffer sql = new StringBuffer();
			sql.append(" select s.product_id,COALESCE(s.count,0),s.create_date,s.pt from cra.sales_statistic s where 1=2 ");
			sql.append(condition.toString());
			JavaRDD<Row> rows = hctx.sql(sql.toString()).javaRDD().cache().repartition(8);// 每日销量
			
			for (Row r : regressionList) {
				List<Tuple2<Double, double[]>> features = new ArrayList<Tuple2<Double, double[]>>();// 特征集合
				for (int i = 0; i < 30; i++) {// 往前取30个特征
					features.add(getPoint(rows, predictDays, i, r));
				}
				
				Tuple2<Double, double[]> predictPoint = getPoint(rows, 0, 0, r);// 预测点
				List<LabeledPoint> points = featureScaling(features, predictPoint);// 缩放
				
				JavaRDD<LabeledPoint> trainData = ctx.parallelize(points);
				LinearRegressionWithSGD sgd = new LinearRegressionWithSGD();
				sgd.setIntercept(true);
				sgd.optimizer().setStepSize(stepSize);
				sgd.optimizer().setNumIterations(iterations);
				LinearRegressionModel model = sgd.run(trainData.rdd());
				double prediction = model.predict(Vectors.dense(predictPoint._2()));// 预测
				double mse = MllibUtil.meanSquareError(model, trainData);// 平均方差
				
				PredictResult result = new PredictResult();
				result.setStepSize(stepSize);
				result.setIterations(iterations);
				result.setPredictDays(predictDays);
				result.setMse(mse);
				result.setIntercept(model.intercept());
				result.setWeight(model.weights());
				result.setPrediction(prediction);
				result.setTaskid(taskid);
				result.setBeginDate(taskStart);
				result.setFinishDate(DateUtil.getCurrentDateStr());
				result.setSku(r.getString(1));
				result.setProductId(r.getLong(0));
				result.setPt(r.getInt(2));
				result.setResultType(1);
				resultList.add(result);
				
				test(predictPoint, points);// 测试
			}
		}
		
		/** 平均预测 */
		for (Row row : avgList) {
			Double prediction = row.getDouble(4) * avgCoefficient;
			
			PredictResult result = new PredictResult();
			result.setPredictDays(predictDays);
			result.setPrediction(prediction);
			result.setTaskid(taskid);
			result.setBeginDate(taskStart);
			result.setFinishDate(DateUtil.getCurrentDateStr());
			result.setSku(row.getString(1));
			result.setProductId(row.getLong(0));
			result.setPt(row.getInt(2));
			result.setResultType(0);
			result.setAvgCoefficient(avgCoefficient);
			result.setSales(row.getDouble(4));
			resultList.add(result);
		}
		
		PredictResult.saveResult(resultList);
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:08:07
	 * @param features
	 * @param predictPoint
	 * @return
	 * @Description: 特征值缩放
	 */
	public static List<LabeledPoint> featureScaling(List<Tuple2<Double, double[]>> features, Tuple2<Double, double[]> predictPoint) {
		testDataBeforeScaling(features, predictPoint);// 测试
		features.add(predictPoint);
		for (int i = 0; i < features.get(0)._2().length; i++) {
			double max = features.get(0)._2()[i];
			double min = max;
			for (Tuple2<Double, double[]> e : features) {
				max = e._2[i] > max ? e._2[i] : max;
				min = e._2[i] > min ? min : e._2[i];
			}
			for (Tuple2<Double, double[]> e : features) {
				e._2()[i] = max == min ? 1 : (e._2()[i] - min) / (max - min);
			}
		}
		features.remove(predictPoint);
		
		List<LabeledPoint> scalingPoints = new ArrayList<LabeledPoint>();
		for (Tuple2<Double, double[]> e : features) {
			LabeledPoint point = new LabeledPoint(e._1(), Vectors.dense(e._2()));
			scalingPoints.add(point);
		}
		return scalingPoints;
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:05:17
	 * @param rows销售量
	 * @param days预测天数
	 * @param forward往前天数
	 * @param row商品
	 * @return
	 * @Description: 获取特征
	 */
	public static Tuple2<Double, double[]> getPoint(JavaRDD<Row> rows, int days, int forward, Row row) {
		Calendar calendar = DateUtil.getCalendarWithoutTime();
		calendar.add(Calendar.DATE, -forward);
		String end = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -days);
		String begin = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -1);// 前一天
		String date1 = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -6);// 前7天
		String date2 = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -7);// 前14天
		String date3 = DateUtil.getDateStr(calendar);
		calendar.add(Calendar.DATE, -16);// 前30天
		String date4 = DateUtil.getDateStr(calendar);
		
		Tuple3<Double, Double, Double> label = getFeature(begin, end, row, rows);
		Tuple3<Double, Double, Double> feature1 = getFeature(date1, begin, row, rows);
		Tuple3<Double, Double, Double> feature2 = getFeature(date2, begin, row, rows);
		Tuple3<Double, Double, Double> feature3 = getFeature(date3, begin, row, rows);
		Tuple3<Double, Double, Double> feature4 = getFeature(date4, begin, row, rows);
		double[] features = { feature1._1(), feature2._1(), feature2._2(), feature2._3(), feature3._1(), feature3._2(), feature3._3(), feature4._1(), feature4._2(),
				feature4._3() };
		
		return new Tuple2<Double, double[]>(label._3(), features);
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:12:45
	 * @param begin开始时间
	 * @param end结束时间
	 * @param row
	 * @param rows
	 * @return 返参tuple<最大销量,最小销量,总销量>
	 * @Description: 计算特征
	 */
	@SuppressWarnings("serial")
	public static Tuple3<Double, Double, Double> getFeature(final String begin, final String end, final Row row, JavaRDD<Row> rows) {
		JavaRDD<Tuple3<Double, Double, Double>> map = rows.filter(new Function<Row, Boolean>() {
			public Boolean call(Row r) throws Exception {
				String orderdate = r.getString(2);
				long productId = r.getLong(0);
				int pt = r.getInt(3);
				if (orderdate.compareTo(begin) >= 0 && orderdate.compareTo(end) < 0 && productId == row.getLong(0) && pt == row.getInt(2)) {
					return true;
				} else {
					return false;
				}
			}
		}).map(new Function<Row, Tuple3<Double, Double, Double>>() {
			public Tuple3<Double, Double, Double> call(Row r) throws Exception {
				return new Tuple3<Double, Double, Double>(r.getDouble(1), r.getDouble(1), r.getDouble(1));
			}
			
		});
		
		if (map.isEmpty()) {
			return new Tuple3<Double, Double, Double>(0d, 0d, 0d);
		}
		
		return map.reduce(new Function2<Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>>() {
			public Tuple3<Double, Double, Double> call(Tuple3<Double, Double, Double> v1, Tuple3<Double, Double, Double> v2) throws Exception {
				return new Tuple3<Double, Double, Double>(v1._1() > v2._1() ? v2._1() : v1._1(), v1._2() > v2._2() ? v1._2() : v2._2(), v1._3() + v2._3());
			}
		});
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:20:32
	 * @param features
	 * @param predictPoint
	 * @Description: 测试缩放前数据
	 */
	public static void testDataBeforeScaling(List<Tuple2<Double, double[]>> features, Tuple2<Double, double[]> predictPoint) {
		if (testflag) {
			testpointBeforeScaling = Vectors.dense(predictPoint._2().clone());
			for (Tuple2<Double, double[]> e : features) {
				testlistBeforeScaling.add(new LabeledPoint(e._1(), Vectors.dense(e._2().clone())));
			}
		}
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:33:49
	 * @param predictPoint
	 * @param points
	 * @Description: 测试
	 */
	public static void test(Tuple2<Double, double[]> predictPoint, List<LabeledPoint> points) {
		if (testflag) {
			System.out.println("===========================================");
			System.out.println("===============beforeScaling===============");
			System.out.println("---predict point:" + testpointBeforeScaling);
			for (LabeledPoint e : testlistBeforeScaling) {
				System.out.println("--lab:" + e.label() + "---point:" + e.features());
			}
			System.out.println("===========================================");
			System.out.println("================afterScaling===============");
			System.out.println("---predict point:" + Vectors.dense(predictPoint._2()));
			for (LabeledPoint e : points) {
				System.out.println("--lab:" + e.label() + "---point:" + e.features());
			}
			System.out.println("===========================================");
		}
	}
}
