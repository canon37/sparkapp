package com.zuipin.sparkapp.util;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

/**
 * @Title: MllibUtil.java
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午9:31:09
 * @Description:
 */
public class MllibUtil {
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午9:45:06
	 * @param model
	 * @param trainData
	 * @return
	 * @Description: 平均方差
	 */
	@SuppressWarnings("serial")
	public static double meanSquareError(final GeneralizedLinearModel model, JavaRDD<LabeledPoint> trainData) {
		JavaRDD<Tuple2<Double, Double>> valuesAndPres = trainData.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
			public Tuple2<Double, Double> call(LabeledPoint point) throws Exception {
				Double prediction = model.predict(point.features());
				return new Tuple2<Double, Double>(prediction, point.label());
			}
		});
		
		double mse = new JavaDoubleRDD(valuesAndPres.map(new Function<Tuple2<Double, Double>, Object>() {
			public Object call(Tuple2<Double, Double> pair) throws Exception {
				return Math.pow(pair._1() - pair._2(), 2.0);
			}
		}).rdd()).mean();
		
		return mse;
	}
}
