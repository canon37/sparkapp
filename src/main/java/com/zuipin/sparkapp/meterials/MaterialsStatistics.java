package com.zuipin.sparkapp.meterials;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.zuipin.sparkapp.util.DBHelper;

/**
 * @Title: MaterialsStatistics
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午11:15:21
 * @Description: 物料统计
 */
public class MaterialsStatistics {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("testapp");
		sparkConf.setMaster("yarn-client");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		HiveContext hivectx = new HiveContext(ctx);
		
		JavaRDD<Row> products = hivectx.sql("select id from cra_test.product").javaRDD();// 商品id
		String sql = "select c.product_id,c.composition_id,c.composition_num from cra_test.product_composition c ";// 商品组成
		final JavaRDD<Row> allComposition = hivectx.sql(sql).javaRDD().cache().repartition(8);
		final List<Composition> result = new ArrayList<Composition>();
		
		for (Row row : products.collect()) {
			Map<String, Composition> compositions = new ConcurrentHashMap<String, Composition>();
			MaterialsStatistics.recursion(Long.toString(row.getLong(0)), Long.toString(row.getLong(0)), allComposition, compositions, 1);
			result.addAll(compositions.values());
		}
		
		MaterialsStatistics.saveCompositionResult(result);
		
		ctx.close();
	}
	
	@SuppressWarnings("serial")
	public static void recursion(final String mainProductId, final String productId, final JavaRDD<Row> allComposition, final Map<String, Composition> compositions,
			final double num) {
		JavaRDD<Row> sub = allComposition.filter(new Function<Row, Boolean>() {
			public Boolean call(Row x) throws Exception {
				if (productId.equals(x.getString(0)) && !x.getString(0).equals(x.getString(1))) {
					return true;
				} else {
					return false;
				}
			}
		});
		
		if (sub.isEmpty()) {
			if (compositions.containsKey(productId)) {
				Composition composition = compositions.get(productId);
				composition.setCompositionNum(composition.getCompositionNum() + num);
			} else {
				Composition composition = new Composition();
				composition.setProductId(mainProductId);
				composition.setCompositionId(productId);
				composition.setCompositionNum(num);
				compositions.put(productId, composition);
			}
		} else {
			for (Row row : sub.collect()) {
				recursion(mainProductId, row.getString(1), allComposition, compositions, row.getDouble(2) * num);
			}
		}
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午11:24:45
	 * @param data
	 * @Description: 保存结果
	 */
	public static void saveCompositionResult(List<Composition> data) {
		try (Connection conn = DBHelper.createPrestoConnection()) {
			Statement stmt = conn.createStatement();
			
			stmt.execute(" delete from cra_test.composition where pt=0 ");
			
			for (Composition e : data) {
				String format = "insert into cra_test.composition (product_id,composition_id,composition_num,pt) values(%s,%s,%s,0)";
				String sql = String.format(format, e.getProductId(), e.getCompositionId(), e.getCompositionNum());
				stmt.execute(sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
