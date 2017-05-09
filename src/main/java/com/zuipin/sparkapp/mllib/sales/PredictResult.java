package com.zuipin.sparkapp.mllib.sales;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import org.apache.spark.mllib.linalg.Vector;

import com.zuipin.sparkapp.util.DBHelper;

/**
 * @Title: PredictResult
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午10:48:58
 * @Description: PredictResult
 */
public class PredictResult {
	private Double	stepSize;
	
	private Integer	iterations;
	
	private Integer	predictDays;
	
	private Double	mse;
	
	private Double	intercept;
	
	private Vector	weight;
	
	private Double	prediction;
	
	private String	taskid;
	
	private String	beginDate;
	
	private String	finishDate;
	
	private String	sku;
	
	private Long	productId;
	
	private Integer	pt;
	
	private Integer	resultType;
	
	private Double	avgCoefficient;
	
	private Double	sales;
	
	public PredictResult() {
		
	}
	
	public Integer getIterations() {
		return iterations;
	}
	
	public void setIterations(Integer iterations) {
		this.iterations = iterations;
	}
	
	public Integer getPredictDays() {
		return predictDays;
	}
	
	public void setPredictDays(Integer predictDays) {
		this.predictDays = predictDays;
	}
	
	public Double getMse() {
		return mse;
	}
	
	public void setMse(Double mse) {
		this.mse = mse;
	}
	
	public Double getIntercept() {
		return intercept;
	}
	
	public void setIntercept(Double intercept) {
		this.intercept = intercept;
	}
	
	public Vector getWeight() {
		return weight;
	}
	
	public void setWeight(Vector weight) {
		this.weight = weight;
	}
	
	public Double getPrediction() {
		return prediction;
	}
	
	public void setPrediction(Double prediction) {
		this.prediction = prediction;
	}
	
	public String getTaskid() {
		return taskid;
	}
	
	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}
	
	public String getBeginDate() {
		return beginDate;
	}
	
	public void setBeginDate(String beginDate) {
		this.beginDate = beginDate;
	}
	
	public String getFinishDate() {
		return finishDate;
	}
	
	public void setFinishDate(String finishDate) {
		this.finishDate = finishDate;
	}
	
	public String getSku() {
		return sku;
	}
	
	public void setSku(String sku) {
		this.sku = sku;
	}
	
	public Long getProductId() {
		return productId;
	}
	
	public void setProductId(Long productId) {
		this.productId = productId;
	}
	
	public Integer getPt() {
		return pt;
	}
	
	public void setPt(Integer pt) {
		this.pt = pt;
	}
	
	public Double getStepSize() {
		return stepSize;
	}
	
	public void setStepSize(Double stepSize) {
		this.stepSize = stepSize;
	}
	
	public Integer getResultType() {
		return resultType;
	}
	
	public void setResultType(Integer resultType) {
		this.resultType = resultType;
	}
	
	public Double getAvgCoefficient() {
		return avgCoefficient;
	}
	
	public void setAvgCoefficient(Double avgCoefficient) {
		this.avgCoefficient = avgCoefficient;
	}
	
	public Double getSales() {
		return sales;
	}
	
	public void setSales(Double sales) {
		this.sales = sales;
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月22日 上午10:49:04
	 * @throws Exception
	 * @Description: 保存结果
	 */
	public static void saveResult(List<PredictResult> list) throws Exception {
		clearResult();
		try (Connection conn = DBHelper.createConnection()) {
			StringBuffer sql = new StringBuffer();
			sql.append("insert into shell_task_info(params,result,shell_task_id,finish_date,remark,begin_date,sku,result_type,pt)");
			sql.append("values(?,?,?,?,?,?,?,?,?)");
			PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());
			
			int count = 0;
			for (PredictResult result : list) {
				count++;
				if (result.getResultType() == 1) {
					String param = "步长：" + result.getStepSize() + "<br/>迭代次数：" + result.getIterations() + "<br/>预测天数：" + result.getPredictDays();
					StringBuffer remark = new StringBuffer();
					remark.append("平均方差：" + result.getMse());
					remark.append("<br/>截距：" + result.getIntercept());
					remark.append("<br/>权重：" + result.getWeight());
					
					prepareStatement.setObject(1, param);
					prepareStatement.setObject(2, new BigDecimal(result.getPrediction()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
					prepareStatement.setObject(3, result.getTaskid());
					prepareStatement.setObject(4, result.getFinishDate());
					prepareStatement.setObject(5, remark.toString());
					prepareStatement.setObject(6, result.getBeginDate());
					prepareStatement.setObject(7, result.getSku());
					prepareStatement.setObject(8, 1);
					prepareStatement.setObject(9, result.getPt());
					prepareStatement.addBatch();
				} else {
					StringBuffer param = new StringBuffer();
					param.append("预测天数：").append(result.getPredictDays());
					param.append("<br/>系数：").append(result.getAvgCoefficient());
					String remark = "近两周销量：" + result.getSales();
					
					prepareStatement.setObject(1, param.toString());
					prepareStatement.setObject(2, new BigDecimal(result.getPrediction()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
					prepareStatement.setObject(3, result.getTaskid());
					prepareStatement.setObject(4, result.getFinishDate());
					prepareStatement.setObject(5, remark);
					prepareStatement.setObject(6, result.getBeginDate());
					prepareStatement.setObject(7, result.getSku());
					prepareStatement.setObject(8, 0);
					prepareStatement.setObject(9, result.getPt());
					prepareStatement.addBatch();
				}
				if (count % 1000 == 0) {
					prepareStatement.executeBatch();
				}
			}
			prepareStatement.executeBatch();
		}
	}
	
	/**
	 * @author: zengxinchao
	 * @date: 2016年11月23日 上午10:08:21
	 * @throws Exception
	 * @Description: 清空表
	 */
	public static void clearResult() throws Exception {
		try (Connection conn = DBHelper.createConnection()) {
			Statement stmt = conn.createStatement();
			stmt.execute("delete from shell_task_info ");
		}
	}
}
