package com.zuipin.sparkapp.meterials;

/**
 * @author zengXinChao
 * @date 2016年9月27日 下午4:22:52
 * @Description:物料组成
 */
public class Composition {
	private String	productId;
	private String	compositionId;
	private Double	compositionNum;
	
	public Composition() {
		
	}
	
	public String getProductId() {
		return productId;
	}
	
	public void setProductId(String productId) {
		this.productId = productId;
	}
	
	public String getCompositionId() {
		return compositionId;
	}
	
	public void setCompositionId(String compositionId) {
		this.compositionId = compositionId;
	}
	
	public Double getCompositionNum() {
		return compositionNum;
	}
	
	public void setCompositionNum(Double compositionNum) {
		this.compositionNum = compositionNum;
	}
}
