package com.yingjun.stock.dto;

import java.io.Serializable;

/**
*
 * 行情 Bean
 *
 * @author yingjun
*
*/
public class StockTestEvent implements Serializable{

    private String area;
    private double speed;
    private String uuid;


    
    public String getArea() {
		return area;
	}



	public void setArea(String area) {
		this.area = area;
	}



	public double getSpeed() {
		return speed;
	}



	public void setSpeed(double speed) {
		this.speed = speed;
	}



	public String getUuid() {
		return uuid;
	}



	public void setUuid(String uuid) {
		this.uuid = uuid;
	}



	@Override
    public String toString() {
        return "StockTestEvent{" +
                "area='" + area + '\'' +
                ", speed=" + speed +
                ", uuid='" + uuid + '\'' +
                '}';
    }
}