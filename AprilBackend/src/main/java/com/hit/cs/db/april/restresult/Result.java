package com.hit.cs.db.april.restresult;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Result<T> {
	private T result = null;	// 接收返回的单个Model对象
	private List<T> list = null;	// 接收返回的多个Model对象
	private int intResult = 0;
	private String stringResult = null;
	private double doubleResult = 0;
	@JsonFormat(pattern = "yyyy-MM-dd")
	private Date dateResult = null;
	@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date timeResult = null;	
	private String status = null;	// 返回执行的状态 OK正常，ERROR异常
	private String message = null;	// 返回执行的消息
	public T getResult() {
		return result;
	}
	public void setResult(T result) {
		this.result = result;
	}
	public List<T> getList() {
		return list;
	}
	public void setList(List<T> list) {
		this.list = list;
	}
	public int getIntResult() {
		return intResult;
	}
	public void setIntResult(int intResult) {
		this.intResult = intResult;
	}
	public String getStringResult() {
		return stringResult;
	}
	public void setStringResult(String stringResult) {
		this.stringResult = stringResult;
	}
	public double getDoubleResult() {
		return doubleResult;
	}
	public void setDoubleResult(double doubleResult) {
		this.doubleResult = doubleResult;
	}
	public Date getDateResult() {
		return dateResult;
	}
	public void setDateResult(Date dateResult) {
		this.dateResult = dateResult;
	}
	public Date getTimeResult() {
		return timeResult;
	}
	public void setTimeResult(Date timeResult) {
		this.timeResult = timeResult;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	
}
