package com.alibaba.middleware.race.utils;

public class IndexBuffer {
	private Object index;
	private String line;
	
	public IndexBuffer(Object index, String line) {
		super();
		this.index = index;
		this.line = line;
	}
	public Object getIndex() {
		return index;
	}
	public void setIndex(Object index) {
		this.index = index;
	}
	public String getLine() {
		return line;
	}
	public void setLine(String line) {
		this.line = line;
	}

}
