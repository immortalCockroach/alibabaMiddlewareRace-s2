package com.alibaba.middleware.race.utils;

public class IndexBuffer {
	private int index;
	private String line;
	
	public IndexBuffer(int index, String line) {
		super();
		this.index = index;
		this.line = line;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public String getLine() {
		return line;
	}
	public void setLine(String line) {
		this.line = line;
	}

}
