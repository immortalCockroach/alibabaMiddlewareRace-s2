package com.alibaba.middleware.race.helper;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * 代表原始文件(order、buyer、good)的记录查找结构 根据index,offset,length定位到某个文件的一行记录
 * 
 * @author immortalCockRoach
 *
 */
public class IndexFileTuple {
	private int fileIndex;
	private long offset;
	private int length;

	public IndexFileTuple(byte[] content) {
		super();
		DataInputStream dos = new DataInputStream(new ByteArrayInputStream(content));
		try {
			fileIndex = dos.readInt();
			offset = dos.readLong();
			length = dos.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getFileIndex() {
		return fileIndex;
	}

	public void setFileIndex(int fileIndex) {
		this.fileIndex = fileIndex;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

}
