package com.alibaba.middleware.race.helper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Good和buyer的索引类
 * 
 * @author immortalCockRoach
 *
 */
public class MetaTuple {
	// 原始的good和buyer文件的Index,offset和length
	private int fileIndex;
	private long originalOffset;
	private int originalLength;
	// order的内存二级索引中存储的数据，代表order的一级有序文件索引中关于该good/buyer的offset和order的数量
	private long indexOffset;
	private int count;

	public MetaTuple(long indexOffset, int count) {
		super();
		this.indexOffset = indexOffset;
		this.count = count;
	}

	public int getFileIndex() {
		return fileIndex;
	}

	public void setFileIndex(int fileIndex) {
		this.fileIndex = fileIndex;
	}

	public long getOriginalOffset() {
		return originalOffset;
	}

	public void setOriginalOffset(long originalOffset) {
		this.originalOffset = originalOffset;
	}

	public int getOriginalLength() {
		return originalLength;
	}

	public void setOriginalLength(int originalLength) {
		this.originalLength = originalLength;
	}

	public long getIndexOffset() {
		return indexOffset;
	}

	public void setIndexOffset(long indexOffset) {
		this.indexOffset = indexOffset;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public byte[] getOriginalByte() {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bo);

		try {
			dos.writeInt(fileIndex);
			dos.writeLong(originalOffset);
			dos.writeInt(originalLength);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bo.toByteArray();
	}

}
