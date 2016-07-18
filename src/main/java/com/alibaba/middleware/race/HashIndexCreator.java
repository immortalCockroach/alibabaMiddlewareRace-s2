package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import com.alibaba.middleware.race.OrderSystemImpl.KV;
import com.alibaba.middleware.race.OrderSystemImpl.Row;
import com.alibaba.middleware.race.utils.CommonConstants;

public class HashIndexCreator implements Runnable{

	private String hashId;
	private BufferedWriter[] writers;
	private Collection<String> files;
	private final int BUCKET_SIZE;
	
	public HashIndexCreator(String hashId, BufferedWriter[] writers, Collection<String> files, int bUCKET_SIZE) {
		super();
		this.hashId = hashId;
		this.writers = writers;
		this.files = files;
		BUCKET_SIZE = bUCKET_SIZE;
	}




	@Override
	public void run() {
		// TODO Auto-generated method stub
		for(String orderFile : this.files) {
			try (BufferedReader reader = createReader(orderFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					KV orderKV = kvMap.removeKV("orderid");
					int index = indexFor(hashWithDistrub(orderKV.longValue), CommonConstants.ORDER_SPLIT_SIZE);
					BufferedWriter bw = this.query1Writers[index];
					bw.write(line);
					bw.newLine();
					line = reader.readLine();
				}
				
			} catch (IOException e) {
				// 忽略
			}
		}
	}

}
