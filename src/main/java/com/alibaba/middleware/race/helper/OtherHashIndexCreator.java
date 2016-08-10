package com.alibaba.middleware.race.helper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * good和buyer的索引创建方式 直接放在内存中
 * 
 * @author immortalCockRoach
 *
 */
public class OtherHashIndexCreator implements Runnable {
	private String hashId;
	private Collection<String> files;
	private CountDownLatch latch;
	private HashSet<String> identitiesSet;
	private IndexOperater operator;
	private int buildCount;
	private int mod;

	public OtherHashIndexCreator(String hashId, Collection<String> files, CountDownLatch latch, String[] identities,
			IndexOperater operater) {
		super();
		this.hashId = hashId;
		this.files = files;
		this.latch = latch;
		this.identitiesSet = new HashSet<>(Arrays.asList(identities));
		this.operator = operater;
		this.buildCount = 0;
		this.mod = 524288;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int fileIndex = 0;
		for (String orderFile : this.files) {
			// System.out.println(orderFile);
			Row kvMap = null;
			// 记录当前行的偏移
			long offset = 0L;
			String id;
			// 记录当前行的总长度
			int length = 0;
			try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, CommonConstants.OTHERFILE_BLOCK_SIZE)) {
				String line = reader.readLine();
				while (line != null) {
					// StringBuilder offSetMsg = new StringBuilder();
					kvMap = StringUtils.createKVMapFromLineWithSet(line, CommonConstants.SPLITTER, this.identitiesSet);
					length = line.getBytes().length;

					// orderId一定存在且为long
					id = kvMap.getKV(hashId).valueAsString();
					if (hashId.equals("goodid")) {
						MetaTuple goodTuple = ((GoodIndexOperater) operator).getGoodTupleByGoodId(id);
						goodTuple.setFileIndex(fileIndex);
						goodTuple.setOriginalOffset(offset);
						goodTuple.setOriginalLength(length);
					} else {
						MetaTuple buyerTuple = ((BuyerIndexOperater) operator).getBuyerTupleByBuyerId(id);
						buyerTuple.setFileIndex(fileIndex);
						buyerTuple.setOriginalOffset(offset);
						buyerTuple.setOriginalLength(length);
					}

					offset += (length + 1);

					buildCount++;
					if ((buildCount & (mod - 1)) == 0) {
						System.out.println(hashId + "construct:" + buildCount);
					}
					line = reader.readLine();
				}
				fileIndex++;

			} catch (IOException e) {
				// 忽略
			}
		}
		this.latch.countDown();
	}

}