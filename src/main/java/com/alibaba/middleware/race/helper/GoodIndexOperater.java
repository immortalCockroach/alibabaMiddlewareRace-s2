package com.alibaba.middleware.race.helper;

public interface GoodIndexOperater extends IndexOperater {
	void createGoodIndex();

	void addTupleToGoodIndex(String key, MetaTuple goodTuple);

	MetaTuple getGoodTupleByGoodId(String goodId);
}
