package com.alibaba.middleware.race.helper;

import com.alibaba.middleware.race.utils.MetaTuple;

public interface GoodIndexOperater extends IndexOperater{
	void createGoodIndex();
	
	void addTupleToGoodIndex(String key, MetaTuple goodTuple);
	
	MetaTuple getGoodTupleByGoodId(String goodId);
}
