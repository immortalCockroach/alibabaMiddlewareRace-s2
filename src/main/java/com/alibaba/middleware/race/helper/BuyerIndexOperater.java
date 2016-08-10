package com.alibaba.middleware.race.helper;

import com.alibaba.middleware.race.utils.MetaTuple;

public interface BuyerIndexOperater extends IndexOperater{
	void createBuyerIndex();
	
	void addTupleToBuyerIndex(String key, MetaTuple buyerTuple);
	
	MetaTuple getBuyerTupleByBuyerId(String buyerId);
}
