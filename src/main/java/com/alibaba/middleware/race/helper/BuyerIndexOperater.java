package com.alibaba.middleware.race.helper;

public interface BuyerIndexOperater extends IndexOperater {
	void createBuyerIndex();

	void addTupleToBuyerIndex(String key, MetaTuple buyerTuple);

	MetaTuple getBuyerTupleByBuyerId(String buyerId);
}
