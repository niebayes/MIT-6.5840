# python3 dstest --iter 100 --workers 10 --timeout 60 --output out.log TestBasic3A TestSpeed3A TestConcurrent3A TestUnreliable3A TestUnreliableOneKey3A TestOnePartition3A TestManyPartitionsOneClient3A TestManyPartitionsManyClients3A TestPersistOneClient3A TestPersistConcurrent3A TestPersistConcurrentUnreliable3A TestPersistPartition3A TestPersistPartitionUnreliable3A TestPersistPartitionUnreliableLinearizable3A
python3 dstest -r --iter 100 --workers 10 --timeout 20 --output out.log TestBasic3A