# python3 dstest -r --iter 1000 --workers 10 --timeout 40 --output out.log TestInitialElection2A TestReElection2A TestManyElections2A
# python3 dstest -r --iter 1000 --workers 10 --timeout 45 --output out.log TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B
python3 dstest -r --iter 1000 --workers 10 --timeout 45 --output out.log TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C
python3 dstest -r --iter 1000 --workers 10 --timeout 45 --output out.log TestFigure8Unreliable2C TestUnreliableChurn2C