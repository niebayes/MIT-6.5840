# Lab3: Fault-tolerant Key-Value Service

### server 层与 raft 层的耦合关系是怎样的？

在一个分布式系统中，raft 或者其它 consensus 算法通常是被实现为一个共识库，被应用层所调用。应用层把一个需要共识的东西，例如 operation，传给共识库。共识库执行共识操作，然后告知应用层某个 operation 已经被成功共识了。此时应用层就可以放心大胆地去 execute 这个 operation。

对于这个 lab 而言，server 层把 operation 传给 raft 层。如果这个 raft peer 不是 leader，那么 raft 会拒绝这个 operation。如果这个 raft peer 是 leader，那么它会把这个 operation 塞进一个 log entry，然后进行共识操作。当这个 log entry 被 commit 之后，committer 线程把它交付给 server 层。server 层每 apply 一个 operation，都会根据 operation 的 clerk id 和 op id 更新去重表以及通知 service handler 某个 operation 已经被 apply 了。

显然，在这整个过程中，server 层的操作完全不需要 raft 层的任何信息，raft 层只需要暴露出接受待共识的 operation 和交付已经共识的 operation 这两个接口。在这个 lab 中，前者是 `Start` 函数，后者是 `applyCh` 。因此，如果在 server 层使用或维护了 raft 层的任何信息，都是不必要甚至是错误的做法。设想把 raft 换成 paxos 或者其它共识算法，难道server 层也要随之改变吗？

在我的实现中，server 层只使用了三个与 raft 层相关的东西：ApplyMsg 中的 `SnapshotValid`，ApplyMsg 中的 `CommandIndex`，以及 `Start` 所返回的 `isLeader` 。其中只有 `CommandIndex` 是必需的，其它二者都是不必要的。（实际上，我还在思考 `CommandIndex` 真的是必需的吗？）

### 为什么 raft 层的 last applied 应该改为 last delivered？

apply 这个操作指代的是把 operation apply 到 state machine，因此 apply 与 raft 层是无关的。在 raft 论文中，作者实际上把 server, state machine module, 和 consensus module 比较紧密地耦合在了一起，因此会把一些在 state machine 模块或者 server 层的用语或功能在 raft 论文中进行使用和描述。

根据我的理解，应该把 server, state machine module, 和 consensus module 以松耦合的方式来实现。为此，我认为 raft 论文中的一些变量名也应该被改变。其中，last applied 明显是属于 state machine 和 server 层的用语，因此我觉得 last applied 应该改为 last delivered，表示 raft 层交付给 server 层的 committed log entries 中最后的那个 log entry 的 index。

### 如何解决 `... missing element` 错误？

问题描述：Get 所拿到的 value 中间缺了连续的一小段，有时候是缺了单个 Append，有时候是缺了多个 Appends。

检查日志发现，每个 server 都成功 apply 了所有的 Appends。但是有一个或多个 server 在 reply Get 时，回复的 value 中却缺了一些 Appends。后来把错误定位到了 snapshot ingestion。下面对该错误发生的场景进行详细的描述。

一个 follower 的 snapshot index 为 X，committed index 为 Y，且有 Y > X。follower 的 committer 线程检测到存在新的 committed log entries，于是异步地把这些 log entries 以 ApplyMsg 的形式交付给 server 层。server 执行以后，检测到需要做 snapshotting，于是在 Y 处生成了一个 snapshot，并准备调用 raft 层的 snapshot 接口把这个 snapshot 传给 raft 层。

就在 server 层将要调用 snapshot 接口之前，这个 follower 收到了一个比较过时的 InstallSnapshot request（可能是由于网络 delay），其 snapshot index 为 Z > X，但 Z < Y。在我错误的实现中，我使用 follower 的 snapshot index 进行 InstallSnapshot 的 stale checking。由于 follower 检测到 Z > X，于是不会 reject 这个 snapshot，而是先在 raft 层执行 log compaction，然后通知 committer 线程来了一个新的 snapshot。committer 线程被唤醒以后，把这个 snapshot 塞进 ApplyMsg 中，然后异步地交付给 server 层。

在异步交付的同时，raft 层收到了 server 层在 Y 处生成的 snapshot。由于 Y > Z，raft 层不会 reject 这个 snapshot，而是正常地执行 log compaction。在这整个过程中，raft 层没有 accept stale snapshot，始终没有产生任何错误。

但是 server 层就不是如此。在我错误的实现中，当 server 收到 committer 线程交付过来的 Z 处的 snapshot 时，server 不会进行任何判断，而是直接以 replace 的方式 ingest 这个 snapshot。也就是说，server 会直接用 Z 处的 snapshot 中所包含的 key-value pairs 和去重表替换 server 当前的 key-value pairs 和去重表。由于 Y > Z，则 server 已经 applied 的、index 为 (Z, Y] 区间内的 commands 实际上都被 discard 了。由于 raft 层在整个过程中没有任何 rollback，这些被 discard 的 commands 再也不会从 raft 层获取到。这就导致 `... missing element` 错误的出现。

解决方法：

我最初是在 server 层加了一个 last applied 变量，在每次 ingest snapshot 或 execute commands 时，都要求 snapshot index 或者 command index 必须大于这个变量。在执行完之后，更新这个变量。这样的限制，使得 server 层不会 ingest stale snapshot，也就不会出现 ingest stale snapshot 所导致的  `... missing element` 的错误。

我执行了 10000 次测试，没有产生任何错误，验证了这个解决方法的正确性。但是显然，这样的方法只是以暴力的方式强制要求 server 层不 rollback，并没有解决错误的根源。通过以上分析，我们知道，正是因为 raft 层 accept 了一个 snapshot index ≤ committed index 的 snapshot，才导致 server 层收到一个 stale snapshot。因此，要根本地解决这个问题，我们需要在 InstallSnapshot request 的 handler 中，按照 follower 的 committed index 而不是 follower 的 snapshot index 进行 InstallSnapshot 的 stale checking。

从语义的角度来讲，当且仅当 leader 通过 next index 发现一个 follower lags behind the latest snapshot 时，leader 才会发送一个 InstallSnapshot request 给这个 follower。这就说明，当 follower 接收到 InstallSnapshot request 时，当且仅当 follower 此时仍然 lag behind，follower 才应该 accept 这个 request。follower 的 committed index 超过 snapshot index，就说明了 follower 已经不 lag behind 了。由于 committed index ≥ applied index ≥ snapshot index，因此使用 committed index 进行 lag behind 的判断才是唯一正确的方法。

从模块解耦的角度来讲，应用层不应该维护共识层的任何状态，因此让 last applied 出现在 server 层本身就是一个绝对的错误。

思考：能不能使用 applied index 进行 InstallSnapshot request 的 stale checking 呢？

这是不行的。因为位于 (applied index, committed index] 区间内的 log entries 可能正在异步交付给 server 层。此时如果收到了一个 InstallSnapshot，其 snapshot index 为 applied index < Z < committed index，这个 snapshot 仍然会被交付给 server 层。于是 `... missing element` 仍有可能发生。

### 如何解决 `history is not linearizable` 错误？

问题描述：Get 所拿到的 value 中间缺了连续的一小段，有时候是缺了单个 Append，有时候是缺了多个 Appends。

是的，这个错误的表现与 `... missing element` 错误的表现是一致的。为什么同样表现的错误，测试脚本却会抛出不同的错误信息呢？这就需要我们稍微研究一下测试脚本。

lab3 partB 的很多测试都是共用 `GenericTest` 这个测试函数，根据测试的内容不同，不同的 test cases 在调用这个函数时会传入不同的参数。这个函数首先创建一个 kv server 集群，然后 spawn 一些 concurrent clients。这些 clients 会使用特定的或随机的 key 持续地执行 Get 或 PutAppend。在这个过程中，每一个 client 所执行的 operations 会被记录，这些 operations 执行成功后应该得到的 value 也会被记录。如果是 Put，则会 reset value；如果是 Append，则会 append value；如果是 Get，则会将 Get 拿到的 value 与目前记录的 value 对比，如果它们不想等，则会抛出 `wanted ... got ...` 错误。在执行期间，tester 会随机或有规律地产生 network partition、unreliable network、crash 等错误。

在执行一段时间之后，tester 会通知 clients 中止。tester 随后再逐个检查每个 client 是否执行正确。检查的方法是：tester 再在每一个 client 上调用一次 Get，将 Get 拿到的 value 与每个 client 所执行的 operations 应该得到的 value 进行对比。这个对比会检查 missing element, dup element, element reorder 错误。例如，如果检测到了 missing element，测试就会抛出 `... missing element` 错误。

如果这个检查过了，则会进行 linearizable 检查。那么 tester 如何进行 linearizable 检查呢？检查是否能线性化，只能从每一个 key 单独入手。因此，linearizable check 函数会首先根据 key 对所有的 operations (Put、Append、Get) 进行 group。然后并行地对每个 key 的所有的 operations 进行 linearizable 检查。检查的依据是 operations 所记录的 start, end 时刻（client 发起一个 request 的时间被记录为 start，client 拿到 reply 的时间被记录为 end）、 operation 的类型、以及 operation 的 output（对于 Put, Append，没有 output；对于 Get，output 为 value） 。一个 operation 被 server 执行的确切时间无法知晓，但是一个 operation 的执行时间点一定在 start 和 end 时刻之间。如果能为所有的 operations 都各自找到一个执行点，并且这些执行点可以串联起来，即所有的 operations 可以根据各自所选择的执行点，排在 timeline 上形成一条直线，那么就称关于这个 key 的所有 operations 能够被线性化。可能有多种执行点的选取方案，每一个方案都是可接受的。只要能找到一个，linearizable 检查就通过。但是如果一个都找不到，那么就会抛出 `history is not linearizable` 错误。

通过分析日志，我将错误发生的场景还原如下：

一个 follower 的 committed index 为 X。它收到了来自 leader 的 InstallSnapshot request，其中 snapshot index 为 Y。这个 request 理所应当地通过了 stale checking，因此 follower 立即进行 log compaction，然后把 `hasPendingSnapshot` 设为 true，并通知 committer 线程来了一个新的 snapshot。committer 线程被唤醒以后，立即把这个 snapshot 塞进一个 ApplyMsg 中，然后异步地交付给 server 层。

此时，follower 收到了来自 leader 更新的 InstallSnapshot request，其中 snapshot index 为 Z，且 Z > Y。由于之前 follower 进行了 log compaction，那么 follower 的 committed index 也为 Y，则这个 InstallSnapshot request 也理所应当地通过了 stale checking。因此，follower 按照正常流程，先执行 log compaction，再通知 committer 线程。在我的实现中，raft 层同一时间只会维护一个 pending snapshot，因此这个新的 snapshot 会替换掉旧的 snapshot。

如果旧的 snapshot 此时还没有交付给 server 层，那么这个替换没有任何问题，因为 server 层会收到更新的 snapshot。如果旧的 snapshot 此时已经交付给 server 层，那么新的 snapshot 再在随后交付给 server 层，不是一个非常正常的行为吗？乍看之下是没有问题的，但是可能出现这样一种情况：在交付旧的 snapshot 的过程中，committer 收到了新的 snapshot 的通知。由于 committer 此时并未 sleep wait，因此这个通知实际上被忽略了。当旧的 snapshot 交付完成之后，committer 重新 committer 拿到锁，把 `hasPendingSnapshot` 设为 false，然后继续 sleep wait。

问题就出现在这里。新的 snapshot 明明已经被 raft 层接收了，但是永远不会被交付给上层，因为之后如果来了更新的 snapshot，这个 snapshot 就会被覆盖。这就导致 server 层缺一些数据。

这个错误是我测试 15000 次才测出来的，在这 15000 次测试中，错误只发生了一次。因此我当时并没有足够的样本来分析错误的根源究竟是什么，于是先入为主地根据抛出的 `history is not linearizable` 信息认为错误的根源在于 linearizability 没有得到保证。但是当我阅读日志以及可视化后的 timeline 之后，我发现实际上错误还是在于 snapshot ingestion 上面，而和 linearizability 没有任何关系。之所以抛出这个错误，只是因为目前的测试脚本在进行 linearizable 检查时发现任何错误都会抛出 `history is not linearizable` 错误，以表示 tester 在 linearizable check 函数中发现错误，而不会真正地判断具体的错误是什么。

解决方法：

通过以上分析知，正是由于 committer 异步交付 apply msg 的设计以及基于 condition variable 的通知机制，才导致有些 snapshot 会被 committer 线程所忽略而不会被交付给 server 层。于是，一个很直接的解决方法是重新设计异步交付，甚至改为同步交付，或者修改通知机制，使用 channel 或让 raft 为 pending snapshot 维护一个队列。这些方法一定都是可行的。但是我思考之后，使用了这个方法：让 follower 在 `hasPendingSnapshot` 为 true 时，reject 所有的 InstallSnapshot request。这就保证了当 follower accept 一个 InstallSnapshot request 时，`hasPendingSnapshot` 一定为 false，即 committer 此时一定不是正在交付 snapshot。则新的 snapshot 的通知不会被忽略。

关于 linearizability 的讨论以及我的实现是如何保证 linearizability 的，参考 `如何保证 linearizability`。

关于 linearizable checker 的原理，参考：

[](https://pdos.csail.mit.edu/6.824/papers/linearizability-faq.txt)

[Testing Distributed Systems for Linearizability](https://www.anishathalye.com/2017/06/04/testing-distributed-systems-for-linearizability/)

### raft 层的 `Snapshot` 接口需要执行什么检查？

只需要检查 server snapshot index 大于 raft snapshot index 即可。

为什么这个检查是必要的？考虑这样一个场景：一个 follower 的 log 比集群中的其它 servers 的 log 落后，于是 leader 发送了一个 InstallSnapshot request 给这个 follower，其中 snapshot index 为 X。在 follower 收到这个 request 之前，server 层在 index = Y 处执行了 checkpoint，且有 Y < X。在 server 层调用 raft 层的 snapshot 接口之前，follower 收到了这个 InstallSnapshot request，并且理所应当地 accept 了这个 request。那么 follower 会将 snapshot index 更新到 X。之后，server 调用 raft 层的 snapshot 接口，把 snapshot index 为 Y 的 snapshot 传给 raft 层。如果没有基于 snapshot index 的 stale check，那么 raft 层就会根据一个 stale snapshot 去执行 log compaction。显然，这本身就是不正常的行为，在我的实现中也会引发很多错误。

为什么不需要检查 `hasPendingSnapshot` 是否为 true？我们要明确，当 raft 层存在一个 pending snapshot 时，raft 层肯定已经根据这个 pending snapshot 执行过 log compaction，只是尚未把这个 pending snapshot 交付给 server 层。那么 raft 层的 snapshot index 肯定已经更新到了 pending snapshot 的 snapshot index。也就是说，只要 server snapshot index > raft snapshot index，就保证了 raft 层此时没有 pending snapshot。这也就避免了 pending snapshot 被覆盖。

### 如何保证 `linearizability` ？

首先说明一下，什么是线性一致性。

参考：

[](https://pdos.csail.mit.edu/6.824/notes/l-linearizability.txt)

关于线性一致性，需要从 client 和 server 两个角度去理解。

- 对于 client：当前 op 必须发生在上一个 op 之后。注意，这里的“之后”并不代表“紧邻”。
- 对于 server：在 execute op 时保证原子性，即不会在没有并发控制的情况同时 execute 两个 op。

在实现时，常用的方法为：

- 对于 client：为 request 的 unique tag 定义一个偏序关系。例如对于 clerk id 和 op id 构成的 request tag，clerk id 可以是随机生成的，op id 则需要是严格单调分配的（可以是严格单调递增，也可以是严格单调递减）。
- 对于 server：
    - 在 apply op 时进行 concurrency control，例如加锁。
    - 为每个已知的 client 维护 max applied op id，拒绝 apply 小于或等于 max applied op id 的 ops。这也是为什么要求 op id 是严格单调分配的。

### 如何解决 `slicing out of bound` 错误？

问题描述：leader 在 make append entries RPC request 时，根据 next index 和 log last log index，对 raft log 进行 slicing 操作，此时抛出了 `slicing out of bound` 错误。

这个错误在 Go 编程中是一个比较常见的错误，表示对 slice 的 access 越界了。但是这个错误每 10000 次测试才会出现 8 次，这就表示我的实现肯定大问题没有，但是有 flaw，并且 slicing 操作绝对不是导致错误产生的根源，否则错误的出现概率不会这么低。

通过对日志的分析，我将错误的场景还原如下：

集群中有 7 个 servers，编号从 0 至 6。在测试执行过程中，出现了多次 network partition，其中与错误直接相关的有三次：

第一次 partition:

[3, 6]: 少数派。它们的 log 落后很多。

[0, 1, 2, 4, 5]: 多数派。N0 为 leader，term = 5。在重新 partition 之前，N0, N1, N4 的 last log index, committed index 和 applied index 均为 757。特别地，N0 在 index = 757 处 checkpoint 了，即其 snapshot index 为 757。N2, N5 稍微落后一点，last log index 为 757，而 committed index 和 applied index 均为 756。特别的，N2, N5 的

第二次 partition:

[0, 1, 4]: 少数派。虽然 N0 被 partition 到了少数派，但是在 step down timer timeout 之前，N0 仍然认为自己是 leader，所以它还是会接收 server 的 commands。在这段时期内，N0 的 last log index 增加到了 772。但是由于被 partition 到了少数派，N0 无法 commit 这些新的 log entries，因此 N0, N1, N4 的 committed index 和 applied index 均保持不变，即仍为 757。

[2, 3, 5, 6]: 多数派。显然，由于之前 N3, N6 属于少数派，它们的 log 落后很多。因此只有 N2, N5 其中之一会成为新的 leader。在本次测试中，N5 成为了 term 9 的 leader。在成为 leader 后，N5 马上接受了 server 的 command，将其 append 到了 index = 758 处。然后，它发送 append entries 给其它所有 servers。N2 收到了这个 append entries RPC，由于 N2, N5 的 log 在 index = 757 及之前都是同步的，因此 N2 接受了这个新的 log entry，也将其 append 到了 index = 758 处。另一方面，由于 N3 之前是少数派，log 落后很多。N5 在经过一轮 RPC 交互后，发现了 N3 lag behind，于是将一份 snapshot 发给了 N3，其中 snapshot index 为 751。在 N2 append log entry 和 N3 install snapshot 之后，马上发生了第三次 partition，因此 N5 尚未来得及 commit 任何 log entries。

第三次 partition:

[1, 5, 6]: 少数派。由于 N5 为 term 9 的 leader，因此在 N5 step down 之前，N1, N6 都以 N5 的 log 为准。这个不是接下来讨论的重点。

[0, 2, 3, 4]: 多数派。注意到，N2 在 term 9 时收到了 N5 的一个 log entry，即 N2 的 last log index 为 758，last log term 为 9。N3 在 install snapshot 之后，last log index 为 751，last log term 为 5。N0, N4 同为上个 partition 时期的少数派，因此不可能接收到 N5 的新 log entry，因此 N0, N4 虽然 last log index 在第一次 partition 期间增加到了比较高的值，但是 last log term 还是只有 5。在这种情况下，N2 理所当然成功当选为新的 term 的 leader。这个 term 是 11。

当 N2 成为 leader 后，它把 next index 初始化为 last log index + 1，即 759。之后，它 broadcast 一个 append entries RPC，其中 prev log index 为 758 。N0 收到以后，发现 index = 758 处的 log entry 的 term 为 5（即 N0 在 term = 5 作为 leader 尚未 step down 之前 append 的一个 log entry），而不是 9（即 N2 在 N5 作为 term = 9 的 leader 时收到的一个 log entry）。则 N0 的 log consistency check 会 fail，并返回 Term Not Matched。

当 N2 收到 N0 的 Term Not Matched 回复之后，N2 根据 accelerated log backtracking 的实现逻辑，在 log 中检查是否存在 term 为 conflict term = 5 的 log entries。N2 中存在 term = 5 的 log entries，即在第一次 partition 的时期，N2 收到的来自 N0 的一些 log entries。其中 index 最大的为 757。因此 N2 会将 N0 的 next index 设为 757。N2 随后又给 N0 发送了一个 append entries RPC，其中 prev log index 为 756。

当 N0 收到这个 RPC 后，发现 index = 756 处的 log entry 已经被 compacted 了，并且由于 checkpoint 点为 index = 757，因此 index = 756 的 log entry 的 term 也拿不到了。因此，N0 会回复 Index Not Matched，表示 N0 的 log 太短了，短到不足以匹配 prev log index。根据我的实现逻辑，N0 会把自己的 last log index 发送回去。由于 N0 之前 append 了很多没有被 committed 的 log entries，因此 N0 发回的 last log index 为 772。

当 N2 收到 N0 的 Index Not Matched 回复之后，根据我当时的实现逻辑，它会认为 N0 的 log 太短，然后把 N0 的 next index 设为 N0 的 last log index + 1，即 773。在发送下一个 append entries RPC 给 N0 时，N2 根据 N0 的 next index = 773 和 N2 自己的 last log index = 759 来做 log 的 slicing，此时就发生了 `slicing out of bound` 错误。

为了解决这个错误，有两种可行的办法，一种是在做 log slicing 时，对 out of bound 进行判断。如果会 out of bound，那么就返回一个 nil log entries array。即使判断出要发送的 log entries 为空，heartbeat 仍然需要根据 prev log index 拿到 prev log term，以做 log consistency checking。然而根据越界的 prev log index，根本就拿不到正确的 prev log term。在我的实现中，对于一个越界的 log index，会默认返回 term 0。

在上面的场景中，当 N0 收到 heartbeat 后，会马上判断出 prev log term = 0 与 N0 的 prev log index 处的 log entry 的 term 不一致，因为没有一个合法的 log entry 的 term 为 0。则 N0 会马上回复 Term Not Matched，且会把 conflict term 设为 5。当 N2 收到该回复以后，判断出 N2 的 log 也有 term = 5 的 log entries，因此就会把 next index 设为 N2 log 中最后的那个 term = 5 的 log entry 的 index，即 757。当 N2 再次发送 append entries RPC 时，N0 会 accept 这个 RPC，因为它们的 log 在 index = 756 及之前都是 matched。那么 N0 就会对 log suffix 进行 conflict 检测。最终把 N0  conflict log suffix 给 discard 了。

这种方法是可行的，我对其进行了 10000 次测试，没有出现一次错误。虽然其可行，但是它根据越界的 prev log index 拿到错误的 default log term 这个行为本身就不应该发生。因此，这个方法实际上是错误的行为。

通过以上分析知，错误的根源是 leader 盲目地相信 follower 发回的 Index Not Matched，在没有真正的验证 `follower's log is too short` 这个条件是否满足的情况下，就把 next index 设为了 follower 发回的 last log index。因此，只需要让 leader 在 handle Index Not Matched 时，根据 leader last log index 和 follower 发回的 last log index，对这个条件进行验证，即可从源头避免该错误的发生。如果发现 follower 的 last log index 更小，leader 才把 next index 更新为 follower last log index + 1。如果发现 leader 的 last log index 更小，那么就把 next index 更新为 leader last log index + 1。设置为 last log index + 1，是为了得到正确的 prev log index，以进行 log consistency checking。

实际上，在 log slicing 时不需要做任何越界检查。如果出现了越界错误，那么必定是其它地方实现有错误。