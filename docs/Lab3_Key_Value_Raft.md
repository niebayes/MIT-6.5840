# Lab3: Fault-tolerant Key-Value Service

### 如何通过 `TestSpeed3A` 测试？

leader 在收到 server 层的 operation 后，会把它 wrap 为一个 log entry。在我之前的实现中，这个 log entry 会在下次 heartbeat timeout 时，才会被发送给 followers。这就使得 raft 层的同步 throughput 实际上与 heartbeat timeout 一致。`TestSpeed3A` 要求的最低 throughput 为每 33 ms commit 一个 log entry，然而我的 heartbeat timeout 为 150 ms，因此过不了测试。这个 heartbeat timeout 是一个比较合理的数值。另一方面，测试要求 heartbeat timeout 不能低于 100 ms，因此不能通过降低 heartbeat timeout 来强行过这个测试。

通过以上分析知，leader 收到 server 层的 operation 以后，实际上可以立刻强制 broadcast append entries，如此就可以提高 throughput。不仅如此，当 leader 收到 append entries reply 后，如果检测到 match index 提高乃至 committed index 因此而提高，那么也可以立即 broadcast append entries，以让 followers 更快地 learn committed index。还有一个可能被忽略的点是，leader 在收到 install snapshot reply 后，如果 follower 成功 install snapshot 了，那么 leader 也可以立即 broadcast append entries，以让 followers 更快地 catch up。

注意，在不加 `-race` 的情况下，执行这个测试的平均时间大概在 1s 左右。如果加了 `-race`，执行时间大概在 40s 左右，且 throughput 平均只有 35 ms 左右，不满足最低要求 33 ms。也就是说，如果加了 `-race`，我的实现是过不了这个测试的。

### executor 与 service handler 之间的通知机制应该如何设计？

在我的实现中，server 层有一个独立的 executor 线程，它负责通过 applyCh 从 raft 层拿 committed commands 或者 snapshots，然后 execute 它们。当一个 operation 被 execute 之后，executor 再通知可能正在等待的 service handlers。service handlers 收到通知之后，再回复 clients。

在讨论通知机制如何时间之前，我们先说明一些必要的事项。最首要的是，以下的讨论建立在允许 follower accept 和 reply clients 的基础上。其次，我们无法设计一种有效的机制，以避免重复 propose 同一个 request。因为即使 propose 了一个 request，raft 层也不可能保证这个 request 会被 committed。例如一个被 partitioned 的 old leader propose 了这个 request，partition 修复之后，这个 old leader 的 conflict log entries 可能会被 discard。也就是说，当 server 检测到某个 request 未被 request 时，它别无选择而只能 propose 它，不管它是不是 dup request。最后注意，这里的通知机制只与单个 server 有关，因此我们只需要考虑同一个 server 接收 requests 的几种场景：

- server 通过 max applied op id 检测到一个已经被 executed 的 dup request，server 立即 reply，不需要通知机制。
- leader 收到一些未被 applied 的 requests，则会在不同的 index 处 propose 这些 requests。
    - 这些 requests 来自不同的 clients：这是非常常见的情况，此时，clerk id 不同、op id 有可能相同、index 不同。设该情况为 A。
    - 这些 requests 来自同一个 client：
        - 这些 requests 可能为同一个 request，即它们是 dup requests，此时，clerk id 相同、op id 相同、index 不同。设该情况为 B。
        - 这些 requests 为不同的 requests。
            - leader 收到了某个 client 的属于同一个 request 的多个 dup requests，leader propose 了它们。leader 随后 execute 了这些 dup requests 中最先被 propose 的那个，然后进行了回复。client 收到回复以后，发来了第二个 request。此时，clerk id 相同、op id 有些相同有些不同、index 不同。设该情况为 C。
            - 例如一个 server 之前是 follower 时收到了来自某个 client 的一个 request，当时的 leader 已经 execute 并 reply 了这个 request。之后，这个 follower 成为了新的 leader，于是收到了来自同一个 client 的新的 request。此时，clerk id 相同、op id 不同、有些 requests 有 index 而有些没有。设该情况为 D。
- follower 收到一些未被 applied 的 requests。由于 follower 不能 propose，因此都不会有 index。除此之外，情况与 leader 一致。

最简单的通知机制当然是 sleep polling。server 会为每个 clerk 维护一个 max applied op id 查询表，当 executor 执行一个 op 以后，会更新这个查询表。service handler 会在一定的时间内，以一定的时间间隔 poll 这个查询表。如果它发现 max applied op id ≥ 自己所等待的那个 op 的 op id，那么便知道这个 op 被 execute 了。这种方法是容易实现，也最不容易出错的，并且它很显然能够应对上面所述的所有情况。

但它的效率不是很高（虽然也不低），因为它会以一定的时间间隔 poll，而 poll 时一定需要拿锁（说实话，这里不拿锁也没事，只是会有 race 错误）。并且如果一个 op 恰巧在本次 poll 之后被 execute 了，那么 service handler 最早才能在下次 poll 时发现。如果 poll interval 设置得比较大，那么显然会降低效率。

另一个使用的比较多的方法是 channel。service handler 在接收到 request 后，为这个 request 注册一个 channel，然后 service handler 就 block receive from channel。executor 执行这个 request 之后，拿到这个 channel，send to channel 以通知 service handler。为了使用基于 channel 的通知机制，我们需要考虑这么几个问题：

- 注册和获取 channel 时使用什么作为 key：
    - 只使用 op id：根本不可能，因为其无法唯一标识一个 request。
    - 只使用 clerk id：对于情况 B、C、D，无法唯一地标识一个 request。
    - 使用 clerk id 和 op id：对于情况 B、C，无法唯一地标识一个 request。
    - 使用 raft 层返回的 log index 作为 key：对于情况 D 以及 follower 相关的情况，由于没有 index，因此无法使用。注意，如果不允许 follower accept 和 reply clients，那么使用 index 是可行的。
- 如何解决 channel 的 block 问题：
    - 由于 service handler 等待时不会拿锁，因此可能会出现这么一种情况：service handler timeout 了，因此准备拿锁以获取之前注册的 channel。就在 service handler 拿到锁之前，executor 拿到锁 execute 了这个 request，然后获取之前注册的 channel。executor 会顺利拿到这个 channel，然后尝试 send to channel。由于此时 service handler 已经没有在 receive from channel，因此这个 send to channel 会 block。我并没有想到可行的方法来解决这个问题。
- 如何 close channel？channel 是一种有限资源，在用完一个 channel 后，需要及时销毁它，以回收资源。
    - 让 service handler close channel：一种做法是，使用 Go 的 select 语句，为 channel 添加 timeout 机制。当 timeout 之后，拿锁，获取之前注册的 channel，然后同步或异步地 close channel。使用 channel 的一个基本准则是只让 sender 一方去 close channel。显然，让 service handler 去 close channel 不是一个推荐的做法。
    - 让 executor close channel：应该是可行的，虽然我没有验证过。

考虑到 sleep polling 的效率不高，以及使用 channel 时需要考虑的各种问题，我选择的是 condition variable 这种通知机制。简要而言，service handler 在 propose 之后，会 sleep wait，直到 executor 通知它这个 request 已经被 execute 了。对于这种方法，我们也需要考虑一些问题。

首先，我们需要设计一种 timeout 机制。对于 channel 而言，timeout 机制可以通过 select 语句轻易实现，然而对于 condition variable，timeout 机制就比较复杂。在我的实现中，每当 service handler 注册或获取一个 condition variable 时，就会立即启动一个 alarm 线程。这个线程会在等待一段时间后，对 condition variable 进行一次 broadcast，以唤醒正在等待的 service handlers。为什么是 broadcast 而不是 signal 呢？因为 signal 的唤醒对象是不确定的（至少我不知道唤醒的顺对象和顺序是如何确定），不如使用 broadcast 来全部唤醒。

另一个需要考虑的问题就是注册和获取 condition variable 时应该使用什么作为 key。这个问题，我们在关于 channel 通知机制的讨论中，也提及过。在之前的讨论中，我们判断某种方法是否可行，其依据是该方法是否可以在各种情况中唯一地标识一个 request。为什么需要唯一标识呢？这是因为 Go 内置的 channel 只能用来 1-to-1 通信，而有些情况需要 1-to-N 通信。恰巧，condition variable 是一个非常适合进行 1-to-N 通信的机制。因此，我们的判断依据也不再是某种方法是否可以唯一地标识一个 request，而是它是否能够与 condition variable 通知机制良好地合作。

经分析，我发现使用 clerk id 作为 key 是一种可行的方法。下面就分情况讨论：

- 情况 A：由于 clerk id 不同，因此 clerk id 可以唯一地标识 requests，故可行。
- 情况 B：虽然由于 clerk id 相同，broadcast condition variable 时会唤醒所有正在等待的 service handlers，但这是一个可以接受的行为。因为这些 requests 实质上都为同一个 requests，那么只要有一个 service handler 回复了，client 就能收到 response。当然，由于网络原因，有些 response 可能会被 discard。如果我们让每个 service handler 都等待 applied 后再进行回复，确实可以更好地应对 unreliable network。但至少，这样的唤醒方式是可接受的。
- 情况 C：由于 clerk id 相同，当等待 index 最低的那个 request 的 service handler 被唤醒时，正在等待 index 较高的那些 requests 的 service handlers 也会被唤醒。这样的唤醒显然是不可接受的。为了避免这样的唤醒，我为每个 condition variable 维护了一个 max registered op id。当注册或获取 condition variable 时，更新这个 max registered op id。不管是 alarm 线程还是 executor 线程，当执行唤醒操作之后，会检查自己的 op id （在创建 alarm 线程时会传入这个参数；executor 线程在 execute op 时自然可以拿到 op id）是否为 max registered op id。如果是，才允许在注册表删除这个 condition variable。当正在等待 index 较高的那些 requests 的 service handlers 被唤醒时，它们会检查注册表中是否还有这个 condition variable，如果没有则说明自己就是此次被唤醒的对象，因此会中止 sleep wait。反之，它们会继续 sleep wait。
- 情况 D：同对情况 C 的处理。

综上，使用 condition variable 作为通知机制，同时使用 clerk id 作为注册和获取 condition variable 的 key，是一种可行的做法。

特别要说的是，使用 clerk id 作为 key，对资源的消耗是最小的。设同时最多有 N 个 clients，那么一个 server 同时最多只需要维护 N 个用于通知的 condition variables。

关于如何正确地关闭 Go 的 channel，参考：

[How to Gracefully Close Channels -Go 101](https://go101.org/article/channel-closing.html)

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

lab3 partB 的很多测试都是共用 `GenericTest` 这个测试函数，根据测试的内容不同，不同的 test cases 在调用这个函数时会传入不同的参数。这个函数首先创建一个 kv server 集群，然后 spawn 一些 concurrent clients。这些 clients 会使用特定的或随机的 key 持续地执行 Get 或 PutAppend。在这个过程中，每一个 client 所执行的 operations 会被记录，这些 operations 执行成功后应该得到的 value 也会被记录。如果是 Put，则会 reset value；如果是 Append，则会 append value；如果是 Get，则会将 Get 拿到的 value 与目前记录的 value 对比，如果它们不相等，则会抛出 `wanted ... got ...` 错误。在执行期间，tester 会随机或有规律地产生 network partition、unreliable network、crash 等错误。

在执行一段时间之后，tester 会通知 clients 中止。tester 随后再逐个检查每个 client 是否执行正确。检查的方法是：tester 再在每一个 client 上调用一次 Get，将 Get 拿到的 value 与每个 client 所执行的 operations 应该得到的 value 进行对比。这个对比会检查 missing element, dup element, element reorder 错误。例如，如果检测到了 missing element，测试就会抛出 `... missing element` 错误。

如果这个检查过了，则会进行 linearizable 检查。那么 tester 如何进行 linearizable 检查呢？检查是否能线性化，只能从每一个 key 单独入手。因此，linearizable check 函数会首先根据 key 对所有的 operations (Put、Append、Get) 进行 group。然后并行地对每个 key 的所有的 operations 进行 linearizable 检查。检查的依据是 operations 所记录的 start, end 时刻（client 发起一个 request 的时间被记录为 start，client 拿到 reply 的时间被记录为 end）、 operation 的类型、以及 operation 的 output（对于 Put, Append，没有 output；对于 Get，output 为 value） 。一个 operation 被 server 执行的确切时间无法知晓，但是一个 operation 的执行时间点一定在 start 和 end 时刻之间。如果能为所有的 operations 都各自找到一个执行点，并且这些执行点可以串联起来，即所有的 operations 可以根据各自所选择的执行点，排在 timeline 上形成一条直线，那么就称关于这个 key 的所有 operations 能够被线性化。可能有多种执行点的选取方案，每一个方案都是可接受的。只要能找到一个，linearizable 检查就通过。但是如果一个都找不到，那么就会抛出 `history is not linearizable` 错误。

通过分析日志，我将错误发生的场景还原如下：

一个 follower 的 committed index 为 X。它收到了来自 leader 的 InstallSnapshot request，其中 snapshot index 为 Y。这个 request 理所应当地通过了 stale checking，因此 follower 立即进行 log compaction，然后把 `hasPendingSnapshot` 设为 true，并通知 committer 线程来了一个新的 snapshot。committer 线程被唤醒以后，立即把这个 snapshot 塞进一个 ApplyMsg 中，然后异步地交付给 server 层。

此时，follower 收到了来自 leader 更新的 InstallSnapshot request，其中 snapshot index 为 Z，且 Z > Y。由于之前 follower 进行了 log compaction，那么 follower 的 committed index 也为 Y，则这个 InstallSnapshot request 也理所应当地通过了 stale checking。因此，follower 按照正常流程，先执行 log compaction，再通知 committer 线程。在我的实现中，raft 层同一时间只会维护一个 pending snapshot，因此这个新的 snapshot 会替换掉旧的 snapshot。

如果旧的 snapshot 此时还没有交付给 server 层，那么这个替换没有任何问题，因为 server 层会收到更新的 snapshot。如果旧的 snapshot 此时已经交付给 server 层，那么新的 snapshot 再在随后交付给 server 层，不是一个非常正常的行为吗？乍看之下是没有问题的，但是可能出现这样一种情况：在交付旧的 snapshot 的过程中，committer 收到了新的 snapshot 的通知。由于 committer 此时并未 sleep wait，因此这个通知实际上被忽略了。当旧的 snapshot 交付完成之后，committer 重新拿到锁，把 `hasPendingSnapshot` 设为 false，然后继续 sleep wait。

问题就出现在这里。新的 snapshot 明明已经被 raft 层接收了，但是永远不会被交付给上层，因为之后如果来了更新的 snapshot，这个 snapshot 就会被覆盖。这就导致 server 层缺一些数据。

这个错误是我测试 15000 次才测出来的，在这 15000 次测试中，错误只发生了一次。因此我当时并没有足够的样本来分析错误的根源究竟是什么，于是先入为主地根据抛出的 `history is not linearizable` 信息认为错误的根源在于 linearizability 没有得到保证。但是当我阅读日志以及可视化后的 timeline 之后，我发现实际上错误还是在于 snapshot ingestion 上面，而和 linearizability 没有任何关系。之所以抛出这个错误，只是因为目前的测试脚本在进行 linearizable 检查时发现任何错误都会抛出 `history is not linearizable` 错误，以表示 tester 在 linearizable check 函数中发现错误，而不会真正地判断具体的错误是什么。

解决方法：

通过以上分析知，正是由于 committer 异步交付 apply msg 的设计以及基于 condition variable 的通知机制，才导致有些 snapshot 会被 committer 线程所忽略而不会被交付给 server 层。于是，一个很直接的解决方法是重新设计异步交付，甚至改为同步交付，或者修改通知机制，使用 channel 或让 raft 为 pending snapshot 维护一个队列。这些方法一定都是可行的。但是我思考之后，使用了这个方法：让 follower 在 `hasPendingSnapshot` 为 true 时，reject 所有的 InstallSnapshot request。这就保证了当 follower accept 一个 InstallSnapshot request 时，`hasPendingSnapshot` 一定为 false，即 committer 此时一定不是正在交付 snapshot。则新的 snapshot 的通知不会被忽略。

关于 linearizability 的讨论以及我的实现是如何保证 linearizability 的，参考 `如何保证 linearizability`。

关于 linearizable checker 的原理，参考：

[MIT 6.824 Linearizability FAQ](https://pdos.csail.mit.edu/6.824/papers/linearizability-faq.txt)

[Testing Distributed Systems for Linearizability](https://www.anishathalye.com/2017/06/04/testing-distributed-systems-for-linearizability/)

### raft 层的 `Snapshot` 接口需要执行什么检查？

只需要检查 server snapshot index 大于 raft snapshot index 即可。

为什么这个检查是必要的？考虑这样一个场景：一个 follower 的 log 比集群中的其它 servers 的 log 落后，于是 leader 发送了一个 InstallSnapshot request 给这个 follower，其中 snapshot index 为 X。在 follower 收到这个 request 之前，server 层在 index = Y 处执行了 checkpoint，且有 Y < X。在 server 层调用 raft 层的 snapshot 接口之前，follower 收到了这个 InstallSnapshot request，并且理所应当地 accept 了这个 request。那么 follower 会将 snapshot index 更新到 X。之后，server 调用 raft 层的 snapshot 接口，把 snapshot index 为 Y 的 snapshot 传给 raft 层。如果没有基于 snapshot index 的 stale check，那么 raft 层就会根据一个 stale snapshot 去执行 log compaction。显然，这本身就是不正常的行为，在我的实现中也会引发很多错误。

为什么不需要检查 `hasPendingSnapshot` 是否为 true？我们要明确，当 raft 层存在一个 pending snapshot 时，raft 层肯定已经根据这个 pending snapshot 执行过 log compaction，只是尚未把这个 pending snapshot 交付给 server 层。那么 raft 层的 snapshot index 肯定已经更新到了 pending snapshot 的 snapshot index。也就是说，只要 server snapshot index > raft snapshot index，就保证了 raft 层此时没有 pending snapshot。这也就避免了 pending snapshot 被覆盖。

### 如何保证 `linearizability` ？

首先对一致性进行简要的说明。一致性是一个 specification（说明，或称要求），指的是对于不同的 clients，service 的 view 应该是怎样的。形象一点的说法，多个人去观察同一个物体，他们眼中的这个物体应该是怎样的。

在一个单机单线程系统中，我们可以很简单地确认系统在任意时刻的状态。在一个单机多线程系统中，由于并发、并行，我们比较难确认系统在任意时刻的状态。而对于一个分布式系统，考虑到 unreliable network, partial failures, async clock, cache, replicas 等因素，要完全确认系统在任意时刻的状态，是一件不可能的事。

在一个不确定的系统之上，构建自己的应用、写程序，很难保证正确性。因此，对于分布式系统，我们通常会要求它们满足某种一致性要求（或称模型），使得它们符合某些业务的要求、以及使得开发者与分布式系统的交互更容易。

一些一致性模型为：

- eventual consistency
- causal consistency
- serializability
- sequential consistency
- linearizability

满足不同一致性模型的分布式系统，在 performance, convenience, robustness 方面的表现不一样。需要根据业务需求和对程序员的友好程度进行选取。

现在对线性一致性进行简要的说明。对于每个 client 所发送的每一个相同的 request，有 invocation time 和 response time，前者表示 client 第一次发送 request 的时间，后者表示 client 第一次收到 response 的时间。注意这里的 response 表示的是收到正确的 response。对于 PutAppend，为 OK。对于 Get，为 value 或者 ErrNoKey。对于 service 而言，不可能知道某个 request 被 execute 的确切时间，因此只能认为这个 request 在 invocation time 与 response time 之间的某个时间点被 execute。对于每一个 key，如果所有 clients 所被执行的、关于这个 key 的 operations，各自能够在 invocation time 和 response time 之间找到唯一一个执行点，然后把这些执行点串起来，能够形成一条随时间增长的、没有任何回路的直线，我们就称关于这个 key 的、所有 clients 所被执行的 operations 是 linearizable 的。这些 operations 统称为关于这个 key 的 history operations，或简称 history。如果每个 key 的 history 都是 linearizable 的，我们就说这个分布式系统在本次运行过程中是 linearizable 的。

以上只是告诉我们，当有了 history 时，如何判断 history 乃至整个系统的运行是否是 linearizable 的。换句话说，以上只是告诉了我们验证 linearizability 的方法。那么我们应该怎么实现一个系统，使得它满足 linearizability 呢？在 raft 论文中，对于 linearizability，有这样一句话：`every operation appears to execute atomically and instantaneously at some point between the invocation and response`。这句话实际上就告诉我们应该保证两个 properties：each operation executes atomically, each operation executes instantaneously。当这两个 properties 得到保证时，系统就是一个 linearizable 系统。

把这句话用 client 的角度重述，有：对于一个 client 在时刻 t1 发出、时刻 t2 收到 response 的 Get operation，Get 的 value 中应该包含所有在 t1 时刻之前被 execute 的 Put, Append。这里的包含意为：这些 Put, Append 只会在这个 value 中出现唯一的一次。也就是说，如果一个 Put, Append 在此之前被 execute 了，它必须出现一次，不能不出现，不能出现多次。这样的重述就是我们一般情况下进行 linearizability 判断的纲领。通过这个纲领，我们可以回答以下两个问题：

为什么需要保证 `atomically` ？假设 server 几乎同时收到了来自不同 clients 的、关于同一个 key 的两个 Appends，如果没有并发控制，那么其中一个 Appends 可能 lost，即发生很常见的 lost write 或称 lost update 并发错误。在 clients 收到两个 Appends 的 responses 后，某个 client 又发送了一个 Get。如果这是一个 linearizable 系统，client 应该看到之前的两个 Appends，因为这两个 Appends 的 response time 都在 Get 的 invocation time 之前，则它们的 execute time 必定都在 Get 的 execute time 之前。但是由于有一个 Append 丢失了，这个 Get 拿到的 value 中实际上只包含一个 Append。这就不满足 linearizability。

为什么需要保证 `instantaneously` ？instantaneously 的意思是，每个 operation 有且仅有唯一的一个执行点，不能是 0 个，也不能是多个，只能是 1 个。换句话说，当某个 client 收到某个 request 的 response 时，client 可以得到 server 执行这个 request exactly once 的保证。考虑一个保证 exactly once 语义的系统，client 发送一个 Append 给 server，server 由于某些原因执行了这个 Append 两次。当 client 收到 response 后，它又发出了一个 Get。理论上，这个 Get 的 value 应该只包含一个 Append，但实际上其中包含了两个 Appends。这也违反了 linearizability。

讨论了这么多，现在就具体到如何为这个 lab 实现一个 linearizable key-value service。对于 atomically，最简单的方法就是让 server apply operation 到 state machine 时，进行加锁。对于 instantaneously，即 exactly once 语义，维护一个关于 operations 的去重表。去重表的具体实现方法不赘述。

### 为什么不需要为 Get operation cache value？

在有些实现中，当 server apply Get operation 时，会把此时拿到的 value 记录在一个数据结构中，待之后通知 service handler 这个 operation 已经 applied 时，交付给 service handler，最终发给 client。

有些人认为不这样做，会破坏 linearizability。实际上，这样的 cache value for Get operations 是不会破坏 linearizability 的。如果不 cache value，那么当 service handler 得到 operation 已经被 applied 的通知后，service handler 会在加锁的情况下执行 Get operation，然后回复 client。显然，与 cache value 的做法对比，虽然 Get operation 的执行时间点往后延了一点，但是 atomically 和 instantaneously 两个 property 依然得到满足。

是的，非常有可能在 apply Get operation 时拿到的 value 与 service handler 执行 Get operation 拿到的 value 是不一致的。比如在这个 time gap，server apply 了来自其它 clients 的 Put, Append。甚至有可能由于 dup requests，多个 service handler 在不同的时间点执行同一个 Get operation。但这些都不影响线性一致性。

当任意一个 service handler 执行 Get operation 时，client 有且仅有两个状态：

- client 已经收到了这个 Get operation 的 reply：那么这次执行Get operation 拿到什么 value 无所谓，因为 client 已经不需要了。
- client 还没有收到这个 Get operation 的 reply：由于没有收到过 reply，此次执行的 Get operation 所拿到的 value 就是 client 将要看到的那个 value。

也就是说，从 client 的角度来看，它自始至终只会看到唯一的一个 value。并且这个 value 必定是包含了所有之前被 execute 的 Put, Append。如果只有一个 client，那么这个 value 就会包含所有由这个 client 所发出的 Put, Append。如果有 concurrent clients，那么这个 value 就会包含所有在之前被 execute 的 Put, Append。从这个 client 看来，这个 Get operation 就是只 execute 了一次，因此 instantaneously 得到满足。

### 为什么 follower 也可以 accept 和 reply clients？

这也是一个与 linearizability 相关的问题。注意，依然只有 leader 能够 propose requests 给 raft，但是 follower 也可以 accept 和 reply clients。

在有些实现中，follower 会 reject client requests。不仅如此，如果 server 收到 requests 是 leader，但是回复 clients 之前不再是 leader 或者 leader term 改变了，server 也不会 reply clients。

实际上，允许 follower accept 和 reply clients，并不会影响 linearizability，只要我们保证 follower 当且仅当检测到这个 request 已经被 apply 了就行。对于 Put, Append，显然 leader 和 follower 都可以 reply clients。对于一个 Get operation，考虑 leader 执行的时刻为 X，但是由于网络原因 client 并没有收到 leader 的 reply。随后 client 可能把 request 重发给某个 follower，follower 收到之后检测到这个 request 已经被 apply 了，于是 service handler 自行执行 Get operation，而不经过 raft 层。设这个执行的时刻为 Y。

很有可能，在 (X, Y) 这段时间内，server 执行了其它 clients 发来的 requests，导致两次 operation 拿到的 value 不一致。但是这并不影响 linearizability，这个我们已经在 `为什么不需要为 Get operation cache value？` 中讨论过了。

设想这样一个场景，leader 在执行 Get operation 后、回复 clients 之前 crash 了，那么 cached 的 value 也没了，因为通常不会持久化 cached value。那么以后不管是 leader 还是 follower 收到同一个重发的 request，它们再次执行这个 Get operation 时，所拿到的 value 也非常可能与第一次拿到的 value 不一致。这种情况是我们无法避免的，而这种情况与 follower 在 leader 之后再次执行 Get operation 相比，本质上是相同的。从另一方面来说，Get operations 对于 state machine 的 state 而言，是幂等的，即执行任意多次某个 Get operation，都不会改变 state machine 的 state。

思考：那么能不能让 follower 在尚未检测到 Get operation 已经被 apply 的情况下，也回复 clients 呢？

这是不行的，会破坏线性一致性。考虑一个 client 在发送 Put 且拿到 response 之后，又发送了一个 Get。如果系统是 linearizable 的，且假设 server 在此期间没有 execute 其它 operations，那么这个 Get 一定会拿到之前 Put 的 value。假设此时是一个 lag behind 的 follower 拿到这个 Get operation，由于它还没有 execute 之前的那个 Put operation，因此 Get 所拿到的 value 并不是之前 Put 的 value。显然，这明显破坏了线性一致性，因为本应该 execute exactly once 的 Put operation，在 client 的 view 中实际上并没有被 execute。

实际上，一个 linearizable 系统通常会有一个严格的 serial component，只要一个 operation 在被 reply clients 之前进入了这个 serial component，那么线性一致性就可以被保证。在我的实现中，所有的 operations，不管是 Put, Append，还是 Get，它们都被传给 raft 层进行共识。而 raft 层维护了一个 consistent 的 replicated log，由于 replicated log 可以被抽象为一个严格有序的数组，因此执行这些 operations 都不会破坏线性一致性。反之，如果 Get operation 不经过 raft 层就被执行，则会破坏线性一致性。

关于线性一致性的定义，参考：

[MIT 6.824 Linearizability](https://pdos.csail.mit.edu/6.824/notes/l-linearizability.txt)

工业界，为了提高 throughput，通常不会让 Get operations 经过 raft 层。这就需要其它手段来保证线性一致性。参考：

[How TiKV Uses "Lease Read" to Guarantee High Performances, Strong Consistency and Linearizability | PingCAP](https://www.pingcap.com/blog/lease-read/)

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