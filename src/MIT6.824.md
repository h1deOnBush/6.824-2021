# MIT6.824

## Lab1(MapReduce)

### Paper

#### 流程：

1.将输入数据分成M份
2.master挑选一个空闲的worker来执行map task和reduce task.共有M个map task和R个reduce task
3.执行map的worker将执行的中间结果存在内存并周期性写入到磁盘中，根据partition函数进行划分为R个文件
4.reduce worker读取了中间结果后先对键值对进行排序再进行聚合
5.每个reduce worker执行后的结果输出到一个文件中，共有R个输出文件
6.通常不用将reduce得到的最终结果合并成一个file，他们通常作为输入文件传递到下一个MapReduce中去

### Master Data Structures:

1.tasks state(idle, in-progress, completed)
2.worker identity(map worker | reduce worker)
3.对每个完成的Map task，master存储中间结果的大小和地址

#### 容错：

##### worker failure:

1.master定期向worker发送心跳，在失效的worker上执行的task将重新回到idle状态，

### Instruction

1.master和worker通过RPC通信，worker向master请求task
2.每个worker从一个或多个文件读取数据，执行task将结果输出到一个或多个文件
3.master发现worker在10s内还没完成任务时，将任务分配给别的worker
4.实现mr/master.go、mr/rpc.go、mr/worker.go
5.map阶段产生的中间结果需要划分到nReduce个中间文件中去,中间文件存在当前目录下,中间文件的命名规则为mr-X-Y,X是map task num，Y是reduce task num，中间文件采用json格式编码
6.map阶段得到的中间结果可以通过ihash(key)来选择放到哪个reduce task中去执行
7.第X个reduce得到的最终输出文件为mr-out-X
8.reduce需要等到所有的map执行完毕后才能开始执行，可以通过worker自己每次请求时进行sleep实现，也可以通过master的处理请求goroutine阻塞来实现
9.为了防止观测由于goroutine中途退出而导致只写了部分的输出文件，可以使用ioutil.TempFile来创建临时文件mr-tmp/mr-tmp-*并最后将其重命名为mr-out-*
10.master收到Done()返回的true则结束，worker可以通过Call()来连接master，当连接失败则认为所有任务完成进行退出，或者master给worker发布一个请停止执行的任务

### My Implement

#### data structure

- master维护idle task channel、completed task channel以及inProgress task map.
-  task包括task类型、所要读取的文件名、以及开始时间、task id

#### process

master启动之后，根据传入的文件名参数和reduce数量参数初始化m个处于idle状态的map任务和r个处于idle状态的reduce任务，将其入队。worker通过rpc向master请求任务，当成功请求到任务时则进行执行并将中间文件存储在磁盘中，master将分配出去的任务在inProgress map中记录下来，当worker成功执行完任务后master将该任务的inProgress置false将该任务记录到completed channel中，若master发现一个任务10s内还没有完成，则将该任务重新置为idle状态并分配给其他worker。只有等所有的map任务都完成后，reduce任务才能开始执行。

## GFS

### Paper

#### 流程：

1. 客户端通过GFS的api将文件名和偏移量转换为chunk索引.
2. 客户端将文件名和chunk索引发送给master.
3. master将chunk标识和所请求的文件副本位置发给客户端
4. 客户端将master传回的元信息进行缓存，以文件名和chunk索引作为key
5. 客户端向最近的chunk server发起请求，请求信息包含chunk的标识和字节范围. 除非缓存的元数据信息过期或文件被重新打开，否则不需要再与master通信
6. 客户端通常在一次请求中查询多个chunk信息，master节点的回应可能包含了紧跟着被请求的chunk后的chunk信息

### Class notes

分布式系统的初衷是为了获得巨大的性能，因此将数据分片到成百上千台服务器中并行获取数据(sharding)

机器很容易出现故障，因此需要容错机制

为了容错需要对数据分片进行复制，因此需要保证replication的一致性

为了保证一致性，性能就降低了

lease

## Raft

raft是一种用于管理复制日志的分布式共识算法。

和其他共识算法相比比较新颖的特性：

- **Strong leader:**  log entries仅从leader传向follower
- **Leader election:**  使用随机计时器来选取leader，仅用在之前的心跳机制上加上很小的改动就能快速简单解决冲突
- **Membership changes: **使用new joint consensus当集群中的成员改变了的时候。通过two-phase来实现

工业界的共识算法通常有以下机制：

- 保证安全，在网络延迟、丢包等情况下也不会返回不正确的结果
- 当多数机器能正常工作时，服务就能正常进行
- 不依靠时钟保证log的一致性
- 当多数机器完成任务时任务就被完成，不用等所有机器完成任务

### Raft三阶段

- **Leader election: **当存在的leader fail时必须选举新的leader
- **Log replication: ** leader从client接收log entry并将其复制给follower
- **Safety: **当一个server在他的状态机中应用了一个log entry后，其他的server不能在相同的log index位置处应用不同的log entry

#### Leader election:

##### paper:

Raft集群中通常有5台机器

- 每个节点都有一个term，term会随着时间递增。当服务器之间进行通信的时候，当发现别的term更大的时候，更新自己的term成大值。若candidate或leader发现自己的term比别的server小，则其立即变为follower。follower在收到term比自己小的请求时立即丢弃。
- 当follower在一定时间内没有收到leader 的心跳信息时，则该follower认为没有leader并开始进行新一轮的选举。该follower首先将自己的term+1并进入candidate状态。接着该follower首先为自己投票，接着**并行**向其他节点发送投票的RPC请求。结果有三种可能
  1. 收到足够多的选票。成功当选leader，立刻向其他节点发送心跳信息来告诉其他节点自己是leader并且防止新的选举进行
  2. 选举过程中收到其他节点发来心跳信息宣布其是leader，如果该leader的term大于等于自己的term，则自己退回follower状态。若该leader的term小于自己的term，则继续进行选举。
  3. 没收到足够多的选票也没有收到leader的心跳。出现多个candidate竞争，没有candidate获得大多数选票，因此每个candidate等到timeout后开始新一轮的选举，选票分散的情况可能会一直延续

Raft通过随机选举时间来保证选票分散的情况不会经常出现(150~300ms)

1.server启动后默认为follower.

2.follower等待election timeout的时间，若收到requestVote或AppendEntry的请求，重新计时。

3.等待时间已过，开始选举。将自身term加一，更改状态为candidate，为自己投票，并行向其他server发送投票请求。





##### hints:

- 将figure2关于leader election的state成员加到Raft struct中去，同时需要定义log entry相关的struct
- 发心跳的时间间隔不会超过1秒10次
- 选取合适的election timeout(150~300ms,可能要更大)和append entry time(>100ms小于elction timeout)使得能够在5s内完成选举

##### my implement

![leader election](/home/zhouhao/go/src/6.824/src/raft/leader election)



#### Log replication

**总体流程：**client向leader发送command，leader将其封装为一个entry加入自己的log中，随后将其复制给所有follower，当成功收到超过半数的成功复制的响应后，leader将该entry提交到状态机，之后发送成功响应给客户端，然后告诉follower将该entry提交到状态机

**细节**：

不同的节点之间的logs中，若有两个entry的有相同的index和term，则存储的是同一个命令，且logs中该entry之前的entries也都相等。

1. 在进行投票时，会带上候选人的最后一个entry的index和term，当被要求投票的server发现自己比候选人的logs要up-to-date时，就拒绝投票，否则进行投票。比较方式为最后一个entry的term不同时，term大的更up-to-date，最后一个entry的term相同时，index更大(即更长的)更up-to-date
2. 客户端每向leader提交一个command，leader先将其作为一个log entry添加到自己的log中，每个log entry记录自己被创造时的term
3. leader将该log entry发给所有follower，当超过半数follower完成复制(将log entry添加完成后并且leader收到完成响应)时，leader将该log entry进行commit，并记录下最新commit的log entry的index，这个index会在leader下一次向follower发送append entry请求时带上，当follower收到这个index时知道要将该entry进行commit了，方便leader和没有完成写入的follower达到最终一致性
4. leader向follower发送log entry时，会带上新的entry前一个entry的index和term，若follower在其log entry中没有找到匹配index和term的entry，则拒绝追加该entry
5. 当一个entry成功被leader复制到大部分server上后，则该entry成为commited状态，这次提交同时也会将该entry之前的entry一起提交
6. logs[0]作为哨兵节点



leader在收到client的command请求后，首先将其封装成一个log entry，接着将其加入到自己的log中去，接着在下一次心跳时，leader将会检查自己和follower一致的index最大的一个entry，接着将那之后的entry一起发送给follower，当leader发现超过半数的server成功复制了entry后，则进行提交并进行apply到状态机，当follower收到leader的Append Entry请求时，若发现该请求所带的commitIndex大于自己的commitIndex，则follower将提交部分entry，其将commitIndex设置为leader的commitIndex和自己的log中长度较小的一个并进行提交。server提交一个entry之后开一个goroutine将其进行apply到状态机。每次在更新commitIndex的同时进行apply，这样做与单独开一个ApplyLoop的优劣对比是：减小了性能上的开销，但是无法及时apply。



#### Log Compaction

##### 目的

raft节点的log不能无限扩张, 否则在复制的时候需要更多的时间和空间，因此需要对日志进行压缩，snapshot机制是日志压缩最简单的机制。

##### Snapshot

Raft节点在log太大时就要求上层service做快照，在本次实验中定时做快照，做完快照后更新自己的状态，同时持久化snapshot和snapshot后的log。

Raft的follower节点在收到leader节点发来的snapshot后，通知上层service将状态迁移到snapshot的状态，同时对自己的log进行处理。

每个节点包含各自的snapshot，snapshot中仅仅包含已提交到状态机的log。snapshot中包含最后提交的日志的index和term，当前状态机的状态。当完成snapshot之后，已包含在snapshot中的log就全部丢弃。

通常情况下snapshot会包含follower没有的log entry，在这种情况下follower将会丢弃所有的log并用snapshot来取代(可能会有未提交的entry和snapshot矛盾)。

如果发来的snapshot里的entry follower都有，那么follower将删除snapshot中含有的entry并保留之后的entry

##### snapshot的时机

**写snapshot的时机：**比较简单的策略是在log的size到达了特定的bytes数时进行snapshot。

**snapshot同步的时机：**leader需要在follower的log落后于自己太多时向其发送snapshot，发生的时机在当leader完成一次snapshot并丢弃掉之前的log entry时，其中的log entry恰好包含还未复制给某个follower的entry，这时leader需要向follower发送快照。

##### 细节

1.上层服务通过Snapshot/CondInstallSnapshot两个接口和Raft节点进行通信

2.follower通过InstallSnapshot接收来自leader的snapshot，将snapshot封装成ApplyMsg发送给上层service，上层service通过CondInstallSnapshot

 

![](/home/zhouhao/桌面/未命名文件.png)



## KV server

1. 客户端通过clerk和服务端进行通信，一个客户端关联一个clerk。
2. 一个kv server和一个raft peer相关联，kv server之间通过raft peer进行通信。
3. clerk收到客户端发来的请求时，寻找与raft leader相关联的server请求执行命令，若找到的server不是leader，则随机请求下一个sever进行服务
4. server收到clerk发来的请求，首先将该命令传给raft peer令其复制给其他raft节点，在该命令成功apply后，检查该命令是否执行过，接着根据命令修改数据库，返回客户端，告知其执行结果





问题：

leader向follower发送快照，follower的snapshotindex小于leader的snapshotindex，但是follower的lastappied比leader的snapshotindex大，因此在安装快照时follower通知上层服务安装快照，从而上层服务的db丢失数据

## Sharded KV service

### 介绍

**shard:**全部kv pair的一个子集，如key以a开头的kv pair, shard的目的是为了提高性能.

**replica group:**一组基于raft的kv server

**configuration:**client向shardmaster查询一个key对应的replica group，replica group向shardmaster查询自己要服务哪些shards

一个replica group负责服务若干个shard的更新查询。

sharded storage system必须能够更改配置，即能够更换服务shards的replica group，一方面是为了replica group的负载均衡，另一方面是replica group可能会加入或退出系统。

将reconfiguration作为一个log entry进行同步是进行配置shift的一个办法。对每个shard最多只有一个replica group在处理client的请求。

reconfiguration需要replica groups之间进行交互，如进行shard内容的迁移。



### lab4A总结

一个shardcontroller中存储着分片的历史配置列表，一个shardcontroller与一个raft peer关联，配置中存着当前每个shard由哪个group负责，每个group中含有哪些server。shardcontroller支持4种操作，客户端可以通过Query指令查询指定的配置信息，管理员可以通过Join和Leave指令添加和删除groups，在执行完Join和Leave指令后，shardcontroller会对shard进行平衡，保证不同group负责的shard数绝对值不超过1来保证平衡，还可以通过Move指令将一个group负责的shard转移给另一个group负责。配置信息的更改会被当做一条command entry先同步到raft节点的日志中，在raft节点同步成功后，进行真正的配置修改操作





### lab4B总结

问题：

1.group中的非leader节点不更新配置，主要是从0到1的配置无法进行更新成功，将shardsOut和shardsIn的更新放在了leader节点中(solved，将shardsOut和shardsIn的更新放在处理applyMsg时)

2.leader更新太快，由配置3->4，而follower更新速度不够快，跟不上配置由2->4，无法更新(solved)

3.不需要从其他服务器pull data的server能够立马完成配置的更新并且放弃对某shard的访问请求的处理，而需要从该服务器pull该shard对应的data的服务要等到pull data成功后才能完成对配置的变更并负责处理该shard的访问请求，因此在此期间无法对该shard进行访问

4.G1在Config1的时候拥有Shard1，在收到append shard1的请求后，收到配置变更的消息，在下个配置中shard1不再属于G1，于是立马更新shardout和shardsIn保存当前的shard1，而在append成功后shard1发生了变化，G2从G1处拉取Shard1的时候得到的是未append前的shard1，从而丢失了append的信息（通过在apply之后再进行check是否应该对其负责引出问题6）(solved, 更换机制，leader在发现有新的config时，根据新的config更新可以提供服务的shards列表，在apply config成功后，更新shardsOut和shardsIn，开始从别的group拉取数据，在拉取到数据时更新可以提供服务的shards列表，拉取完所有数据后进行配置的迁移)

5.server重启之后配置消失，要从0开始更新配置，并且数据清零，因此需要进行persist一些状态。(solved)

6.leader更新到Config2之后开始负责Shard1数据的更新并将其apply给follower，然而follower还未更新Config从而拒绝Shard1的更新。(solved)

7.客户端能拉取到最新配置，所以客户端会正确将数据更新到对应的gid服务器上，如果G1在Config1持有shard1，G2在Config2持有shard1并做出修改，G1在Config3重新持有shard1，则在unreliable情况下get(shard1)在Config1的结果和get(shard1)在Config3的结果不同，若G1卡在Config1，则get到的结果就不正确。(未验证正确性)

8.G1在Config1时持有Shard1，G2-leader从G1拉取Shard1后更新到Config2，G2-leader对Shard1进行更新，G2-follower1仍处于Config1，但是收到G2-leader拉取并更新的Shard1，于是对Shard1进行存储。服务器进行重启，G2-follower1成功当选leader，存储的状态仍然处于Config1，并且在Config1就拥有了更新过的Shard1，之后G2-follower1发现Config2于是G1拉取未更新的Shard1覆盖了之前的更新。(solved，配置变更时保证了配置变更和更新shard的因果性)

9.leader发给follower的Config只发送一次，不管follower是否跟的上leader的Config变更速度，当leader发现Config3的时候通过raft进行同步Config3,而此时follower还处在Config1，此时收到的Config3无法进行更新，于是follower永远卡在Config1。(solved，将判断配置变更的逻辑移到拉取shard成功时，每次拉取shard成功时检测是否还有需要拉取的shard，若没有则进行shard变更)