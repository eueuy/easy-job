## Easy Job
Easy-Job 是一个小巧但完善的分布式任务调度执行框架。它的设计目标是**易于使用**，**易于理解**和**足够简单**。

它在功能上进行了仔细取舍，只实现最基本的功能。包括分布式任务调度、弹性扩容、分片、自动故障转移。同时也保留了良好的抽象和扩展接口。

由于其易于理解的特性，在Easy-Job的基础上进行定制化开发非常容易，非常适合作为调度类任务开发的起点。

## Todo List

 * 任务状态回写repository
 * 处理puller等daemon thread堵塞的问题
 * allocation table 的事务操作
 * 多个node操作repository的并发安全问题
    * 确保repository的实现是并发安全的
    * rebalance 时要阻止对job registry 的其它修改请求。
 * metrics组件