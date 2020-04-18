## Todo List

 * 任务状态回写repository
 * 处理puller等daemon thread堵塞的问题
 * allocation table 的事务操作
 * 多个node操作repository的并发安全问题
    * 确保repository的实现是并发安全的
    * rebalance 时要阻止对job registry 的其它修改请求。
 * metrics组件