## Easy Job
Easy-job is a small but complete distributed task scheduling framework.It is designed to be easy to use, easy to understand and simple enough.It has made a careful choice of functions and only realizes the most basic functions.Including distributed task scheduling, elastic capacity expansion, sharding, automatic failover.It also retains a good abstraction and extension interface.

Because of its easy-to-understand nature, it is Easy to do customized development based on easy-job, which is a good starting point for the development of scheduling class tasks.It has been deployed on a number of mid-sized and larger internal projects.

Easy-Job 是一个小巧但完善的分布式任务调度执行框架。它的设计目标是**易于使用**，**易于理解**和**足够简单**。它在功能上进行了仔细取舍，只实现最基本的功能。包括分布式任务调度、弹性扩容、分片、自动故障转移。同时也保留了良好的抽象和扩展接口。

由于其易于理解的特性，在Easy-Job的基础上进行定制化开发非常容易，非常适合作为调度类任务开发的起点。它已经在多个中等规模以上的内部项目中部署。

## Quick Start
```java
    public static void main(String[] args) throws IOException {

        ZookeeperConfiguration zkConfig = ZookeeperConfiguration.builder()
                .connectString("localhost:9092")
                .build();

        JobNode jobNode1 = new JobNode("easy-job-1", 50, 2, zkConfig);
        JobNode jobNode2 = new JobNode("easy-job-2", 50, 2, zkConfig);

        JobDefinition someJob = JobDefinition.builder()
                .jobClassName(TestJob.class.getCanonicalName())
                .addParameter("someParameter", 123)
                .sharding(4)
                .build();

        jobNode1.getJobService().submit(someJob);

        System.in.read();

        jobNode1.shutdown();
        jobNode2.shutdown();

    }
```
