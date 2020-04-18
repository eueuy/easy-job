## Easy Job
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
