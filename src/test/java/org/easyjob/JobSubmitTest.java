//package org.easyjob;
//
//import org.easyjob.zookeeper.ZookeeperConfiguration;
//
//public class JobSubmitTest {
//
//
//    public static void main(String[] args) throws InterruptedException {
//
//        ZookeeperConfiguration zkConfig = ZookeeperConfiguration.builder()
//                .connectString("10.201.3.102:2181,10.201.3.103:2181,10.201.3.104:2181")
//                .build();
//
//        JobNode jobNode1 = new JobNode("easy-job-dev6", "zy-desktop1", 200, 2, true, zkConfig);
//        JobNode jobNode2 = new JobNode("easy-job-dev6", "zy-desktop2", 200, 2, true, zkConfig);
//        JobNode jobNode3 = new JobNode("easy-job-dev6", "zy-desktop3", 200, 2, true, zkConfig);
//
//        Thread.sleep(5000);
//
//        try {
//
//            for (int i = 0; i < 100; i++) {
//                JobDefinition testJob = JobDefinition.builder()
//                        .jobClassName(TestJob.class.getCanonicalName())
//                        .addParameter("testPara", 123)
////                    .sharding(4)
//                        .build();
//
//                jobNode1.getJobService().execute(testJob);
//                System.out.println("------- continue ?");
//                System.in.read();
//            }
//
//            System.out.println("over");
//
//        } catch (Exception e) {
//            System.err.println(e);
//        } finally {
//            jobNode1.shutdown();
//            jobNode2.shutdown();
//            jobNode3.shutdown();
//        }
//
//    }
//
//    public static class TestPartitionJob extends Job {
//
//        @Override
//        public void onInit() {
//
//        }
//
//        @Override
//        public void onStart() {
//
//        }
//
////        @Override
////        public void onStart(int shardNumber,int totalShard) {
////            switch (shardNumber) {
////                case 0:
////                    // do something by sharding item 0
////                    break;
////                case 1:
////                    // do something by sharding item 1
////                    break;
////                case 2:
////                    // do something by sharding item 2
////                    break;
////                // case n = totalShard: ...
////            }
////        }
//
//        @Override
//        public void onStop() {
//
//        }
//
//        @Override
//        public void onFinished() {
//
//        }
//    }
//
//
//}
