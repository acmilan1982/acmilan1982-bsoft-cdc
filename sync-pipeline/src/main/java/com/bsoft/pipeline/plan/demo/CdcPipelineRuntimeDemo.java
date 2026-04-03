package com.bsoft.pipeline.plan.demo;

public class CdcPipelineRuntimeDemo {

    private static final String ORACLE_URL = "jdbc:oracle:thin:@10.16.22.30:1521/orcl";
    private static final String ORACLE_USER = "CDC";
    private static final String ORACLE_PASSWORD = "CDC";

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        PlanContext context = new PlanContext(tableEnv);
//        PlanNodeFactory factory = CdcPipelinePlanNodeFactoryBuilder.build();
//        PlanExecutor executor = new PlanExecutor(factory);
//
//        CdcPipelinePlanRepository repository = new CdcPipelinePlanRepository();
//        List<RulePlan> plans = repository.loadRulePlans(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD);
//        if (plans.isEmpty()) {
//            throw new IllegalStateException("CDC_PIPELINE is empty");
//        }
//
//        for (RulePlan plan : plans) {
//            executor.execute(plan, context);
//        }
//
//        env.execute("cdc-pipeline-runtime");
    }
}

