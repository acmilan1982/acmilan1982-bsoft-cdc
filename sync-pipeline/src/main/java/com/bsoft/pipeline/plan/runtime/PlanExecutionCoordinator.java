package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.plan.scn.SnapshotScnProvider;
import com.bsoft.pipeline.plan.state.PipelinePlanPhase;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;
import com.bsoft.pipeline.plan.state.PipelinePlanRunStateRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * PlanExecutionCoordinator 是新的执行总协调器。
 *
 * 这版职责和旧版不一样，核心思想是：
 * 1. snapshot 和 incremental 不再按“单个 plan 跑完整条链路”处理；
 * 2. snapshot 阶段按 data_source_id 分组：
 *    - 不同源库并行
 *    - 同一源库串行
 * 3. incremental 阶段目标是“一个总 job”，
 *    只把已经完成 snapshot 的 plan 纳入增量集合。
 *
 * 为什么这样设计：
 * - 生产环境可能有很多源库，全部串行 snapshot 太慢；
 * - 同一源库内部并发跑多个 snapshot SQL，容易把源库打爆；
 * - 老 plan 不能因为新 plan 还没 snapshot 完，就一直不能恢复增量；
 * - 最终增量仍应收敛成一个总 job，而不是几十个长期运行的小 job。
 *
 * 当前这个类先把 snapshot 调度模型改正确，
 * 同时对 incremental 做“兼容桥接”：
 * - 如果 runner 未来已经升级为支持 List<RulePlan>，这里会优先调用新接口；
 * - 如果 runner 还是旧的单 plan 接口，那么：
 *   - 只有 1 个可增量 plan 时，仍然兼容执行；
 *   - 多于 1 个时，显式报错，防止静默按旧模型误跑。
 */
public class PlanExecutionCoordinator {

    private static final Logger logger = LogManager.getLogger(PlanExecutionCoordinator.class);

    private final PipelinePlanRunStateRepository stateRepository;
    private final SnapshotScnProvider snapshotScnProvider;
    private final SnapshotPhaseRunner snapshotPhaseRunner;
    private final IncrementalPhaseRunner incrementalPhaseRunner;

    // snapshot 阶段运行时资源，用于阶段结束后关闭 MiniCluster
    private final AutoCloseable snapshotRuntime;

    public PlanExecutionCoordinator(PipelinePlanRunStateRepository stateRepository,
                                    SnapshotScnProvider snapshotScnProvider,
                                    SnapshotPhaseRunner snapshotPhaseRunner,
                                    IncrementalPhaseRunner incrementalPhaseRunner,
                                    AutoCloseable snapshotRuntime) {
        // 保存状态仓库
        this.stateRepository = Objects.requireNonNull(stateRepository, "stateRepository");

        // 保存 snapshot SCN provider
        this.snapshotScnProvider = Objects.requireNonNull(snapshotScnProvider, "snapshotScnProvider");

        // 保存 snapshot runner
        this.snapshotPhaseRunner = Objects.requireNonNull(snapshotPhaseRunner, "snapshotPhaseRunner");

        // 保存 incremental runner
        this.incrementalPhaseRunner = Objects.requireNonNull(incrementalPhaseRunner, "incrementalPhaseRunner");

        // 保存 snapshot 运行时资源
        this.snapshotRuntime = snapshotRuntime;
    }

    /**
     * 执行全部 RulePlan。
     * <p>
     * 这是整个 coordinator 的总入口。
     * <p>
     * 这版执行顺序明确分成两段：
     * 1. 先处理 snapshot：
     * - 先把所有 plan 加载/校验/建状态；
     * - 按 data_source_id 分组；
     * - 不同组并行、组内串行地推进 snapshot。
     * 2. 再处理 incremental：
     * - 收集所有已经具备增量资格的 plan；
     * - 尝试把这些 plan 一次性纳入总 incremental job。
     * <p>
     * 为什么不能再像旧版那样：
     * for (plan : plans) {
     * snapshot(plan);
     * incremental(plan);
     * }
     * <p>
     * 因为 incremental 是长跑作业，一旦第一个 plan 先进入 env.execute()，
     * 后面的 plan 基本就没有机会继续执行了。
     */
    public void executeAll(List<RulePlan> plans) throws Exception {


        // 先统一校验并加载每个 plan 对应的运行状态,后面 snapshot 分组执行时，需要 plan 和 state 成对处理；
        List<PlanExecutionUnit> units = new ArrayList<>();
        for (RulePlan plan : plans) {
            validatePlan(plan);
            // 加载 或 新建状态
            PipelinePlanRunState state = loadOrCreateState(plan);
            // 封装 plan 与 状态
            units.add(new PlanExecutionUnit(plan, state));
        }



        /*
            按 data_source_id 分组,同一个 data_source_id 的 plan 放在一个组里；
              组内串行执行 snapshot
              组间并行执行 snapshot
         */
        Map<String, List<PlanExecutionUnit>> groupedUnits = groupByDataSourceId(units);

        // 执行 snapshot 阶段
        try {
            // 执行 snapshot 分组调度
            runGroupedSnapshotPhase(groupedUnits);
        } finally {
            // snapshot 阶段结束后关闭共享 MiniCluster
            closeSnapshotRuntime();
        }

        /*
         * 第 4 步：收集所有已经具备增量资格的 plan。
         *
         * 当前先按这几个 phase 视为“可进入增量集合”：
         * - SNAPSHOT_DONE
         * - INCREMENTAL_STARTING
         * - INCREMENTAL_RUNNING
         *
         * 后面如果你引入 INCREMENTAL_READY，这里可以一起纳入。
         */
        List<PlanExecutionUnit> incrementalCandidates = units.stream()
                .filter(this::isEligibleForIncremental)
                .collect(Collectors.toList());

        if (incrementalCandidates.isEmpty()) {
            logger.info("no plans are eligible for incremental phase, skip incremental execution");
            return;
        }

        /*
         * 第 5 步：尝试进入总 incremental job。
         *
         * 这里优先尝试调用“未来升级后的多 plan 接口”；
         * 如果 runner 还是旧接口，则只允许单个 plan 兼容执行；
         * 多于一个 plan 时直接报错，避免静默退化为旧模型。
         */
        runIncrementalPhase(incrementalCandidates);
    }


    /**
     * 执行单个 RulePlan。
     *
     * 这个方法在新模型里的定位，和旧版已经不一样了。
     *
     * 旧版语义是：
     * - 对单个 plan 直接跑完整链路：
     *   capture scn -> snapshot -> incremental
     *
     * 新版语义不再这么做。
     * 因为当前系统目标已经改成：
     * 1. snapshot 阶段按 data_source_id 分组调度；
     * 2. incremental 阶段是一个总 job，而不是单个 plan 一个 job。
     *
     * 所以这里的 execute(plan) 更适合作为“单 plan 调试入口”或“兼容入口”：
     * - 它会完成该 plan 的状态加载；
     * - 按新规则执行 snapshot；
     * - 然后尝试进入 incremental；
     * - 但如果当前增量阶段需要多个 plan 一起运行，这里也会复用 runIncrementalPhase(...) 的统一逻辑。
     *
     * 为什么还保留这个方法：
     * - 便于单个 plan 的局部调试；
     * - 便于兼容少量旧调用方；
     * - 同时可以保证单 plan 和 executeAll(...) 共用同一套状态推进逻辑。
     *
     * 注意：
     * 在真正生产主链中，推荐优先使用 executeAll(...)，
     * 因为只有 executeAll(...) 才能体现“按 data_source_id 分组的 snapshot 调度”和“总 incremental job”的完整模型。
     */
    public void execute(RulePlan plan) throws Exception {
        /*
         * 第 0 步：先校验 plan。
         *
         * 这里沿用统一校验逻辑，确保：
         * - plan 不为 null
         * - planId 非空
         * - 节点列表非空
         */
        validatePlan(plan);

        /*
         * 第 1 步：加载或创建这个 plan 对应的运行状态。
         *
         * 为什么这里不能直接 new 一个临时 state：
         * - 因为这个状态是需要持久化恢复的；
         * - 重启后必须能够知道这个 plan 之前是否已经 snapshot 完成；
         * - 也必须能知道 snapshotScn 是否已经固定过。
         */
        PipelinePlanRunState state = loadOrCreateState(plan);

        /*
         * 第 2 步：执行该 plan 的 snapshot 逻辑。
         *
         * 这里调用的是“单 plan snapshot 编排”方法，
         * 它内部会按当前 phase 决定：
         * - 是否需要 capture SCN
         * - 是否需要真正跑 snapshot
         * - 如果 snapshot 已完成，则直接跳过
         *
         * 注意：
         * 这里虽然是单 plan 执行，
         * 但内部推进规则仍然和 executeAll(...) 保持一致。
         */
        executeSnapshotForUnit(new PlanExecutionUnit(plan, state));

        /*
         * 第 3 步：如果 snapshot 完成并具备增量资格，则尝试进入 incremental。
         *
         * 这里仍然复用统一的 runIncrementalPhase(...)，
         * 而不是自己直接调 incrementalPhaseRunner.run(plan, state)。
         *
         * 这样做的原因：
         * - 保证单 plan 入口和批量入口共享同一套增量准入逻辑；
         * - 如果后面 IncrementalPhaseRunner 升级成支持多 plan，
         *   这里也不需要再改主流程。
         */
        PlanExecutionUnit unit = new PlanExecutionUnit(plan, state);
        if (!isEligibleForIncremental(unit)) {
            logger.info("plan is not eligible for incremental phase after snapshot, planId={}, runId={}, phase={}",
                    state.getPlanId(),
                    state.getRunId(),
                    state.getPhase());
            return;
        }

        /*
         * 第 4 步：把当前 plan 作为一个“单元素候选集合”传给增量阶段。
         *
         * 这样即使后续 runIncrementalPhase(...) 已经升级为多 plan 入口，
         * 单 plan 的兼容调用也仍然成立。
         */
        List<PlanExecutionUnit> incrementalCandidates = new ArrayList<>();
        incrementalCandidates.add(unit);

        runIncrementalPhase(incrementalCandidates);
    }

    private PipelinePlanRunState loadOrCreateState(RulePlan plan) {
        // 已存在状态则直接复用
        Optional<PipelinePlanRunState> existing = stateRepository.findByPlanId(plan.getPlanId());
        if (existing.isPresent()) {
            return existing.get();
        }

        // 首次插入时直接标记为 SNAPSHOT_RUNNING，但暂不采集 SCN
        PipelinePlanRunState state = PipelinePlanRunState.initSnapshotRunning(
                plan.getPlanId(),
                UUID.randomUUID().toString().replace("-", "")
        );

        // 首次只 insert，不 delete
        return stateRepository.insert(state);
    }

    private void validatePlan(RulePlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan is null");
        }
        if (plan.getPlanId() == null || plan.getPlanId().trim().isEmpty()) {
            throw new IllegalArgumentException("planId is blank");
        }
        if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
            throw new IllegalArgumentException("plan nodes is empty, planId=" + plan.getPlanId());
        }
    }


    /**
     * 按 data_source_id 对执行单元分组。
     *
     * 同一源库的 plan 会被放到同一组，后面组内串行执行 snapshot。
     */
    private Map<String, List<PlanExecutionUnit>> groupByDataSourceId(List<PlanExecutionUnit> units) {
        Map<String, List<PlanExecutionUnit>> grouped = new LinkedHashMap<>();
        if (units == null || units.isEmpty()) {
            return grouped;
        }

        for (PlanExecutionUnit unit : units) {
            if (unit == null) {
                continue;
            }

            String dataSourceId = resolvePrimaryDataSourceId(unit.getPlan());
            grouped.computeIfAbsent(dataSourceId, key -> new ArrayList<>()).add(unit);
        }

        return grouped;
    }

    /**
     * 从一个 RulePlan 中解析主 data_source_id。
     *
     * 当前规则保持和 OracleSnapshotScnProvider 一致：
     * 顺序扫描节点，只要发现第一个非空的 data_source_id / dataFromSourceId 就返回。
     */
    private String resolvePrimaryDataSourceId(RulePlan plan) {
        validatePlan(plan);

        for (PlanNodeSpec node : plan.getNodes()) {
            if (node == null || node.getParams() == null || node.getParams().isEmpty()) {
                continue;
            }

            String dataSourceId = extractFirstNonBlankParam(node, "data_source_id", "dataFromSourceId");
            if (dataSourceId != null) {
                return dataSourceId;
            }
        }

        throw new IllegalArgumentException("cannot resolve data_source_id from planId=" + plan.getPlanId());
    }

    /**
     * 从节点参数中按顺序取第一个非空字符串参数。
     *
     * 这个方法和 KafkaPipeline 里的 extractRequiredStringParam 不同：
     * 这里是“可选读取”，找不到就返回 null。
     */
    private String extractFirstNonBlankParam(PlanNodeSpec node, String... keys) {
        if (node == null || keys == null || keys.length == 0) {
            return null;
        }

        Map<String, Object> params = node.getParams();
        if (params == null || params.isEmpty()) {
            return null;
        }

        for (String key : keys) {
            if (key == null || key.trim().isEmpty()) {
                continue;
            }

            Object value = params.get(key);
            if (value == null) {
                continue;
            }

            String text = String.valueOf(value).trim();
            if (!text.isEmpty()) {
                return text;
            }
        }

        return null;
    }




    /**
     * 执行分组后的 snapshot 阶段：
     * - 不同 data_source_id 分组并行
     * - 同一 data_source_id 分组内部串行
     */
    private void runGroupedSnapshotPhase(Map<String, List<PlanExecutionUnit>> groupedUnits) throws Exception {
        if (groupedUnits == null || groupedUnits.isEmpty()) {
            logger.info("no snapshot groups found, skip snapshot phase");
            return;
        }

        int poolSize = Math.max(1, groupedUnits.size());
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            // 每个entry，是相同data_source_id的plan
            for (Map.Entry<String, List<PlanExecutionUnit>> entry : groupedUnits.entrySet()) {
                String dataSourceId = entry.getKey();
                List<PlanExecutionUnit> groupUnits = entry.getValue();

                tasks.add(() -> {
                    runSnapshotGroup(dataSourceId, groupUnits);
                    return null;
                });
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * 同一个 data_source_id 分组内串行执行 snapshot。
     *
     * 串行的原因是控制同源库压力，避免多个 snapshot SQL 同时打到同一个库。
     */
    private void runSnapshotGroup(String dataSourceId, List<PlanExecutionUnit> groupUnits) throws Exception {
        // 空分组直接跳过
        if (groupUnits == null || groupUnits.isEmpty()) {
            logger.info("skip empty snapshot group, dataSourceId={}", dataSourceId);
            return;
        }

        logger.info("start snapshot group, dataSourceId={}, planCount={}", dataSourceId, groupUnits.size());

        // 同一个 data_source_id 组内串行执行
        for (PlanExecutionUnit unit : groupUnits) {
            executeSnapshotForUnit(unit);
        }

        logger.info("finish snapshot group, dataSourceId={}, planCount={}", dataSourceId, groupUnits.size());
    }

    private void executeSnapshotForUnit(PlanExecutionUnit unit) throws Exception {
        Objects.requireNonNull(unit, "unit");

        RulePlan plan = unit.getPlan();
        PipelinePlanRunState state = unit.getState();

        // 只有 SNAPSHOT_RUNNING / SNAPSHOT_FAILED 才执行快照
        if (!state.shouldRunSnapshot()) {
            logger.info("skip snapshot phase, planId={}, runId={}, phase={}",
                    state.getPlanId(),
                    state.getRunId(),
                    state.getPhase());
            return;
        }

        try {
            // 每次快照尝试都重新采集 SCN，不复用旧 snapshotScn
            long snapshotScn = snapshotScnProvider.captureSnapshotScn(plan);

            // 标记新一轮快照尝试
            state.beginSnapshotAttempt(snapshotScn);
            stateRepository.update(state);

            logger.info("start snapshot phase, planId={}, runId={}, snapshotScn={}",
                    state.getPlanId(),
                    state.getRunId(),
                    state.getSnapshotScn());

            // 执行当前 plan 的 snapshot job
            snapshotPhaseRunner.run(plan, state);

            // 快照完成
            state.markSnapshotDone();
            stateRepository.update(state);

            logger.info("snapshot phase finished, planId={}, runId={}, snapshotScn={}",
                    state.getPlanId(),
                    state.getRunId(),
                    state.getSnapshotScn());
        } catch (Exception e) {
            // 快照失败，下次启动会重新采集 SCN 并重跑
            state.markSnapshotFailed(safeTrim(e.getMessage(), 2000));
            stateRepository.update(state);

            logger.error("snapshot phase failed, planId={}, runId={}, dataSourceId={}, error={}",
                    state.getPlanId(),
                    state.getRunId(),
                    resolvePrimaryDataSourceId(plan),
                    state.getLastError(),
                    e);

            throw e;
        }
    }



//    /**
//     * 如果当前 plan 还没有固定 snapshotScn，就先采集一次。
//     *
//     * 同一个 plan 后续重启时应复用已有 snapshotScn，而不是重复采集。
//     */
//    private void captureSnapshotScnIfNeeded(RulePlan plan, PipelinePlanRunState state) throws Exception {
//        Objects.requireNonNull(plan, "plan");
//        Objects.requireNonNull(state, "state");
//
//        if (state.getSnapshotScn() != null) {
//            return;
//        }
//
//        long snapshotScn = snapshotScnProvider.captureSnapshotScn(plan);
//        state.setSnapshotScn(snapshotScn);
//        state.setPhase(PipelinePlanPhase.CAPTURE_SCN);
//        state.setLastError(null);
//        stateRepository.save(state);
//
//        logger.info("captured snapshot scn, planId={}, runId={}, scn={}",
//                state.getPlanId(),
//                state.getRunId(),
//                state.getSnapshotScn());
//    }

    /**
     * 当前 plan 是否还需要执行 snapshot。
     */
    private boolean shouldRunSnapshot(PipelinePlanRunState state) {
        if (state == null || state.getPhase() == null) {
            return false;
        }

        PipelinePlanPhase phase = state.getPhase();
        return phase == PipelinePlanPhase.SNAPSHOT_RUNNING
                || phase == PipelinePlanPhase.SNAPSHOT_FAILED;
    }

    private boolean isEligibleForIncremental(PlanExecutionUnit unit) {
        // 空对象不能进入增量
        if (unit == null || unit.getState() == null) {
            return false;
        }

        PipelinePlanRunState state = unit.getState();

        // 没有 snapshotScn 不能进入增量
        if (state.getSnapshotScn() == null) {
            return false;
        }

        // SNAPSHOT_DONE / INCREMENTAL_RUNNING / INCREMENTAL_FAILED 都进入增量恢复逻辑
        return state.isEligibleForIncremental();
    }


    private void runIncrementalPhase(List<PlanExecutionUnit> incrementalCandidates) throws Exception {
        if (incrementalCandidates == null || incrementalCandidates.isEmpty()) {
            logger.info("no incremental candidates found, skip incremental phase");
            return;
        }

        List<RulePlan> plans = new ArrayList<>();
        List<PipelinePlanRunState> states = new ArrayList<>();

        for (PlanExecutionUnit unit : incrementalCandidates) {
            if (unit == null || unit.getPlan() == null || unit.getState() == null) {
                continue;
            }

            PipelinePlanRunState state = unit.getState();

            // 增量必须依赖已完成快照的 snapshotScn
            if (state.getSnapshotScn() == null) {
                throw new IllegalStateException(
                        "snapshotScn is null, cannot start incremental, planId=" + state.getPlanId()
                                + ", runId=" + state.getRunId()
                );
            }

            plans.add(unit.getPlan());
            states.add(state);
        }

        if (plans.isEmpty()) {
            logger.info("no valid incremental plans found after filtering, skip incremental phase");
            return;
        }

        try {
            // 增量 runner 应提交长跑 job，不应阻塞等待结束
            incrementalPhaseRunner.run(plans, states);

            // 提交成功后标记增量运行中
            for (PipelinePlanRunState state : states) {
                state.markIncrementalRunning();
                stateRepository.update(state);
            }

            logger.info("incremental phase started, planCount={}", plans.size());
        } catch (Exception e) {
            String error = safeTrim(e.getMessage(), 2000);

            // 增量失败不回退快照，后续按增量消费 SCN 恢复
            for (PipelinePlanRunState state : states) {
                state.markIncrementalFailed(error);
                stateRepository.update(state);
            }

            logger.error("incremental phase failed, planCount={}, error={}",
                    plans.size(),
                    error,
                    e);

            throw e;
        }
    }





    private String safeTrim(String value, int maxLength) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (maxLength <= 0) {
            return trimmed;
        }

        if (trimmed.length() <= maxLength) {
            return trimmed;
        }

        return trimmed.substring(0, maxLength);
    }


    private void closeSnapshotRuntime() {
        // 没有运行时资源则直接返回
        if (snapshotRuntime == null) {
            return;
        }

        try {
            // 关闭 snapshot 阶段共享资源
            snapshotRuntime.close();
        } catch (Exception e) {
            // 关闭失败只记录日志，避免覆盖主流程异常
            logger.warn("failed to close snapshot runtime", e);
        }
    }

}