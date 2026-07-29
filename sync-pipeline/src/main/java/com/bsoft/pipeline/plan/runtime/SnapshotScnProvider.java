package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.RulePlan;

/**
 * ClassName: SnapshotScnProvider
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:19
 */

public interface SnapshotScnProvider {
    Long capture(RulePlan plan) throws Exception;
}