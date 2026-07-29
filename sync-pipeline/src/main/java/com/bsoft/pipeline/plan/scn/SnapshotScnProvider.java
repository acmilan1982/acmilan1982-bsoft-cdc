package com.bsoft.pipeline.plan.scn;

import com.bsoft.common.pipeline.model.RulePlan;

/**
 * ClassName: SnapshotScnProvider
 * Package:   com.bsoft.pipeline.plan.scn
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:15
 */
public interface SnapshotScnProvider {
    long captureSnapshotScn(RulePlan plan) throws Exception;
}
