package com.bsoft.pipeline.plan.scn;

/**
 * ClassName: OracleSnapshotScnProvider
 * Package:   com.bsoft.pipeline.plan.scn
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:16
 */
import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.BusinessDataSourceConfigRegistry;
import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

public class OracleSnapshotScnProvider implements SnapshotScnProvider {

    private final BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;

    public OracleSnapshotScnProvider(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry) {
        this.businessDataSourceConfigRegistry = businessDataSourceConfigRegistry;
    }

    @Override
    public long captureSnapshotScn(RulePlan plan) throws Exception {
        if (plan == null || plan.getNodes() == null || plan.getNodes().isEmpty()) {
            throw new IllegalArgumentException("plan is empty");
        }

        String dataSourceId = resolvePrimaryDataSourceId(plan);
        BusinessDataSourceConfig sourceConfig =
                businessDataSourceConfigRegistry.findBusinessDataSourceConfigByDataSourceId(dataSourceId);

        if (sourceConfig == null) {
            throw new IllegalStateException("sourceConfig not found, dataSourceId=" + dataSourceId);
        }

        DataSource dataSource = sourceConfig.getDataSource();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("select current_scn from v$database");
             ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new IllegalStateException("query current_scn returned no row");
            }
            return rs.getLong(1);
        }
    }

    private String resolvePrimaryDataSourceId(RulePlan plan) {
        for (PlanNodeSpec node : plan.getNodes()) {
            if (node == null || node.getParams() == null) {
                continue;
            }
            Map<String, Object> params = node.getParams();
            Object value = params.get("data_source_id");
            if (value == null) {
                value = params.get("dataFromSourceId");
            }
            if (value != null && !String.valueOf(value).trim().isEmpty()) {
                return String.valueOf(value).trim();
            }
        }
        throw new IllegalArgumentException("cannot resolve data_source_id from planId=" + plan.getPlanId());
    }
}
