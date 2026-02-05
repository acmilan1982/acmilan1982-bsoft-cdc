package com.bsoft.server.sql.delete;

/**
 * ClassName: DorisDeleteStrategy
 * Package:   com.bsoft.server.sql.delete
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/25 - 10:54
 */

/**
 * Doris 数据库删除策略实现（单条删除）
 * - 支持两种策略：
 *   1) DELETE：物理删除（DELETE FROM ... WHERE pk=? AND ...）
 *   2) DELETE_FLAG：逻辑删除（UPDATE ... SET delete_flag=1, delete_time=NOW() WHERE pk=? AND ...）
 *
 * 注意：
 * - Doris 3.x 下，DELETE/UPDATE 语句可用；
 * - 若你的逻辑删除字段名不是 delete_flag / delete_time，请在这里按需调整。
 */

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.TableRowDeleteStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DorisDeleteStrategy extends AbstractDeleteStrategy {
    public DorisDeleteStrategy() {
    }

    public DorisDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig,TableRowDeleteStrategy tableRowDeleteStrategy) {
        super(targetDataSourceConfig,tableRowDeleteStrategy);
    }

    @Override
    public String buildDeleteSql(String tableName, Set<String> primaryKeySet) {
        // 没有主键，无法安全删除
        validatePrimaryKeySet(tableName,primaryKeySet);
        // WHERE：固定顺序（字典序），避免 Set 迭代顺序漂移
        String whereClause = buildWhereClause(primaryKeySet);


        switch (tableRowDeleteStrategy) {
            case DELETE:
                // 物理删除
                return String.format("delete from %s where %s", quote(tableName), whereClause);

            case DELETE_FLAG:
                // 逻辑删除（字段名可按你的表结构调整为 cdc_delete_flag / cdc_delete_time 等）
                return String.format(
                        "update %s set cdc_op = 'd' ,%s = 1, %s = NOW() where %s",
                        quote(tableName),
                        quote("cdc_delete_flag"),
                        quote("cdc_delete_time"),
                        whereClause
                );

            default:
                throw new IllegalArgumentException("不支持的删除策略: " + tableRowDeleteStrategy);
        }
    }
//
//    @Override
//    public void setParameters(PreparedStatement ps,
//                              Map<String, Object> beforeData,
//                              Set<String> primaryKeys,
//                              TableRowDeleteStrategy deleteStrategy) throws SQLException {
//        // 仅绑定 WHERE 子句的主键参数（单条删除）
//        int index = 1;
//        for (String pk : primaryKeys) {
//            Object value = beforeData.get(pk);
//            ps.setObject(index++, value);
//        }
//    }

    /** 反引号转义，兼容 Doris(MySQL 协议) */
    private String quote(String ident) {
        if (ident == null) return "``";
        return "`" + ident.replace("`", "") + "`";
    }
}
