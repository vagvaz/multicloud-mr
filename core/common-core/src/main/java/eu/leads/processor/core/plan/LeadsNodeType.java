package eu.leads.processor.core.plan;

import org.apache.tajo.plan.logical.*;

/**
 * Created by vagvaz on 8/29/14.
 */
public enum LeadsNodeType {
    ROOT(LogicalRootNode.class),
    EXPRS(EvalExprNode.class),
    PROJECTION(ProjectionNode.class),
    LIMIT(LimitNode.class),
    SORT(SortNode.class),
    HAVING(HavingNode.class),
    GROUP_BY(GroupbyNode.class),
    WINDOW_AGG(WindowAggNode.class),
    SELECTION(SelectionNode.class),
    JOIN(JoinNode.class),
    UNION(UnionNode.class),
    EXCEPT(ExceptNode.class),
    INTERSECT(IntersectNode.class),
    TABLE_SUBQUERY(TableSubQueryNode.class),
    SCAN(ScanNode.class),
    PARTITIONS_SCAN(PartitionedTableScanNode.class),
    BST_INDEX_SCAN(IndexScanNode.class),
    STORE(StoreTableNode.class),
    INSERT(InsertNode.class),
    DISTINCT_GROUP_BY(DistinctGroupbyNode.class),

    CREATE_DATABASE(CreateDatabaseNode.class),
    DROP_DATABASE(DropDatabaseNode.class),
    CREATE_TABLE(CreateTableNode.class),
    DROP_TABLE(DropTableNode.class),
    ALTER_TABLESPACE(AlterTablespaceNode.class),
    ALTER_TABLE(AlterTableNode.class),
    TRUNCATE_TABLE(TruncateTableNode.class),
    WGS_URL(WGSUrlDepthNode.class),
    OUTPUT_NODE(OutputNode.class),
    EPQ(EncryptedPointQueryNode.class);


    private final Class<? extends LogicalNode> baseClass;

    LeadsNodeType(Class<? extends LogicalNode> baseClass) {
        this.baseClass = baseClass;
    }

    public Class<? extends LogicalNode> getBaseClass() {
        return this.baseClass;
    }

    public LeadsNodeType resolve(NodeType type) {
        return LeadsNodeType.valueOf(type.toString());
    }
}
