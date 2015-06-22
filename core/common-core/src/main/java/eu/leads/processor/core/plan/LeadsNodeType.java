package eu.leads.processor.core.plan;

import org.apache.tajo.plan.logical.AlterTableNode;
import org.apache.tajo.plan.logical.AlterTablespaceNode;
import org.apache.tajo.plan.logical.CreateDatabaseNode;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.DropDatabaseNode;
import org.apache.tajo.plan.logical.DropTableNode;
import org.apache.tajo.plan.logical.EvalExprNode;
import org.apache.tajo.plan.logical.ExceptNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.plan.logical.HavingNode;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.IntersectNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LimitNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.PartitionedTableScanNode;
import org.apache.tajo.plan.logical.ProjectionNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.logical.SelectionNode;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.plan.logical.StoreTableNode;
import org.apache.tajo.plan.logical.TableSubQueryNode;
import org.apache.tajo.plan.logical.TruncateTableNode;
import org.apache.tajo.plan.logical.UnionNode;
import org.apache.tajo.plan.logical.WindowAggNode;

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
