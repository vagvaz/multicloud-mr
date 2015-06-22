package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.core.plan.LeadsNodeType;
import eu.leads.processor.infinispan.operators.DemoMapReduceOperator;
import eu.leads.processor.infinispan.operators.FilterOperator;
import eu.leads.processor.infinispan.operators.GroupByOperator;
import eu.leads.processor.infinispan.operators.JoinOperator;
import eu.leads.processor.infinispan.operators.LimitOperator;
import eu.leads.processor.infinispan.operators.Operator;
import eu.leads.processor.infinispan.operators.ProjectOperator;
import eu.leads.processor.infinispan.operators.ScanOperator;
import eu.leads.processor.infinispan.operators.SortOperator;
import eu.leads.processor.infinispan.operators.mapreduce.IntersectOperator;
import eu.leads.processor.infinispan.operators.mapreduce.UnionOperator;

import org.apache.tajo.algebra.OpType;

/**
 * Created by vagvaz on 9/23/14.
 */
public class SQLOperatorFactory {

  public static Operator getOperator(Node com, InfinispanManager persistence, LogProxy log,
                                     Action action) {
    Operator result = null;
    String implemenationType = action.getData().getString("implementation");
    LeadsNodeType operatorType = LeadsNodeType.valueOf(action.getData().getString("operatorType"));
    switch (operatorType) {
      case ROOT:
        result = getCustomMapReduceOperator(com, persistence, log, action);
        break;
      case EXPRS:
        result = getSingleOperator(com, persistence, log, action);
        break;
      case PROJECTION:
        result = new ProjectOperator(com, persistence, log, action);
        break;
      case LIMIT:
        result = new LimitOperator(com, persistence, log, action);
        break;
      case SORT:
        result = new SortOperator(com, persistence, log, action);
        break;
      case HAVING:
        result = new FilterOperator(com, persistence, log, action);
        break;
      case GROUP_BY:
        result = new GroupByOperator(com, persistence, log, action);
        break;
      case WINDOW_AGG:
        break;
      case SELECTION:
        result = new FilterOperator(com, persistence, log, action);
        break;
      case JOIN:
        result = new JoinOperator(com, persistence, log, action);
        break;
      case UNION:
        result = new UnionOperator(com, persistence, log, action);
        break;
      case EXCEPT:
        break;
      case INTERSECT:
        result = new IntersectOperator(com, persistence, log, action);
        break;
      case TABLE_SUBQUERY:
        break;
      case SCAN:
        result = new ScanOperator(com, persistence, log, action);
        break;
      case PARTITIONS_SCAN:
        break;
      case BST_INDEX_SCAN:
        break;
      case STORE:
        break;
      case INSERT:
        //Is Covered with EXPRS
        break;
      case DISTINCT_GROUP_BY:
        break;
      case CREATE_DATABASE:
        break;
      case DROP_DATABASE:
        break;
      case CREATE_TABLE:
        break;
      case DROP_TABLE:
        break;
      case ALTER_TABLESPACE:
        break;
      case ALTER_TABLE:
        break;
      case TRUNCATE_TABLE:
        break;
      case WGS_URL:
        break;
      case OUTPUT_NODE:
        break;
    }
    return result;
  }

  private static Operator getCustomMapReduceOperator(Node com, InfinispanManager persistence,
                                                     LogProxy log, Action action) {
    Operator result = null;
    result = new DemoMapReduceOperator(com, persistence, log, action);
    return result;
  }

  private static Operator getSingleOperator(Node com, InfinispanManager persistence, LogProxy log,
                                            Action action) {
    Operator result = null;
    String opType = action.getData().getObject("operator").getObject("configuration")
        .getObject("body").getString("operationType");
    if (opType.equals(OpType.Insert.toString())) {
//                   result = new InsertOperator(com,persistence,log,action);
    } else {
      log.error("Trying to create Unimplemented operator " + opType);

    }
    return result;

  }
}
