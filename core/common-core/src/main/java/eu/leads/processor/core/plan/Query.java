package eu.leads.processor.core.plan;

/**
 * Created by vagvaz on 8/4/14.
 */
public interface Query {

  String getQueryType();

  void setQueryType(String queryType);

  String getUser();

  void setUser(String user);

  String getId();

  void setId(String id);

  String getMessage();

  void setMessage(String msg);

  boolean isCompleted();

  void setCompleted(boolean complete);

  QueryStatus getQueryStatus();

  void setQueryStatus(QueryStatus status);

  String getLocation();

  void setLocation(String location);

  Plan getPlan();

  void setPlan(Plan plan);

  QueryContext getContext();

  void setContext(QueryContext context);

  ReadStatus getReadStatus();

  void setReadStatus(ReadStatus readStatus);

}
