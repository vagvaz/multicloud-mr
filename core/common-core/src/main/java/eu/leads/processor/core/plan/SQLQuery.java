package eu.leads.processor.core.plan;

import eu.leads.processor.core.DataType;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by vagvaz on 8/4/14.
 */
public class SQLQuery extends DataType implements Query {


    public SQLQuery(String user, String sql) {
        setUser(user);
        setSQL(sql);
        setPlan(new SQLPlan());
        setQueryStatus(new QueryStatus());
        setReadStatus(new ReadStatus());
        setLocation("localation");
        setMessage("unsued");
        setQueryType(QueryType.SQL.toString());

    }

    public SQLQuery(JsonObject object) {
        super(object);
    }

    @Override
    public String getQueryType() {
        return data.getString("queryType");
    }

    @Override
    public void setQueryType(String queryType) {
        data.putString("queryType", queryType);
    }

    @Override
    public String getUser() {
        return data.getString("user");
    }

    @Override
    public void setUser(String user) {
        data.putString("user", user);
    }

    @Override
    public String getId() {
        return data.getString("id");
    }

    @Override
    public void setId(String id) {
        data.putString("id", id);
    }

    @Override
    public String getMessage() {
        return data.getString("message");
    }

    @Override
    public void setMessage(String msg) {
        data.putString("message", msg);
    }

    @Override
    public boolean isCompleted() {
        QueryStatus status = new QueryStatus(data.getObject("status"));
        return status.getStatus() == QueryState.COMPLETED;
    }

    @Override
    public void setCompleted(boolean complete) {
        QueryStatus status = new QueryStatus(data.getObject("status"));
        status.setStatus(QueryState.COMPLETED);
    }

    @Override
    public QueryStatus getQueryStatus() {
        QueryStatus status = new QueryStatus(data.getObject("status"));
        return status;
    }

    @Override
    public void setQueryStatus(QueryStatus status) {
        data.putObject("status", status.asJsonObject());
    }



    @Override
    public String getLocation() {
        return data.getString("location");
    }

    @Override
    public void setLocation(String location) {
        data.putString("location", location);
    }

    @Override
    public Plan getPlan() {
        Plan result = new SQLPlan(data.getObject("plan"));
        return result;
    }

    @Override
    public void setPlan(Plan plan) {
        JsonObject jsonPlan = ((SQLPlan) plan).asJsonObject();
        data.putObject("plan", jsonPlan);
    }

    @Override
    public QueryContext getContext() {
        QueryContext context = new QueryContext(data.getObject("context"));
        return context;
    }

    @Override
    public void setContext(QueryContext context) {
        data.putObject("context", context.asJsonObject());
    }

    @Override
    public ReadStatus getReadStatus() {
        ReadStatus read = new ReadStatus(data.getObject("read"));
        return read;
    }

    @Override
    public void setReadStatus(ReadStatus readStatus) {
        data.putObject("read", readStatus.asJsonObject());
    }

    public String getSQL() {
        return data.getString("sql");
    }

    public void setSQL(String SQL) {
        data.putString("sql", SQL);
    }



}
