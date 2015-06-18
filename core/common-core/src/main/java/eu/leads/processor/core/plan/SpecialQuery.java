package eu.leads.processor.core.plan;

import eu.leads.processor.core.DataType;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by vagvaz on 8/4/14.
 */
public class SpecialQuery extends DataType implements Query {


    public SpecialQuery() {

        setSpecialQueryType("default");
        setPlan(new SQLPlan());
        setQueryStatus(new QueryStatus());
        setReadStatus(new ReadStatus());
        setLocation("location");
        setMessage("unused");
        data.putObject("query", new JsonObject());
        setQueryType(QueryType.SPECIAL.toString());
    }

    public SpecialQuery(JsonObject object) {
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
        return null;
    }

    @Override
    public void setPlan(Plan plan) {

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

    public String getQueryAttribute(String attribute) {
        return data.getObject("query").getString(attribute);
    }

    public void putQueryAttribute(String attribute, String value) {
        data.getObject("query").putString(attribute, value);
    }

    public String getSpecialQueryType() {
        return data.getString("specialQueryType");
    }

    public void setSpecialQueryType(String queryType) {
        data.putString("specialQueryType", queryType);
    }
}
