package eu.leads.processor.math;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.StdConverter;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 9:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class MathTreeOperatorConverter extends StdConverter<JsonNode, FilterOperatorTree> {
    @Override
    public FilterOperatorTree convert(JsonNode value) {
        return null;
    }

//    @Override
//    public MathOperatorTree convert(JsonNode jsonNode) {
//        MathOperatorTreeNode root;
//        MathTreeOperatorNodeConverter converter = new MathTreeOperatorNodeConverter();
//        root = converter.convert(jsonNode.path("root"));
//        return new MathOperatorTree(root);
//    }
}
