package eu.leads.processor.core;

import com.mongodb.util.JSON;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//@SerializeWith(Tuple.TupleExternalizer.class)
public class Tuple extends DataType_bson implements Serializable, Externalizable {


  public Tuple() {
    super();
  }

  public Tuple(String value) {
    this.data = new BasicBSONObject();
    this.data = (BSONObject) JSON.parse(value);

  }

  public Tuple(Tuple tl, Tuple tr, ArrayList<String> ignoreColumns) {
//        super(tl.toString());
    super();
    super.copy(tl.asBsonObject());
    if (ignoreColumns != null) {
      for (String field : ignoreColumns) {
        if (data.containsField(field)) {
          data.removeField(field);
        }
      }
      tr.removeAtrributes(ignoreColumns);
    }
    data.putAll(tr.asBsonObject());
  }

  public Tuple(Tuple tuple) {
    data = new BasicBSONObject(tuple.asBsonObject().toMap());
  }

  public Tuple(BSONObject object) {
    data = object;
  }
//
//  public Tuple(Tuple tmp) {
//
//  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    // Serialize it
    BasicBSONEncoder encoder = new BasicBSONEncoder();
    byte[] array = encoder.encode(data);
    out.writeObject(array);
//      out.writeInt(data.toString().length());
//      out.writeBytes(data.toString());
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    // Deserialize it
    byte[] array = (byte[]) in.readObject();
    BasicBSONDecoder decoder = new BasicBSONDecoder();
    data = decoder.readObject(array);
//         int size = in.readInt();
//         byte[] bb =  new byte[size];
//         in.readFully(bb);
//         String fromString = new String(bb);
//       data =
  }

  private void readObjectNoData() throws ObjectStreamException {
    data = new BasicBSONObject();
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    BasicBSONEncoder encoder = new BasicBSONEncoder();
    byte[] array = encoder.encode(data);
    out.writeObject(array);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    BasicBSONDecoder decoder = new BasicBSONDecoder();
    byte[] array = (byte[]) in.readObject();
    data = decoder.readObject(array);
  }

  public String asString() {
    return data.toString();
  }

  public String toString() {
    return data.toString();
  }

  public Set<String> getFieldSet() {
    return data.keySet();
  }

  public void setAttribute(String attributeName, String value) {
    data.put(attributeName, value);
  }

  public void setNumberAttribute(String attributeName, Number value) {
    data.put(attributeName, value);
  }

  public String getAttribute(String column) {
    Object result = null;
    result = data.get(column);
    try {
      if (result == null) {
        if (!data.containsField(column)) {
          System.err.println("Could not find attribute " + column + " " + data.keySet().toString());
        } else {
          System.err.println("Attribute " + column + " is null ");
        }
        return null;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result.toString();
  }

  public Number getNumberAttribute(String column) {
    return (Number) data.get(column);
  }

  public void keepOnly(List<String> columns) {
    Set<String> fields = new HashSet<>();
    fields.addAll(data.keySet());
    Set<String> keep = new HashSet<>();
    keep.addAll(columns);
    for (String field : fields) {
      if (!keep.contains(field)) {
        data.removeField(field);
      }
    }
  }

  public void removeAtrribute(String name) {
    data.removeField(name);
  }

  public void removeAtrributes(List<String> columns) {
    for (String column : columns) {
      data.removeField(column);
    }
  }

  public Set<String> getFieldNames() {
    return data.keySet();
  }

  public boolean hasField(String attribute) {
    return data.containsField(attribute);
  }

  public void removeAttribute(String field) {
    data.removeField(field);
  }

  public void renameAttribute(String oldName, String newName) {
    if (oldName.equals(newName)) {
      return;
    }
    Object value = data.get(oldName);
    data.removeField(oldName);
    data.put(newName, value);
  }

  public Object getGenericAttribute(String attribute) {
    return data.get(attribute);
  }

  public void setAttribute(String name, Object tupleValue) {
    if (tupleValue != null) {
      data.put(name, tupleValue);
    } else {
      System.err.println("set " + name + " has tupleValue null");
      data.put(name, tupleValue);
    }
  }

  public void renameAttributes(Map<String, List<String>> toRename) {
    for (Map.Entry<String, List<String>> entry : toRename.entrySet()) {
      {
        Object value = getGenericAttribute(entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            renameAttribute(entry.getKey(), entry.getValue().get(i));
          } else {
            setAttribute(entry.getValue().get(i), value);
          }
        }
      }
    }
  }

  public static class TupleExternalizer implements AdvancedExternalizer<Tuple> {

    @Override
    public void writeObject(ObjectOutput output, Tuple object) throws IOException {
      BasicBSONEncoder encoder = new BasicBSONEncoder();
      byte[] array = encoder.encode(object.asBsonObject());
//            byte[] array1 = Base64.encode(array);
      output.writeObject(array);
    }

    @Override
    public Tuple readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      BasicBSONDecoder decoder = new BasicBSONDecoder();
      byte[] array = (byte[]) input.readObject();
//            String tmp = (String) input.readObject();
//            byte[] array = Base64.decode(tmp.getBytes());

      BSONObject object = decoder.readObject(array);
      Tuple tuple = new Tuple(object);
      return tuple;
    }

    @Override
    public Set<Class<? extends Tuple>> getTypeClasses() {
      Set<Class<? extends Tuple>> result = new HashSet<>();
      result.add(Tuple.class);
      return result;
    }

    @Override
    public Integer getId() {
      return 27011988;
    }
  }
}
