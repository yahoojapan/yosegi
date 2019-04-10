/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.design.java;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.ByteField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.UnionField;
import jp.co.yahoo.yosegi.message.design.UnionField;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class JavaObjectToField {
  @FunctionalInterface
  private interface DispatchedFunc {
    public IField apply(
        String name,
        Properties properties,
        Map<Object,Object> javaObject) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<String, DispatchedFunc> dispatcher;

  static {
    SwitchDispatcherFactory<String, DispatchedFunc> sw = new SwitchDispatcherFactory<>();
    sw.set("ARRAY", (name, properties, javaObject) -> {
      IField arrayChild = get(getChild(javaObject).get(0));
      return new ArrayContainerField(name, arrayChild, properties);
    });
    sw.set("UNION", (name, properties, javaObject) -> {
      UnionField union = new UnionField(name, properties);
      for (Map<Object,Object> childObj : getChild(javaObject)) {
        union.set(get(childObj));
      }
      return union;
    });
    sw.set("MAP", (name, properties, javaObject) -> {
      IField defaultField = get((Map<Object,Object>)javaObject.get("default"));
      MapContainerField map = new MapContainerField(name, defaultField, properties);
      for (Map<Object,Object> childObj : getChild(javaObject)) {
        map.set(get(childObj));
      }
      return map;
    });
    sw.set("STRUCT", (name, properties, javaObject) -> {
      StructContainerField struct = new StructContainerField(name, properties);
      for (Map<Object,Object> childObj : getChild(javaObject)) {
        struct.set(get(childObj));
      }
      return struct;
    });

    sw.set("BOOLEAN", (name, properties, javaObject) -> new BooleanField(name, properties));
    sw.set("BYTE",    (name, properties, javaObject) -> new ByteField(name, properties));
    sw.set("BYTES",   (name, properties, javaObject) -> new BytesField(name, properties));
    sw.set("DOUBLE",  (name, properties, javaObject) -> new DoubleField(name, properties));
    sw.set("FLOAT",   (name, properties, javaObject) -> new FloatField(name, properties));
    sw.set("INTEGER", (name, properties, javaObject) -> new IntegerField(name, properties));
    sw.set("LONG",    (name, properties, javaObject) -> new LongField(name, properties));
    sw.set("SHORT",   (name, properties, javaObject) -> new ShortField(name, properties));
    sw.set("STRING",  (name, properties, javaObject) -> new StringField(name, properties));
    sw.set("NULL",    (name, properties, javaObject) -> new NullField(name, properties));
    dispatcher = sw.create();
  }

  private JavaObjectToField() {}

  private static List<Map<Object,Object>> getChild(final Map<Object,Object> javaObject) {
    return (List<Map<Object,Object>>)javaObject.get("child");
  }


  /**
   * Create an IField from a Java object representing the schema structure.
   */
  public static IField get(final Map<Object,Object> javaObject) throws IOException {
    DispatchedFunc dispatchedFunc = dispatcher.get(javaObject.get("type").toString());
    if (Objects.isNull(dispatchedFunc)) {
      throw new IOException("Invalid schema type.");
    }

    Properties properties = new Properties();
    Map<Object,Object> propertiesObj = (Map<Object,Object>)javaObject.get("properties");
    for ( Map.Entry<Object,Object> entry : propertiesObj.entrySet()) {
      properties.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return dispatchedFunc.apply(javaObject.get("name").toString(), properties, javaObject);
  }
}

