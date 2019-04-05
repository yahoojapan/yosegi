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

package jp.co.yahoo.yosegi.message.design;

import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SimpleField implements IField {
  private final String name;
  private final Properties properties;
  private final FieldType type;

  protected SimpleField(final String name, final Properties properties, FieldType type) {
    this.name = name;
    this.properties = properties;
    this.type = type;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public FieldType getFieldType() {
    return type;
  }

  protected LinkedHashMap<Object,Object> toJavaObjectBase() throws IOException {
    LinkedHashMap<Object,Object> schemaJavaObject = new LinkedHashMap<Object,Object>();
    schemaJavaObject.put("name" , getName());
    schemaJavaObject.put("type" , getFieldType().toString());
    schemaJavaObject.put("properties" , getProperties().toMap());
    return schemaJavaObject;
  }

  @Override
  public Map<Object,Object> toJavaObject() throws IOException {
    return toJavaObjectBase();
  }
}

