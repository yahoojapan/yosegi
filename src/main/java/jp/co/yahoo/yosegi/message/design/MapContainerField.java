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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapContainerField extends SimpleField implements INamedContainerField {
  private final IField defaultField;
  private final Map<String,IField> fieldContainer = new HashMap<String,IField>();

  /**
   * Creates an object representing Map with the specified parameters.
   */
  public MapContainerField(final String name, final IField defaultField) {
    this(name, defaultField, new Properties());
  }

  /**
   * Creates an object representing Map with the specified parameters.
   */
  public MapContainerField(
      final String name,
      final IField defaultField,
      final Properties properties) {
    super(name, properties, FieldType.MAP);
    this.defaultField = defaultField;
  }

  @Override
  public IField getField() {
    return defaultField;
  }

  @Override
  public void set(final IField field) throws IOException {
    fieldContainer.put(field.getName(), field);
  }

  @Override
  public IField get(final String key) throws IOException {
    return fieldContainer.getOrDefault(key, getField());
  }

  @Override
  public boolean containsKey(final String key) throws IOException {
    return fieldContainer.containsKey(key);
  }

  @Override
  public String[] getKeys() throws IOException {
    Set<String> keys = fieldContainer.keySet();
    return keys.toArray(new String[keys.size()]);
  }

  @Override
  public void merge( final IField target ) throws IOException {
    if ( ! ( target instanceof MapContainerField ) ) {
      throw new UnsupportedOperationException( "target is not MapContainerField." );
    }
    MapContainerField targetField = (MapContainerField)target;
    for ( String targetKey : targetField.getKeys() ) {
      IField targetChildField = targetField.get( targetKey );
      if ( containsKey( targetKey ) ) {
        IField childField = get( targetKey );
        if ( targetChildField.getFieldType() != childField.getFieldType()
            && childField.getFieldType() != FieldType.UNION ) {
          UnionField newField = new UnionField(
              childField.getName() , childField.getProperties() 
          );
          newField.set( childField );
          childField = newField;
          set( childField );
        }
        childField.merge( targetChildField );
      } else {
        set( targetChildField );
      }
    }
  }

  @Override
  public Map<Object,Object> toJavaObject() throws IOException {
    LinkedHashMap<Object,Object> schemaJavaObject = toJavaObjectBase();
    schemaJavaObject.put("default", getField().toJavaObject());
    List<Object> childList = new ArrayList<Object>();
    for (String key : getKeys()) {
      childList.add(get(key).toJavaObject());
    }
    schemaJavaObject.put("child", childList);
    return schemaJavaObject;
  }
}

