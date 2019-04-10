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

public class UnionField extends SimpleField implements INamedContainerField {
  private final List<String> keyList = new ArrayList<String>();
  private final Map<String,IField> fieldContainer = new HashMap<String,IField>();

  public UnionField(final String name) {
    this(name, new Properties());
  }

  public UnionField(final String name, final Properties properties) {
    super(name, properties, FieldType.UNION);
  }

  @Override
  public IField getField() {
    throw new UnsupportedOperationException("UnionField does not have a default field.");
  }

  @Override
  public void set( final IField field ) throws IOException {
    String fieldName = field.getClass().getName();
    if ( fieldContainer.containsKey( fieldName ) ) {
      throw new IOException( fieldName + " is already set." );
    }
    keyList.add( fieldName );
    fieldContainer.put( fieldName , field );
  }

  @Override
  public IField get(final String key) throws IOException {
    return fieldContainer.get(key);
  }

  @Override
  public boolean containsKey(final String key) throws IOException {
    return fieldContainer.containsKey(key);
  }

  @Override
  public String[] getKeys() throws IOException {
    return keyList.toArray(new String[keyList.size()]);

  }

  @Override
  public void merge( final IField target ) throws IOException {
    if ( ! ( target instanceof UnionField ) ) {
      String fieldName = target.getClass().getName();
      if ( fieldContainer.containsKey( fieldName ) ) {
        IField childField = get( fieldName );
        childField.merge( target );
      } else {
        set( target );
      }
    } else {
      UnionField targetField = (UnionField)target;
      for ( String targetKey : targetField.getKeys() ) {
        IField targetChildField = targetField.get( targetKey );
        if ( containsKey( targetKey ) ) {
          IField childField = get( targetKey );
          childField.merge( targetChildField );
        } else {
          set( targetChildField );
        }
      }
    }
  }

  @Override
  public Map<Object,Object> toJavaObject() throws IOException {
    LinkedHashMap<Object,Object> schemaJavaObject = toJavaObjectBase();
    List<Object> childList = new ArrayList<Object>();
    for (String key : getKeys()) {
      childList.add(get(key).toJavaObject());
    }
    schemaJavaObject.put("child", childList);
    return schemaJavaObject;
  }
}

