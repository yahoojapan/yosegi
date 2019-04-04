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

public class StructContainerField extends SimpleField implements INamedContainerField {
  private final List<String> keyList = new ArrayList<String>();
  private final Map<String,IField> fieldContainer = new HashMap<String,IField>();

  public StructContainerField(final String name) {
    super(name);
  }

  public StructContainerField( final String name , final Properties properties ) {
    super(name, properties);
  }

  @Override
  public IField getField() {
    throw new UnsupportedOperationException(
      "StructContainerField does not have a default field." );
  }

  @Override
  public void set( final IField field ) throws IOException {
    String fieldName = field.getName();
    if ( fieldContainer.containsKey( fieldName ) ) {
      throw new IOException(
        fieldName + " is already set. keys::" + fieldContainer.keySet().toString() );
    }
    keyList.add( fieldName );
    fieldContainer.put( fieldName , field );
  }

  public void update(final IField field) throws IOException {
    fieldContainer.put(field.getName(), field);
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
    String[] keyArray = new String[ keyList.size() ];
    return keyList.toArray( keyArray );
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.STRUCT;
  }

  @Override
  public void merge( final IField target ) throws IOException {
    if ( ! ( target instanceof StructContainerField ) ) {
      throw new UnsupportedOperationException( "target is not StructContainerField." );
    }
    StructContainerField targetField = (StructContainerField)target;
    for ( String targetKey : targetField.getKeys() ) {
      IField targetChildField = targetField.get( targetKey );
      if ( containsKey( targetKey ) ) {
        IField childField = get( targetKey );
        if ( targetChildField.getFieldType() != childField.getFieldType()
            && childField.getFieldType() != FieldType.UNION ) {
          UnionField newField = new UnionField( childField.getName() , childField.getProperties() );
          newField.set( childField );
          childField = newField;
          update( childField );
        }
        childField.merge( targetChildField );
      } else {
        set( targetChildField );
      }
    }
  }

  @Override
  public Map<Object,Object> toJavaObject() throws IOException {
    LinkedHashMap<Object,Object> schemaJavaObject = new LinkedHashMap<Object,Object>();
    schemaJavaObject.put( "name" , getName() );
    schemaJavaObject.put( "type" , getFieldType().toString() );
    schemaJavaObject.put( "properties" , getProperties().toMap() );
    List<Object> childList = new ArrayList<Object>();
    for ( String key : getKeys() ) {
      childList.add( get( key ).toJavaObject() );
    }
    schemaJavaObject.put( "child" , childList );
    return schemaJavaObject;
  }

}
