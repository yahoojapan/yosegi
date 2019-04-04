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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class ArrayContainerField extends SimpleField implements IContainerField {
  private IField childField;

  /**
   * Creates an object representing Array with the specified parameters.
   */
  public ArrayContainerField(final String name, final IField childField) {
    super(name);
    this.childField = childField;
  }

  /**
   * Creates an object representing Array with the specified parameters.
   */
  public ArrayContainerField(
      final String name,
      final IField childField,
      final Properties properties) {
    super(name, properties);
    this.childField = childField;
  }

  @Override
  public IField getField() {
    return childField;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.ARRAY;
  }

  @Override
  public void merge( final IField target ) throws IOException {
    if ( ! ( target instanceof ArrayContainerField ) ) {
      throw new UnsupportedOperationException( "target is not ArrayContainerField." );
    }
    ArrayContainerField targetField = (ArrayContainerField)target;
    IField targetChildField = targetField.getField();
    if ( targetChildField.getFieldType() != childField.getFieldType() 
        && childField.getFieldType() != FieldType.UNION ) {
      UnionField newField = new UnionField( getName() , getProperties() );
      newField.set( childField );
      childField = newField;
    }
    childField.merge( targetChildField );
  }

  @Override
  public Map<Object,Object> toJavaObject() throws IOException {
    LinkedHashMap<Object,Object> schemaJavaObject = new LinkedHashMap<Object,Object>();
    schemaJavaObject.put( "name" , getName() );
    schemaJavaObject.put( "type" , getFieldType().toString() );
    schemaJavaObject.put( "properties" , getProperties().toMap() );
    List<Object> childList = new ArrayList<Object>();
    childList.add( getField().toJavaObject() );
    schemaJavaObject.put( "child" , childList );
    return schemaJavaObject;
  }

}
