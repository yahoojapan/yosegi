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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestNullField {

  @Test
  public void T_createNewInstance_void_withFieldName() {
    IField field = new NullField( "test" );
  }

  @Test
  public void T_createNewInstance_void_withFieldNameAndProperties() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new NullField( "test" , prop );
  }

  @Test
  public void T_getName_sameAsTheSetValue() {
    IField field = new NullField( "test" );
    assertEquals( field.getName() , "test" );
  }

  @Test
  public void T_getProperties_sameAsTheSetValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new NullField( "test" , prop );
    Properties prop2 = field.getProperties();
    assertEquals( prop2.get( "key1" ) , "value1" );
  }

  @Test
  public void T_getFieldType_typeIsNull() {
    IField field = new NullField( "test" );
    assertEquals( FieldType.NULL , field.getFieldType() );
  }

}
