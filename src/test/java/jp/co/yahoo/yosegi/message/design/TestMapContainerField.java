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

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestMapContainerField {

  @Test
  public void T_newInstance_1() {
    IField field = new MapContainerField( "test" , new StringField( "child" ) );
  }

  @Test
  public void T_newInstance_2() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new MapContainerField( "test" , new StringField( "child" ) , prop );
  }

  @Test
  public void T_getName_1() {
    IField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertEquals( field.getName() , "test" );
  }

  @Test
  public void T_getProperties_1() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new MapContainerField( "test" , new StringField( "child" ) , prop );
    Properties prop2 = field.getProperties();
    assertEquals( prop2.get( "key1" ) , "value1" );
  }

  @Test
  public void T_getFieldType_1() {
    IField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertEquals( FieldType.MAP , field.getFieldType() );
  }

  @Test
  public void T_set_get_1() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    field.set( new IntegerField( "key1" ) );
    IField child = field.get( "key1" );
    assertTrue( ( child instanceof IntegerField ) );
  }

  @Test
  public void T_set_get_2() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    IField child = field.get( "key1" );
    assertTrue( ( child instanceof StringField ) );
  }

  @Test
  public void T_containsKey_1() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    field.set( new IntegerField( "key1" ) );
    assertTrue( field.containsKey( "key1" ) );
  }

  @Test
  public void T_containsKey_2() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertFalse( field.containsKey( "key1" ) );
  }

  @Test
  public void T_merge_1() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertFalse( field.containsKey( "key1" ) );
    field.set( new StringField( "key1" ) );

    assertTrue( field.containsKey( "key1" ) );
    assertFalse( field.containsKey( "key2" ) );

    INamedContainerField field2 = new MapContainerField( "test" , new IntegerField( "child" ) );
    field2.set( new IntegerField( "key2" ) );
    field.merge( field2 );

    assertTrue( field.containsKey( "key1" ) );
    assertTrue( field.containsKey( "key2" ) );

    assertTrue( ( field.get( "key1" ) instanceof StringField ) );
    assertTrue( ( field.get( "key2" ) instanceof IntegerField ) );
  }

  @Test
  public void T_merge_2() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertFalse( field.containsKey( "key1" ) );
    field.set( new StringField( "key1" ) );

    INamedContainerField field2 = new MapContainerField( "test" , new IntegerField( "child" ) );
    field2.set( new StringField( "key1" ) );

    field.merge( field2 );

    assertTrue( ( field.get( "key1" ) instanceof StringField ) );
  }

  @Test
  public void T_merge_3() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertFalse( field.containsKey( "key1" ) );
    field.set( new StringField( "key1" ) );

    INamedContainerField field2 = new MapContainerField( "test" , new IntegerField( "child" ) );
    field2.set( new IntegerField( "key1" ) );

    field.merge( field2 );

    IField newField = field.get( "key1" );
    assertTrue( ( newField instanceof UnionField ) );

    UnionField union = (UnionField)newField;
    assertTrue( union.containsKey( StringField.class.getName() ) );
    assertTrue( union.containsKey( IntegerField.class.getName() ) );
  }

  @Test
  public void T_merge_4() throws IOException {
    INamedContainerField field = new MapContainerField( "test" , new StringField( "child" ) );
    assertFalse( field.containsKey( "key1" ) );
    field.set( new StringField( "key1" ) );

    assertThrows( UnsupportedOperationException.class ,
      () -> {
        field.merge( new StringField( "key2" ) );
      }
    );
  }

}
