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

public class TestUnionField {

  @Test
  public void T_newInstance_1() {
    IField field = new UnionField( "test" );
  }

  @Test
  public void T_newInstance_2() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new UnionField( "test" , prop );
  }

  @Test
  public void T_getName_1() {
    IField field = new UnionField( "test" );
    assertEquals( field.getName() , "test" );
  }

  @Test
  public void T_getProperties_1() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    IField field = new UnionField( "test" , prop );
    Properties prop2 = field.getProperties();
    assertEquals( prop2.get( "key1" ) , "value1" );
  }

  @Test
  public void T_getFieldType_1() {
    IField field = new UnionField( "test" );
    assertEquals( FieldType.UNION , field.getFieldType() );
  }

  @Test
  public void T_set_get_1() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new IntegerField( "key1" ) );
    IField child = field.get( IntegerField.class.getName() );
    assertTrue( ( child instanceof IntegerField ) );
  }

  @Test
  public void T_set_get_2() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new IntegerField( "key1" ) );
    field.set( new StringField( "key1" ) );
    IField child = field.get( IntegerField.class.getName() );
    assertTrue( ( child instanceof IntegerField ) );

    IField child2 = field.get( StringField.class.getName() );
    assertTrue( ( child2 instanceof StringField ) );
  }

  @Test
  public void T_set_get_3() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new IntegerField( "key1" ) );
    assertThrows( IOException.class ,
      () -> {
        field.set( new IntegerField( "key2" ) );
      }
    );
  }

  @Test
  public void T_containsKey_1() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new IntegerField( "key1" ) );
    assertTrue( field.containsKey( IntegerField.class.getName() ) );
  }

  @Test
  public void T_containsKey_2() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new IntegerField( "key1" ) );
    assertFalse( field.containsKey( "key1" ) );
  }

  @Test
  public void T_merge_1() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new StringField( "key1" ) );

    INamedContainerField field2 = new UnionField( "test" );
    field2.set( new IntegerField( "key2" ) );

    field.merge( field2 );

    assertTrue( field.containsKey( StringField.class.getName() ) );
    assertTrue( field.containsKey( IntegerField.class.getName() ) );
  }

  @Test
  public void T_merge_2() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new StringField( "key1" ) );

    INamedContainerField field2 = new UnionField( "test" );
    field2.set( new StringField( "key2" ) );

    field.merge( field2 );

    assertTrue( field.containsKey( StringField.class.getName() ) );
  }

  @Test
  public void T_merge_3() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    field.set( new StringField( "key1" ) );

    field.merge( new IntegerField( "key2" ) );

    assertTrue( field.containsKey( StringField.class.getName() ) );
    assertTrue( field.containsKey( IntegerField.class.getName() ) );
  }

  @Test
  public void T_merge_4() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    StructContainerField structField = new StructContainerField( "key1" );
    structField.set( new StringField( "s1" ) );
    field.set( structField );

    INamedContainerField field2 = new UnionField( "test" );
    StructContainerField structField2 = new StructContainerField( "key1" );
    structField2.set( new StringField( "s2" ) );
    field2.set( structField2 );

    field.merge( field2 );

    assertTrue( field.containsKey( StructContainerField.class.getName() ) );
    StructContainerField newStructField =
        (StructContainerField)( field.get( StructContainerField.class.getName() ) );
    assertTrue( newStructField.containsKey( "s1" ) );
    assertTrue( newStructField.containsKey( "s2" ) );
  }

  @Test
  public void T_merge_5() throws IOException {
    INamedContainerField field = new UnionField( "test" );
    StructContainerField structField = new StructContainerField( "key1" );
    structField.set( new StringField( "s1" ) );
    field.set( structField );

    StructContainerField structField2 = new StructContainerField( "key1" );
    structField2.set( new StringField( "s2" ) );

    field.merge( structField2 );

    assertTrue( field.containsKey( StructContainerField.class.getName() ) );
    StructContainerField newStructField =
        (StructContainerField)( field.get( StructContainerField.class.getName() ) );
    assertTrue( newStructField.containsKey( "s1" ) );
    assertTrue( newStructField.containsKey( "s2" ) );
  }

}
