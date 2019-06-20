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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestProperties {

  @Test
  public void T_createNewInstance_nothingToDo() {
    Properties prop = new Properties();
  }

  @Test
  public void T_createNewInstance_nothingToDo_withCustomMap() {
    Map<String,String> map = new HashMap<String,String>();
    Properties prop = new Properties( map );
  }

  @Test
  public void T_setAndGet_equalsSetValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    assertEquals( prop.get( "key1" ) , "value1" );
  }

  @Test
  public void T_setAndGet_equalsSetValue_whenOverwrite() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    assertEquals( prop.get( "key1" ) , "value1" );
    prop.set( "key1" , "value1-2" );
    assertEquals( prop.get( "key1" ) , "value1-2" );
  }

  @Test
  public void T_get_equalsSetValue_fromCustomMap() {
    Map<String,String> map = new HashMap<String,String>();
    map.put( "key1" , "value1" );
    Properties prop = new Properties( map );
    assertEquals( prop.get( "key1" ) , "value1" );
  }

  @Test
  public void T_get_equalsSetValue_whenOverwriteCustomMapValue() {
    Map<String,String> map = new HashMap<String,String>();
    map.put( "key1" , "value1" );
    Properties prop = new Properties( map );
    assertEquals( prop.get( "key1" ) , "value1" );
    prop.set( "key1" , "value1-2" );
    assertEquals( prop.get( "key1" ) , "value1-2" );
  }

  @Test
  public void T_toMap_createNewMap() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    prop.set( "key2" , "value2" );
    Map<String,String> map = prop.toMap();
    assertEquals( map.size() , 2 );
    assertEquals( map.get( "key1" ) , "value1" );
    assertEquals( map.get( "key2" ) , "value2" );
  }

  @Test
  public void T_toMap_createEmptyNewMap_whenKeyIsEmpty() {
    Properties prop = new Properties();
    Map<String,String> map = prop.toMap();
    assertEquals( map.size() , 0 );
  }

  @Test
  public void T_getKey_equalsSetKey() {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    prop.set( "key2" , "value2" );
    Set<String> set = prop.getKey();
    assertEquals( set.size() , 2 );
    assertTrue( set.contains( "key1" ) );
    assertTrue( set.contains( "key2" ) );
  }

  @Test
  public void T_getKey_sizeIsZero_whenEmpty() {
    Properties prop = new Properties();
    Set<String> set = prop.getKey();
    assertEquals( set.size() , 0 );
  }

  @Test
  public void T_containsKey_true_whenKeyIsExists() throws IOException {
    Properties prop = new Properties();
    prop.set( "key1" , "value1" );
    assertTrue( prop.containsKey( "key1" ) );
  }

  @Test
  public void T_containsKey_false_whenKeyIsNotExists() throws IOException {
    Properties prop = new Properties();
    assertFalse( prop.containsKey( "key1" ) );
  }

  @Test
  public void T_getInt_equalsSetValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "100" );
    assertEquals( prop.getInt( "key1" ) , 100 );
  }

  @Test
  public void T_getInt_defaultIntValue_whenKeyIsNotExists() {
    Properties prop = new Properties();
    assertEquals( prop.getInt( "key1" , 100 ) , 100 );
  }

  @Test
  public void T_getInt_equalsSetValue_whenMaxInt() {
    Properties prop = new Properties();
    prop.set( "key1" , Integer.valueOf( Integer.MAX_VALUE ).toString() );
    assertEquals( prop.getInt( "key1" ) , Integer.MAX_VALUE );
  }

  @Test
  public void T_getInt_equalsSetValue_whenMinInt() {
    Properties prop = new Properties();
    prop.set( "key1" , Integer.valueOf( Integer.MIN_VALUE ).toString() );
    assertEquals( prop.getInt( "key1" ) , Integer.MIN_VALUE );
  }

  @Test
  public void T_getInt_throwsException_whenOutOfRangeOfMaxIntValue() {
    Properties prop = new Properties();
    prop.set( "key1" , Long.valueOf( (long)Integer.MAX_VALUE + 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getInt( "key1" );
      }
    );
  }

  @Test
  public void T_getInt_throwsException_whenOutOfRangeOfMinIntValue() {
    Properties prop = new Properties();
    prop.set( "key1" , Long.valueOf( (long)Integer.MIN_VALUE - 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getInt( "key1" );
      }
    );
  }

  @Test
  public void T_getInt_throwsException_whenString() {
    Properties prop = new Properties();
    prop.set( "key1" , "a" );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getInt( "key1" );
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "100" );
    assertEquals( prop.getLong( "key1" ) , 100L );
  }

  @Test
  public void T_getLong_defaultLongValue_whenKeyIsNotExists() {
    Properties prop = new Properties();
    assertEquals( prop.getLong( "key1" , 100L ) , 100L );
  }

  @Test
  public void T_getLong_equalsSetValue_whenMaxValueLong() {
    Properties prop = new Properties();
    prop.set( "key1" , Long.valueOf( Long.MAX_VALUE ).toString() );
    assertEquals( prop.getLong( "key1" ) , Long.MAX_VALUE );
  }

  @Test
  public void T_getLong_equalsSetValue_whenMinValueLong() {
    Properties prop = new Properties();
    prop.set( "key1" , Long.valueOf( Long.MIN_VALUE ).toString() );
    assertEquals( prop.getLong( "key1" ) , Long.MIN_VALUE );
  }

  @Test
  public void T_getLong_throwsException_whenOutOfRangeOfMaxLongValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "9223372036854775808" );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getLong( "key1" );
      }
    );
  }

  @Test
  public void T_getLong_throwsException_whenOutOfRangeOfMinLongValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "-9223372036854775809" );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getLong( "key1" );
      }
    );
  }

  @Test
  public void T_getLong_throwsException_whenString() {
    Properties prop = new Properties();
    prop.set( "key1" , "a" );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getLong( "key1" );
      }
    );
  }

  @Test
  public void T_getDouble_equalsSetValue() {
    Properties prop = new Properties();
    prop.set( "key1" , "0.1" );
    assertEquals( prop.getDouble( "key1" ) , 0.1d );
  }

  @Test
  public void T_getDouble_defaultDoubleValue_whenKeyIsNotExists() {
    Properties prop = new Properties();
    assertEquals( prop.getDouble( "key1" , 0.1d ) , 0.1d );
  }

  @Test
  public void T_getDouble_throwsException_whenString() {
    Properties prop = new Properties();
    prop.set( "key1" , "a" );
    assertThrows( NumberFormatException.class ,
      () -> {
        prop.getDouble( "key1" );
      }
    );
  }

  @Test
  public void T_getObject_classInstance() throws IOException {
    Properties prop = new Properties();
    prop.set( "key1" , "java.lang.String" );
    Object obj = prop.getObject( "key1" );
    assertTrue( ( obj instanceof String ) );
  }

  @Test
  public void T_getObject_defaultClassInstance_whenKeyIsNotExists() throws IOException {
    Properties prop = new Properties();
    Object obj = prop.getObject( "key1" , "java.lang.String" );
    assertTrue( ( obj instanceof String ) );
  }

  @Test
  public void T_getObject_throwsException_whenNotFoundClass() {
    Properties prop = new Properties();
    prop.set( "key1" , "NotFound" );
    assertThrows( IOException.class ,
      () -> {
        prop.getObject( "key1" );
      }
    );
  }

}
