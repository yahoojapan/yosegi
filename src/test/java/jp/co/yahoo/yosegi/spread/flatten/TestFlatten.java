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
package jp.co.yahoo.yosegi.spread.flatten;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.message.objects.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class TestFlatten {

  private Spread createNewSpread() throws IOException {
    Spread spread = new Spread();
    Map<String,Object> root = new HashMap<String,Object>();
    root.put( "C1" , new StringObj( "a" ) );
    root.put( "C2" , new StringObj( "b" ) );
    root.put( "C3" , new StringObj( "c" ) );
    Map<String,Object> child = new HashMap<String,Object>();
    child.put( "C1" , new StringObj( "aa" ) );
    child.put( "C2" , new StringObj( "bb" ) );
    child.put( "C3" , new StringObj( "cc" ) );
    root.put( "c4" , child );
    spread.addRow( root );
    return spread;
  }

  @Test
  public void T_flatten_equalsSetValue_withV1Json() throws IOException {
    String json = "[";
    json += "{\"nodes\":[\"C1\"],\"link_name\":\"root_c1\"},";
    json += "{\"nodes\":[\"C2\"],\"link_name\":\"root_c2\"},";
    json += "{\"nodes\":[\"C3\"],\"link_name\":\"root_c3\"},";
    json += "{\"nodes\":[\"c4\",\"C1\"],\"link_name\":\"c4_c1\"},";
    json += "{\"nodes\":[\"c4\",\"C2\"],\"link_name\":\"c4_c2\"},";
    json += "{\"nodes\":[\"c4\",\"C3\"],\"link_name\":\"c4_c3\"}";
    json += "]";
    Configuration config = new Configuration();
    config.set( "spread.reader.flatten.column" , json );
    IFlattenFunction flattenFunction = FlattenFunctionFactory.get( config );
    assertTrue( ( flattenFunction instanceof FlattenFunction ) );
    Spread flattenSpread = flattenFunction.flatten( createNewSpread() );
    assertEquals( "a" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c3" ).get(0).getRow() ) ).getString() );
    assertEquals( "aa" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "bb" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "cc" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c3" ).get(0).getRow() ) ).getString() );
  }

  @Test
  public void T_flatten_equalsSetValue_withV2Json() throws IOException {
    String json = "{";
    json += "\"version\":2,";
    json += "\"replacement\":\"lower\",";
    json += "\"flatten\":[";
    json += "{\"prefix\":\"root\",\"target\":[\"C1\",\"C2\",\"C3\"]},";
    json += "{\"column\":[\"c4\"],\"prefix\":\"c4\",\"target\":[\"C1\",\"C2\",\"C3\"]}";
    json += "]}";
    Configuration config = new Configuration();
    config.set( "spread.reader.flatten.column" , json );
    IFlattenFunction flattenFunction = FlattenFunctionFactory.get( config );
    assertTrue( ( flattenFunction instanceof FlattenFunction ) );
    Spread flattenSpread = flattenFunction.flatten( createNewSpread() );
    assertEquals( "a" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c3" ).get(0).getRow() ) ).getString() );
    assertEquals( "aa" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "bb" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "cc" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c3" ).get(0).getRow() ) ).getString() );
  }

  @Test
  public void T_flatten_equalsSetValue_withV1AndV2() throws IOException {
    String jsonV1 = "[";
    jsonV1 += "{\"nodes\":[\"C1\"],\"link_name\":\"root_c1\"},";
    jsonV1 += "{\"nodes\":[\"C2\"],\"link_name\":\"root_c2\"},";
    jsonV1 += "{\"nodes\":[\"C3\"],\"link_name\":\"root_c3\"}";
    jsonV1 += "]";
    String jsonV2 = "{";
    jsonV2 += "\"version\":2,";
    jsonV2 += "\"replacement\":\"lower\",";
    jsonV2 += "\"flatten\":[";
    jsonV2 += "{\"column\":[\"c4\"],\"prefix\":\"c4\",\"target\":[\"C1\",\"C2\",\"C3\"]}";
    jsonV2 += "]}";
    Configuration config = new Configuration();
    config.set( "spread.reader.flatten.column.v1" , jsonV1 );
    config.set( "spread.reader.flatten.column.v2" , jsonV2 );
    IFlattenFunction flattenFunction = FlattenFunctionFactory.get( config );
    assertTrue( ( flattenFunction instanceof FlattenFunction ) );
    Spread flattenSpread = flattenFunction.flatten( createNewSpread() );
    assertEquals( "a" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( flattenSpread.getColumn( "root_c3" ).get(0).getRow() ) ).getString() );
    assertEquals( "aa" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c1" ).get(0).getRow() ) ).getString() );
    assertEquals( "bb" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c2" ).get(0).getRow() ) ).getString() );
    assertEquals( "cc" , ( (PrimitiveObject)( flattenSpread.getColumn( "c4_c3" ).get(0).getRow() ) ).getString() );
  }

  @Test
  public void T_flatten_equalsSetValue_withDuplicateLinkName() throws IOException {
    String json = "[";
    json += "{\"nodes\":[\"C1\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"C2\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"C3\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C1\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C2\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C3\"],\"link_name\":\"c\"}";
    json += "]";
    Configuration config = new Configuration();
    config.set( "spread.reader.flatten.column" , json );
    IFlattenFunction flattenFunction = FlattenFunctionFactory.get( config );
    assertTrue( ( flattenFunction instanceof FlattenFunction ) );
    Spread flattenSpread = flattenFunction.flatten( createNewSpread() );
    assertEquals( "a" , ( (PrimitiveObject)( flattenSpread.getColumn( "c" ).get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_0" ).get(0).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_1" ).get(0).getRow() ) ).getString() );
    assertEquals( "aa" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_2" ).get(0).getRow() ) ).getString() );
    assertEquals( "bb" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_3" ).get(0).getRow() ) ).getString() );
    assertEquals( "cc" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_4" ).get(0).getRow() ) ).getString() );
  }

  @Test
  public void T_flatten_equalsSetValue_withDuplicateLinkName2() throws IOException {
    String json = "[";
    json += "{\"nodes\":[\"C1\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"C2\"],\"link_name\":\"c_0\"},";
    json += "{\"nodes\":[\"C3\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C1\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C2\"],\"link_name\":\"c\"},";
    json += "{\"nodes\":[\"c4\",\"C3\"],\"link_name\":\"c\"}";
    json += "]";
    Configuration config = new Configuration();
    config.set( "spread.reader.flatten.column" , json );
    IFlattenFunction flattenFunction = FlattenFunctionFactory.get( config );
    assertTrue( ( flattenFunction instanceof FlattenFunction ) );
    Spread flattenSpread = flattenFunction.flatten( createNewSpread() );
    assertEquals( "a" , ( (PrimitiveObject)( flattenSpread.getColumn( "c" ).get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_0" ).get(0).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_1" ).get(0).getRow() ) ).getString() );
    assertEquals( "aa" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_2" ).get(0).getRow() ) ).getString() );
    assertEquals( "bb" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_3" ).get(0).getRow() ) ).getString() );
    assertEquals( "cc" , ( (PrimitiveObject)( flattenSpread.getColumn( "c_4" ).get(0).getRow() ) ).getString() );
  }

}
