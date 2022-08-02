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
package jp.co.yahoo.yosegi.message.parser;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.filter.BackwardMatchStringFilter;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.StringExtractNode;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.reader.*;
import jp.co.yahoo.yosegi.writer.*;

public class TestYosegiSchemaSpreadReader{

  @Test
  public void T_parser_1() throws IOException{
    Spread spread = new Spread();
    Map<String,Object> data = new HashMap<String,Object>();
    data.put( "col3" , new StringObj( "a" ) );
    spread.addRow( data );
    data.put( "col3" , new StringObj( "b" ) );
    spread.addRow( data );

    YosegiSchemaSpreadReader reader = new YosegiSchemaSpreadReader( spread );
    assertTrue(reader.hasNext());
    IParser parser = reader.next();
    PrimitiveObject a = parser.get( "col3" );
    assertEquals( "a" , a.getString() );
    assertTrue(reader.hasNext());
    parser = reader.next();
    a = parser.get( "col3" );
    assertEquals( "b" , a.getString() );
    assertFalse(reader.hasNext());
    reader.close();
  }

}
