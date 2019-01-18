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
package jp.co.yahoo.yosegi.spread.analyzer;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.*;

public class TestSpreadColumnAnalizer {

  @Test
  public void T_getAnalizer_1() throws IOException{
    SpreadColumn column = new SpreadColumn( "spread" );
    Map<Object,Object> data = new HashMap<Object,Object>();
    data.put( "s1" , new StringObj( "abc" ) );
    data.put( "i1" , new IntegerObj( 100 ) );
    column.add( ColumnType.SPREAD , data , 0 );

    data.clear();
    data.put( "s1" , new StringObj( "abc" ) );
    data.put( "i2" , new IntegerObj( 100 ) );
    column.add( ColumnType.SPREAD , data , 1 );

    SpreadColumnAnalizer a = new SpreadColumnAnalizer( column );

    IColumnAnalizeResult result = a.analize();
    List<IColumnAnalizeResult> childResult = result.getChild();
    assertEquals( 3 , childResult.size() );
  }

}
