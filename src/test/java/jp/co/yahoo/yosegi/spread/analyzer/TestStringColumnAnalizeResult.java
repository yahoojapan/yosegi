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
import jp.co.yahoo.yosegi.spread.column.NullColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestStringColumnAnalizeResult {

  @Test
  public void T_getAnalizer_1() throws IOException{
    StringColumnAnalizeResult result = new StringColumnAnalizeResult( "name" , 100 , true , 10 , 90 , 2 , 40 , 0 , 40 , 50 , 30 , 20 , 10 , 100 , 90 , 80 , "a" , "z"  );
    assertEquals( "name" , result.getColumnName() );
    assertEquals( ColumnType.STRING , result.getColumnType() );
    assertEquals( 100 , result.getColumnSize() );
    assertEquals( true , result.maybeSorted() );
    assertEquals( 10 , result.getNullCount() );
    assertEquals( 90 , result.getRowCount() );
    assertEquals( 2 , result.getUniqCount() );
    assertEquals( 40 , result.getLogicalDataSize() );
    assertEquals( 50 , result.getTotalUtf8ByteSize() );
    assertEquals( 30 , result.getUniqLogicalDataSize() );
    assertEquals( 20 , result.getUniqUtf8ByteSize() );
    assertEquals( 10 , result.getMinCharLength() );
    assertEquals( 100 , result.getMaxCharLength() );
    assertEquals( 90 , result.getMinUtf8Bytes() );
    assertEquals( 80 , result.getMaxUtf8Bytes() );
    assertEquals( "a" , result.getMin() );
    assertEquals( "z" , result.getMax() );
  }

}
