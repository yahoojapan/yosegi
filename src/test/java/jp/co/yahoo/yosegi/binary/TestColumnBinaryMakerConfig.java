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
package jp.co.yahoo.yosegi.binary;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestColumnBinaryMakerConfig{

  private static Stream<Arguments> parametersForT_getColumnMaker_1(){
    return Stream.of(
      arguments( ColumnType.UNION , DumpUnionColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.ARRAY , DumpArrayColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.SPREAD , DumpSpreadColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.BOOLEAN , DumpBooleanColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.BYTE , UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.BYTES , DumpBytesColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.DOUBLE , UnsafeRangeDumpDoubleColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.FLOAT , UnsafeRangeDumpFloatColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.INTEGER , UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.LONG , UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.SHORT , UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.STRING , UnsafeOptimizeDumpStringColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.NULL , UnsupportedColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.EMPTY_ARRAY , UnsupportedColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.EMPTY_SPREAD , UnsupportedColumnBinaryMaker.class.getName() ),
      arguments( ColumnType.UNKNOWN , UnsupportedColumnBinaryMaker.class.getName() )
    );
  }

  @Test
  public void T_newInstance_1() throws IOException{
    new ColumnBinaryMakerConfig();
  }

  @Test
  public void T_newInstance_2() throws IOException{
    new ColumnBinaryMakerConfig( new ColumnBinaryMakerConfig() );
  }

  @ParameterizedTest
  @MethodSource( "parametersForT_getColumnMaker_1" )
  public void T_getColumnMaker_1( final ColumnType columnType , final String className ) throws IOException{
    ColumnBinaryMakerConfig config = new ColumnBinaryMakerConfig();
    IColumnBinaryMaker maker = config.getColumnMaker( columnType );
    assertEquals( maker.getClass().getName() , className );
  }

}
