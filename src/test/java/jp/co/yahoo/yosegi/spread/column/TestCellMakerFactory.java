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
package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.message.objects.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestCellMakerFactory {

  @Test
  public void getCellFromBooleanType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.BOOLEAN );
    assertTrue( ( maker.create( null ) instanceof BooleanCell ) );
  }

  @Test
  public void getCellFromByteType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.BYTE );
    assertTrue( ( maker.create( null ) instanceof ByteCell ) );
  }

  @Test
  public void getCellFromBytesType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.BYTES );
    assertTrue( ( maker.create( null ) instanceof BytesCell ) );
  }

  @Test
  public void getCellFromFloatType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.FLOAT );
    assertTrue( ( maker.create( null ) instanceof FloatCell ) );
  }

  @Test
  public void getCellIntegerFloatType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.INTEGER );
    assertTrue( ( maker.create( null ) instanceof IntegerCell ) );
  }

  @Test
  public void getCellIntegerLongType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.LONG );
    assertTrue( ( maker.create( null ) instanceof LongCell ) );
  }

  @Test
  public void getCellIntegerShortType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.SHORT );
    assertTrue( ( maker.create( null ) instanceof ShortCell ) );
  }

  @Test
  public void getCellIntegerType() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    assertTrue( ( maker.create( null ) instanceof StringCell ) );
  }

  @Test
  public void getCellFromUnionTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.UNION );
      }
    );
  }

  @Test
  public void getCellFromArrayTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.ARRAY );
      }
    );
  }

  @Test
  public void getCellFromSpreadTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.SPREAD );
      }
    );
  }

  @Test
  public void getCellFromNullTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.NULL );
      }
    );
  }

  @Test
  public void getCellFromEmptyArrayTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.EMPTY_ARRAY );
      }
    );
  }

  @Test
  public void getCellFromEmptySpreadTypeWithException() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.EMPTY_SPREAD );
      }
    );
  }

}
