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
package jp.co.yahoo.yosegi.binary.maker;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestConstantColumnBinaryMaker {

  @Test
  public void T_createBinary_boolean_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( true , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getBoolean() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_byte_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( (byte)20 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary  );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (byte)20 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getByte() );
    assertEquals( (byte)20 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getByte() );
    assertEquals( (byte)20 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getByte() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_short_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( (short)20 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getShort() );
    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getShort() );
    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getShort() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_int_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( 20 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( 20 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getInt() );
    assertEquals( 20 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getInt() );
    assertEquals( 20 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getInt() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  public void T_createBinary_long_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( 20l ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( 20l , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getLong() );
    assertEquals( 20l , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getLong() );
    assertEquals( 20l , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getLong() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_float_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( (float)0.1 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (float)0.1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getFloat() );
    assertEquals( (float)0.1 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getFloat() );
    assertEquals( (float)0.1 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getFloat() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_double_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( (double)0.1 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (double)0.1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getDouble() );
    assertEquals( (double)0.1 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getDouble() );
    assertEquals( (double)0.1 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getDouble() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_string_1() throws IOException{
    String str = "hogehoge";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( str ) , "hoge" , 3 )
;

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getString() );
    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getString() );
    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getString() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createBinary_bytes_1() throws IOException{
    String str = "hogehoge";
    byte[] bytes = str.getBytes( "UTF-8" );
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( bytes ) , "hoge" , 3 )
;

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getString() );
    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getString() );
    assertEquals( str , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getString() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_getPrimitiveObjectArray_1() throws IOException{
    String str = "hogehoge";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( str ) , "hoge" , 3 )
;

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    AllExpressionIndex index = new AllExpressionIndex( 10 );
    PrimitiveObject[] array = decodeColumn.getPrimitiveObjectArray( index , 0 , 10 );
    assertEquals( str ,  array[0].getString() );
    assertEquals( str ,  array[1].getString() );
    assertEquals( str ,  array[2].getString() );
    for( int i = 3 ; i < 10 ; i++ ){
      assertEquals( null ,  array[i] );
    }
  }

  @Test
  public void T_setPrimitiveObjectArray_1() throws IOException{
    String str = "hogehoge";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( str ) , "hoge" , 3 )
;

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    AllExpressionIndex index = new AllExpressionIndex( 10 );
    MyAllocator allocator = new MyAllocator();
    decodeColumn.setPrimitiveObjectArray( index , 0 , 10 , allocator );

    assertEquals( str ,  allocator.array[0].getString() );
    assertEquals( str ,  allocator.array[1].getString() );
    assertEquals( str ,  allocator.array[2].getString() );
    for( int i = 3 ; i < 10 ; i++ ){
      assertEquals( null ,  allocator.array[i] );
    }
  }

  private class MyAllocator implements IMemoryAllocator{

    public PrimitiveObject[] array = new PrimitiveObject[10];

    @Override
    public void setPrimitiveObject( final int index , final PrimitiveObject value ) throws IOException{
      array[index] = value;
    }
  }

}
