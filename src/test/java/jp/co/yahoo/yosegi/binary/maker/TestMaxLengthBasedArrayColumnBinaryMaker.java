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
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import jp.co.yahoo.yosegi.message.objects.*;

public class TestMaxLengthBasedArrayColumnBinaryMaker{


  private class TestArrayMemoryAllocator implements IMemoryAllocator{

    public final List<Integer> startList;
    public final List<Integer> endList;

    public TestArrayMemoryAllocator(){
      startList = new ArrayList<Integer>();
      endList = new ArrayList<Integer>();
      for( int i = 0 ; i < 6 ; i++ ){
        startList.add(null);
        endList.add(null);
      }
    }

    @Override
    public void setNull( final int index ){
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
    }

    @Override
    public void setByte( final int index , final byte value ) throws IOException{
    }

    @Override
    public void setShort( final int index , final short value ) throws IOException{
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException{
    }

    @Override
    public void setLong( final int index , final long value ) throws IOException{
    }

    @Override
    public void setFloat( final int index , final float value ) throws IOException{
    }

    @Override
    public void setDouble( final int index , final double value ) throws IOException{
    }

    @Override
    public void setBytes( final int index , final byte[] value ) throws IOException{
    }

    @Override
    public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
    }

    @Override
    public void setString( final int index , final String value ) throws IOException{
    }

    @Override
    public void setString( final int index , final char[] value ) throws IOException{
    }

    @Override
    public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
    }

    @Override
    public void setValueCount( final int index ) throws IOException{

    }

    @Override
    public int getValueCount() throws IOException{
      return 0;
    }

    @Override
    public void setArrayIndex( final int index , final int start , final int end ) throws IOException{
      startList.set( index , start );
      endList.set( index , end );
    }

    @Override
    public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
      return new TestArrayMemoryAllocator();
    }
  }

  @Test
  public void T_toBinary_equalsSetValue() throws IOException{
    IColumn column = new ArrayColumn( "array" );
    List<Object> value = new ArrayList<Object>();
    value.add( new StringObj( "a" ) );
    value.add( new StringObj( "b" ) );
    value.add( new StringObj( "c" ) );
    column.add( ColumnType.ARRAY , value , 0 );
    column.add( ColumnType.ARRAY , value , 1 );
    column.add( ColumnType.ARRAY , value , 2 );
    column.add( ColumnType.ARRAY , value , 3 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new MaxLengthBasedArrayColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "array" );
    assertEquals( columnBinary.rowCount , 4 );
    assertEquals( columnBinary.columnType , ColumnType.ARRAY );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    IColumn expandColumn = decodeColumn.getColumn(0);
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
    for( int i = 0 ; i < 3 * 4 ; i+=3 ){
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i).getRow() ) ).getString() , "a" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+1).getRow() ) ).getString() , "b" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+2).getRow() ) ).getString() , "c" );
    }
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
  }

  @Test
  public void T_loadInMemoryStorage_equalsSetValue() throws IOException{
    IColumn column = new ArrayColumn( "array" );
    List<Object> value = new ArrayList<Object>();
    value.add( new StringObj( "a" ) );
    value.add( new StringObj( "b" ) );
    value.add( new StringObj( "c" ) );
    column.add( ColumnType.ARRAY , value , 0 );
    column.add( ColumnType.ARRAY , value , 1 );
    column.add( ColumnType.ARRAY , value , 2 );
    column.add( ColumnType.ARRAY , value , 3 );
    column.add( ColumnType.ARRAY , value , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new MaxLengthBasedArrayColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "array" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.ARRAY );

    TestArrayMemoryAllocator allocator = new TestArrayMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );
    assertEquals( allocator.startList.size() , 6 );
    assertEquals( allocator.endList.size() , 6 );

    assertEquals( allocator.startList.get(0).intValue() , 0 );
    assertEquals( allocator.startList.get(1).intValue() , 3 );
    assertEquals( allocator.startList.get(2).intValue() , 6 );
    assertEquals( allocator.startList.get(3).intValue() , 9 );
    assertEquals( allocator.startList.get(4) , null );
    assertEquals( allocator.startList.get(5).intValue() , 12 );

    assertEquals( allocator.endList.get(0).intValue() , 3 );
    assertEquals( allocator.endList.get(1).intValue() , 3 );
    assertEquals( allocator.endList.get(2).intValue() , 3 );
    assertEquals( allocator.endList.get(3).intValue() , 3 );
    assertEquals( allocator.endList.get(4) , null );
    assertEquals( allocator.endList.get(5).intValue() , 3 );
  }

}
