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
import java.nio.ByteBuffer;

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
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

public class TestDumpBytesColumnBinaryMaker {

  private class TestBytesMemoryAllocator implements IMemoryAllocator{

    public final List<byte[]> list;

    public TestBytesMemoryAllocator(){
      list = new ArrayList<byte[]>();
      for( int i = 0 ; i < 6 ; i++ ){
        list.add( null );
      }
    }

    @Override
    public void setNull( final int index ) {
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
      list.set( index , value );
    }

    @Override
    public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
      byte[] buffer = new byte[length];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
      wrapBuffer.put( value , start , length );
      list.set( index , buffer );
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
    }

    @Override
    public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
      return null;
    }
  }

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "bytes" );
    column.add( ColumnType.BYTES , new BytesObj( new String("a").getBytes() ) , 0 );
    column.add( ColumnType.BYTES , new BytesObj( new String("b").getBytes() ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpBytesColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "bytes" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( "a" , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getString() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "bytes" );
    column.add( ColumnType.BYTES , new BytesObj( new String("a").getBytes() ) , 0 );
    column.add( ColumnType.BYTES , new BytesObj( new String("b").getBytes() ) , 1 );
    column.add( ColumnType.BYTES , new BytesObj( new String("b").getBytes() ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpBytesColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "bytes" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    TestBytesMemoryAllocator allocator = new TestBytesMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );

    assertEquals( "a" , new String( allocator.list.get(0) ) );
    assertEquals( "b" , new String( allocator.list.get(1) ) );
    assertEquals( null , allocator.list.get(2) );
    assertEquals( null , allocator.list.get(3) );
    assertEquals( null , allocator.list.get(4) );
    assertEquals( "b" , new String( allocator.list.get(5) ) );

  }

}

