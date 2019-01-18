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
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

public class TestDumpBooleanColumnBinaryMaker {

  private class TestBooleanMemoryAllocator implements IMemoryAllocator{

    public final List<Boolean> list;

    public TestBooleanMemoryAllocator(){
      list = new ArrayList<Boolean>();
      for( int i = 0 ; i < 6 ; i++ ){
        list.add( null );
      }
    }

    @Override
    public void setNull( final int index ){
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
      list.set( index , value );
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
    }

    @Override
    public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
      return null;
    }
  }

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "boolean" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpBooleanColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "boolean" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertTrue( ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getBoolean() );
    assertFalse( ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getBoolean() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "boolean" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 1 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpBooleanColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "boolean" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    TestBooleanMemoryAllocator allocator = new TestBooleanMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );

    assertEquals( allocator.list.get( 0 ).booleanValue() , true );
    assertEquals( allocator.list.get( 1 ).booleanValue() , false );
    assertEquals( allocator.list.get( 2 ) , null );
    assertEquals( allocator.list.get( 3 ) , null );
    assertEquals( allocator.list.get( 4 ) , null );
    assertEquals( allocator.list.get( 5 ).booleanValue() , true );
  }

}

