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
package jp.co.yahoo.yosegi.binary.maker.index;

import java.io.IOException;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.maker.IDicManager;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.*;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;

public class TestBufferDirectSequentialStringCellIndex{

  private class TestDicManager implements IDicManager {

    private final List<PrimitiveObject> dic;

    public TestDicManager( final List<PrimitiveObject> dic ){
      this.dic = dic;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return dic.get( index );
    }

    @Override
    public int getDicSize() throws IOException{
      return dic.size();
    }

  }

  @Test
  public void T_newInstance_1() throws IOException{
    ICellIndex index = new BufferDirectSequentialStringCellIndex( null , null );
  }

  @Test
  public void T_filter_1() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    IFilter filter = new PerfectMatchStringFilter( "abc" );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 20 );
    for( int i = 0,n=0 ; n < 100 ; i++,n+=5 ){
      assertEquals( result.get(i) , n );
    }
  }

  @Test
  public void T_filter_2() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    IFilter filter = new PartialMatchStringFilter( "b" );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 40 );
    for( int i = 0,n=0 ; n < 100 ; i+=2,n+=5 ){
      assertEquals( result.get(i) , n );
      assertEquals( result.get(i+1) , n+1 );
    }
  }

  @Test
  public void T_filter_3() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    IFilter filter = new ForwardMatchStringFilter( "bc" );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 20 );
    for( int i = 0,n=0 ; n < 100 ; i++,n+=5 ){
      assertEquals( result.get(i) , n+1 );
    }
  }

  @Test
  public void T_filter_4() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    IFilter filter = new BackwardMatchStringFilter( "bc" );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 20 );
    for( int i = 0,n=0 ; n < 100 ; i++,n+=5 ){
      assertEquals( result.get(i) , n );
    }
  }

  @Test
  public void T_filter_5() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    IFilter filter = new RegexpMatchStringFilter( "e.g" );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 20 );
    for( int i = 0,n=0 ; n < 100 ; i++,n+=5 ){
      assertEquals( result.get(i) , n+4 );
    }
  }

  @Test
  public void T_filter_6() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    boolean[] result = index.filter( new NullFilter( ColumnType.STRING ) , new boolean[100] );
    assertEquals( result , null );
  }

  @Test
  public void T_filter_7() throws IOException{
    List<PrimitiveObject> dic = new ArrayList<PrimitiveObject>();
    dic.add( new StringObj( "abc" ) );
    dic.add( new StringObj( "bcd" ) );
    dic.add( new StringObj( "cde" ) );
    dic.add( new StringObj( "def" ) );
    dic.add( new StringObj( "efg" ) );
    IntBuffer buffer = IntBuffer.allocate( 100 );
    for( int i = 0 ; i < 100 ; i++ ){
      buffer.put( i % 5 );
    }
    ICellIndex index = new BufferDirectSequentialStringCellIndex( new TestDicManager( dic ) , buffer );
    Set<String> filterDic = new HashSet<String>();
    filterDic.add( "abc" );
    filterDic.add( "bcd" );
    IFilter filter = new StringDictionaryFilter( filterDic );

    FilterdExpressionIndex result = new FilterdExpressionIndex( index.filter( filter , new boolean[100] ) );
    assertEquals( result.size() , 40 );
    for( int i = 0,n=0 ; n < 100 ; i+=2,n+=5 ){
      assertEquals( result.get(i) , n );
      assertEquals( result.get(i+1) , n+1 );
    }
  }

}
