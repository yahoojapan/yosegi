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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestLazyColumn {

  private class TestColumnMnager implements IColumnManager{
    IColumn innerColumn;

    public TestColumnMnager() throws IOException{
      innerColumn = new PrimitiveColumn( ColumnType.STRING , "DUMMY" );
      innerColumn.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
      innerColumn.add( ColumnType.STRING , new StringObj( "b" ) , 1 );
      innerColumn.add( ColumnType.STRING , new StringObj( "c" ) , 2 );
    }

    @Override
    public IColumn get(){
      return innerColumn;
    }

    @Override
    public List<String> getColumnKeys(){
      return innerColumn.getColumnKeys();
    }

    @Override
    public int getColumnSize(){
      return innerColumn.getColumnSize();
    }
  }

  @Test
  public void T_newInstance_1()throws IOException{
    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    assertEquals( column.getColumnName() , "dummy" );
    assertEquals( column.getColumnType() , ColumnType.STRING );
  }

  @Test
  public void T_setParentsColumn()throws IOException{
    IColumn parentColumn = new PrimitiveColumn( ColumnType.INTEGER , "PARENT" );
    parentColumn.add( ColumnType.INTEGER , new IntegerObj( 1 ) , 0 );
    parentColumn.add( ColumnType.INTEGER , new IntegerObj( 2 ) , 1 );

    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    column.setParentsColumn( parentColumn );

    IColumn resultParentColumn = column.getParentsColumn();
    assertEquals( "PARENT" , resultParentColumn.getColumnName() );
    assertEquals( 2 , resultParentColumn.size() );
  }

  @Test
  public void T_add_1()throws IOException{
    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    column.add( ColumnType.STRING ,  new StringObj( "d" ) , 3 );
  }

  @Test
  public void T_addCell_1()throws IOException{
    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    column.addCell( ColumnType.STRING ,  new PrimitiveCell( ColumnType.STRING , null ) , 3 );
  }

  @Test
  public void T_setCellManager_1()throws IOException{
    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    column.setCellManager( null );
  }

  @Test
  public void T_get_1()throws IOException{
    LazyColumn column = new LazyColumn( "dummy" , ColumnType.STRING , new TestColumnMnager() );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "c" );
  }

}
