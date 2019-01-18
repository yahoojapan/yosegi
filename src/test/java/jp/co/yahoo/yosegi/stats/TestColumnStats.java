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
package jp.co.yahoo.yosegi.stats;

import java.util.Map;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestColumnStats {

  private ColumnStats createChildStats( final String childName ){
    ColumnStats columnStats = new ColumnStats( childName );
    columnStats.addSummaryStats( ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    return columnStats;
  }

  private ColumnStats createMargeTestStats(){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    stats.addChild( "child" , createChildStats( "child" ) );
    return stats;
  }

  public static Stream<Arguments> data1(){
    return Stream.of(
      arguments( ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.INTEGER , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.UNION , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.ARRAY , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.SPREAD , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.BOOLEAN , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.BYTE , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.BYTES , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.DOUBLE , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.FLOAT , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.LONG , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.SHORT , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.NULL , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.EMPTY_ARRAY , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.EMPTY_SPREAD , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ),
      arguments( ColumnType.UNKNOWN , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) )
    );
  }

  @Test
  public void T_newInstance_1(){
    ColumnStats stats = new ColumnStats( null );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_addSummaryStats_1( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_mergeSummaryStats_1( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( columnType , summaryStats );
    stats.mergeSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_mergeSummaryStats_2( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.mergeSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @Test
  public void T_addChild_1(){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addChild( "child" , createChildStats( "child" ) );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    System.out.println( childStats.toString() );
  }

  @Test
  public void T_merge_1(){
    ColumnStats stats = createMargeTestStats();
    stats.merge( createMargeTestStats() );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    Map<ColumnType,SummaryStats> childStatsMap = childStats.getSummaryStats();
    result = childStatsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );
  }

  @Test
  public void T_merge_2(){
    ColumnStats stats = new ColumnStats( "new" );
    stats.merge( createMargeTestStats() );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    Map<ColumnType,SummaryStats> childStatsMap = childStats.getSummaryStats();
    result = childStatsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @Test
  public void T_doIntegration_1(){
    ColumnStats stats = createMargeTestStats();
    stats.merge( createMargeTestStats() );
    SummaryStats total = stats.doIntegration();

    assertEquals( total.getRowCount() , 40 );
    assertEquals( total.getRawDataSize() , 400 );
    assertEquals( total.getRealDataSize() , 200 );
  }

  @Test
  public void T_toJavaObject_1(){
    ColumnStats stats = createMargeTestStats();
    stats.merge( createMargeTestStats() );
    SummaryStats total = stats.doIntegration();
    Map<Object,Object> javaObj = stats.toJavaObject();
    assertEquals( javaObj.get( "name" ) , "root" );

    Map<Object,Object> totalMap = (Map<Object,Object>)( javaObj.get( "total" ) );
    assertEquals( totalMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( totalMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( totalMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    Map<Object,Object> integMap = (Map<Object,Object>)( javaObj.get( "integratoin_total" ) );
    assertEquals( integMap.get( "field_count" ) , Long.valueOf( 40 ) );
    assertEquals( integMap.get( "raw_data_size" ) , Long.valueOf( 400 ) );
    assertEquals( integMap.get( "real_data_size" ) , Long.valueOf( 200 ) );

    List<Object> columnTypeList = (List<Object>)( javaObj.get( "column_types" ) );
    Map<Object,Object> columnTypeMap = (Map<Object,Object>)( columnTypeList.get( 0 ) );
    assertEquals( columnTypeMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( columnTypeMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( columnTypeMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    Map<Object,Object> childContainerMap = (Map<Object,Object>)( javaObj.get( "child" ) );
    Map<Object,Object> childMap = (Map<Object,Object>)( childContainerMap.get( "child" ) );
    Map<Object,Object> childTotalMap = (Map<Object,Object>)( childMap.get( "total" ) );
    assertEquals( childTotalMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( childTotalMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( childTotalMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    System.out.println( stats.toString() );
    System.out.println( javaObj.toString() );
  }


}
