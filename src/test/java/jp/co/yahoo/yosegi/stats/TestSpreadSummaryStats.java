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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestSpreadSummaryStats {

  @Test
  public void T_newInstance_1(){
    SpreadSummaryStats stats = new SpreadSummaryStats();
    assertEquals( 0 , stats.getLineCount() );

    SummaryStats summary = stats.getSummaryStats();
    assertEquals( 0 , summary.getRowCount() );
    assertEquals( 0 , summary.getRawDataSize() );
    assertEquals( 0 , summary.getRealDataSize() );
  }

  @Test
  public void T_newInstance_2(){
    SpreadSummaryStats stats = new SpreadSummaryStats( 5 , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    assertEquals( 5 , stats.getLineCount() );

    SummaryStats summary = stats.getSummaryStats();
    assertEquals( 10 , summary.getRowCount() );
    assertEquals( 100 , summary.getRawDataSize() );
    assertEquals( 50 , summary.getRealDataSize() );

    System.out.println( stats.toString() );
  }

  @Test
  public void T_merge_1(){
    SpreadSummaryStats stats = new SpreadSummaryStats( 5 , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    stats.merge( new SpreadSummaryStats( 5 , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ) );
    assertEquals( 10 , stats.getLineCount() );

    SummaryStats summary = stats.getSummaryStats();
    assertEquals( 20 , summary.getRowCount() );
    assertEquals( 200 , summary.getRawDataSize() );
    assertEquals( 100 , summary.getRealDataSize() );
  }

  @Test
  public void T_merge_2(){
    SpreadSummaryStats stats = new SpreadSummaryStats();
    stats.merge( new SpreadSummaryStats( 5 , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) ) );
    assertEquals( 5 , stats.getLineCount() );

    SummaryStats summary = stats.getSummaryStats();
    assertEquals( 10 , summary.getRowCount() );
    assertEquals( 100 , summary.getRawDataSize() );
    assertEquals( 50 , summary.getRealDataSize() );
  }

  @Test
  public void T_average_1(){
    SpreadSummaryStats stats = new SpreadSummaryStats( 5 , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    assertEquals( 5 , stats.getLineCount() );

    SummaryStats summary = stats.getSummaryStats();
    assertEquals( 10 , summary.getRowCount() );
    assertEquals( 100 , summary.getRawDataSize() );
    assertEquals( 50 , summary.getRealDataSize() );

    assertEquals( (double)20 , stats.getAverageRecordSize() );
    assertEquals( (double)10 , stats.getAverageRecordRealSize() );
    assertEquals( (double)2 , stats.getAverageRecordPerField() );
  }

}
