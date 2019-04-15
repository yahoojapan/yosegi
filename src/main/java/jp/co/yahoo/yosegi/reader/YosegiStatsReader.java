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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.stats.ColumnStats;
import jp.co.yahoo.yosegi.stats.SpreadSummaryStats;
import jp.co.yahoo.yosegi.stats.SummaryStats;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class YosegiStatsReader {

  private final List<SpreadSummaryStats> spreadSummaryStatsList =
      new ArrayList<SpreadSummaryStats>();
  private final List<ColumnStats> columnStatsList = new ArrayList<ColumnStats>();

  public void readStream(
      final InputStream in ,
      final long dataSize ,
      final Configuration config ) throws IOException {
    readStream( in , dataSize , config , 0 , dataSize );
  }

  /**
   * Read statistics from file.
   */
  public void readStream(
      final InputStream in ,
      final long dataSize ,
      final Configuration config ,
      final long start ,
      final long length ) throws IOException {
    spreadSummaryStatsList.clear();
    columnStatsList.clear();

    YosegiReader reader = new YosegiReader();
    reader.setNewStream( in , dataSize , config , start , length );
    while ( reader.hasNext() ) {
      List<ColumnBinary> columnBinaryList = reader.nextRaw();
      int lineCount = reader.getCurrentSpreadSize();
      SummaryStats stats = new SummaryStats();
      ColumnStats columnStats = new ColumnStats( "ROOT" );
      for ( ColumnBinary columnBinary : columnBinaryList ) {
        if ( Objects.nonNull(columnBinary) ) {
          stats.merge( columnBinary.toSummaryStats() );
          columnStats.addChild( columnBinary.columnName , columnBinary.toColumnStats() );
        }
      }
      spreadSummaryStatsList.add( new SpreadSummaryStats( lineCount , stats ) );
      columnStatsList.add( columnStats );
    }
    reader.close();
  }

  /**
   * Obtain integrated statistical information of all columns.
   */
  public SpreadSummaryStats getTotalSummaryStats() {
    SpreadSummaryStats stats = new SpreadSummaryStats();
    for ( SpreadSummaryStats childStats : spreadSummaryStatsList ) {
      stats.merge( childStats );
    }
    return stats;
  }

  public List<SpreadSummaryStats> getSpreadSummaryStatsList() {
    return spreadSummaryStatsList;
  }

  /**
   * Obtain integrated statistical information of all columns.
   */
  public ColumnStats getTotalColumn() {
    ColumnStats stats = new ColumnStats( "ROOT" );
    for ( ColumnStats childStats : columnStatsList ) {
      stats.merge( childStats );
    }
    return stats;
  }

  public List<ColumnStats> getColumnStatsList() {
    return columnStatsList;
  }

}
