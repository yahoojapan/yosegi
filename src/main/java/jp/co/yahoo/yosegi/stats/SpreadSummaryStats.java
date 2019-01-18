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

public class SpreadSummaryStats {

  private final SummaryStats summaryStats;

  private long lineCount;

  public SpreadSummaryStats() {
    summaryStats = new SummaryStats();
  }

  public SpreadSummaryStats( final long lineCount , final SummaryStats summaryStats ) {
    this.lineCount = lineCount;
    this.summaryStats = summaryStats;
  }

  public void merge( final SpreadSummaryStats spreadStats ) {
    this.lineCount += spreadStats.getLineCount();
    this.summaryStats.merge( spreadStats.getSummaryStats() );
  }

  public long getLineCount() {
    return lineCount;
  }

  public SummaryStats getSummaryStats() {
    return summaryStats;
  }

  public double getAverageRecordSize() {
    return (double)summaryStats.getRawDataSize() / (double)lineCount;
  }

  public double getAverageRecordRealSize() {
    return (double)summaryStats.getRealDataSize() / (double)lineCount;
  }

  public double getAverageRecordPerField() {
    return (double)summaryStats.getRowCount() / (double)lineCount;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append( String.format( "Line count=%d , " , lineCount ) );
    result.append( String.format( "Average record size=%f , " , getAverageRecordSize() ) );
    result.append( String.format( "Average record real size=%f , " , getAverageRecordRealSize() ) );
    result.append( String.format( "Average record per field=%f , " , getAverageRecordPerField() ) );
    result.append( String.format( "%s" ,summaryStats.toString() ) );
    return result.toString();
  }

}
