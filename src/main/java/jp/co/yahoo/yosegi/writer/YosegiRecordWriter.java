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

package jp.co.yahoo.yosegi.writer;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.spread.Spread;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class YosegiRecordWriter implements AutoCloseable {

  public static final int DEFAULT_SPREAD_SIZE = 1024 * 1024 * 112;
  public static final int DEFAULT_MAX_RECORDS = 50000;
  public static final int DEFAULT_MIN_SIZE = 1024 * 1024 * 16;
  public static final int DEFAULT_MIN_RECORDS = 1000;

  private final YosegiWriter fileWriter;
  private int maxRows;
  private int currentDataSize;
  private int currentRows;
  private int spreadSize;
  private Spread currentSpread;

  public YosegiRecordWriter( final OutputStream out ) throws IOException {
    this( out , new Configuration() );
  }

  /**
   * Initialize by setting OutputStream.
   */
  public YosegiRecordWriter(
      final OutputStream out , final Configuration config ) throws IOException {
    fileWriter = new YosegiWriter( out , config );
    currentSpread = new Spread();
    spreadSize = config.getInt( "spread.size" , DEFAULT_SPREAD_SIZE );
    if ( spreadSize < DEFAULT_MIN_SIZE ) {
      spreadSize = DEFAULT_MIN_SIZE;
    }
    maxRows = config.getInt( "record.writer.max.rows" , DEFAULT_MAX_RECORDS );
    if ( maxRows < DEFAULT_MIN_RECORDS ) {
      maxRows = DEFAULT_MIN_RECORDS;
    }
  }

  /**
   * Add row data.
   */
  public void addRow( final Map<String,Object> row ) throws IOException {
    currentDataSize += currentSpread.addRow( row );
    currentRows++;
    flushSpread();
  }

  /**
   * Add row data.
   */
  public void addParserRow( final IParser parser )throws IOException {
    currentDataSize += currentSpread.addParserRow( parser );
    currentRows++;
    flushSpread();
  }

  private void flushSpread() throws IOException {
    if ( spreadSize < currentDataSize || maxRows <= currentRows ) {
      fileWriter.append( currentSpread );
      currentSpread = new Spread();
      currentDataSize = 0;
      currentRows = 0;
    }
  }

  /**
   * Close.
   */
  public void close() throws IOException {
    if ( currentSpread.size() != 0 ) {
      fileWriter.append( currentSpread );
    }
    fileWriter.close();
  }

}
