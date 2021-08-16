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

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.ISettableIndexParser;
import jp.co.yahoo.yosegi.message.parser.IStreamReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import java.io.IOException;
import java.util.List;

public class YosegiSchemaSpreadReader implements IStreamReader {

  private final ISettableIndexParser currentParser;
  private int currentIndex;
  private int currentSpreadSize;

  /**
   * Set Spread to read and initialize.
   */
  public YosegiSchemaSpreadReader( final Spread spread ) throws IOException {
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );

    currentIndex = 0;
    currentSpreadSize = spread.size();
    currentParser = YosegiParserFactory.get( spreadColumn , currentIndex );
  }

  @Override
  public boolean hasNext() throws IOException {
    return currentIndex != currentSpreadSize;
  }

  @Override
  public IParser next() throws IOException {
    if ( currentIndex == currentSpreadSize ) {
      return null;
    }
    currentParser.setIndex( currentIndex );
    currentIndex++;
    return currentParser;
  }

  @Override
  public void close() throws IOException {
  }

}
