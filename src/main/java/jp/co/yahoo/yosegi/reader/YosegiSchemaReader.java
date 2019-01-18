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

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.ISettableIndexParser;
import jp.co.yahoo.yosegi.message.parser.IStreamReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IndexFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class YosegiSchemaReader implements IStreamReader {

  private final YosegiReader currentReader = new YosegiReader();
  private final SpreadColumn spreadColumn = new SpreadColumn( "root" );

  private ISettableIndexParser currentParser;
  private IExpressionNode node = new AndExpressionNode();
  private IExpressionIndex currentIndexList;
  private Spread currentSpread;
  private int currentIndex;

  public void setNewStream(
      final InputStream in ,
      final long dataSize,
      final Configuration config ) throws IOException {
    currentReader.setNewStream( in , dataSize , config );
    nextReader();
  }

  public void setNewStream(
      final InputStream in ,
      final long dataSize,
      final Configuration config ,
      final long start ,
      final long length ) throws IOException {
    currentReader.setNewStream( in , dataSize , config , start , length );
    nextReader();
  }

  /**
   * Set filter conditions.
   */
  public void setExpressionNode( final IExpressionNode node ) {
    if ( node == null ) {
      this.node = new AndExpressionNode();
    } else {
      this.node = node;
    }
  }

  private boolean nextReader() throws IOException {
    if ( ! currentReader.hasNext() ) {
      currentSpread = null;
      currentIndex = 0;
      return false;
    }
    currentSpread = currentReader.next();
    if ( currentSpread.size() == 0 ) {
      return nextReader();
    }
    currentIndexList = IndexFactory.toExpressionIndex( currentSpread , node.exec( currentSpread ) );
    currentIndex = 0;
    if ( currentIndexList.size() == 0 ) {
      return nextReader();
    }

    spreadColumn.setSpread( currentSpread );
    currentParser = YosegiParserFactory.get( spreadColumn , currentIndexList.get( currentIndex ) );
    return true;
  }


  @Override
  public boolean hasNext() throws IOException {
    if ( currentSpread == null || currentIndex == currentIndexList.size() ) {
      if ( ! nextReader() ) {
        return false;
      }
    }
    return true;
  }

  @Override
  public IParser next() throws IOException {
    if ( currentSpread == null || currentIndex == currentIndexList.size() ) {
      if ( ! nextReader() ) {
        return null;
      }
    }
    currentParser.setIndex( currentIndexList.get( currentIndex ) );
    currentIndex++;
    return currentParser;
  }

  @Override
  public void close() throws IOException {
    currentReader.close();
  }

}
