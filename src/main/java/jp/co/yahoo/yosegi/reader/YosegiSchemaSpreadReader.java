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
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IndexFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class YosegiSchemaSpreadReader implements IStreamReader {

  private final ISettableIndexParser currentParser;
  private final IExpressionIndex currentIndexList;
  private int currentIndex;

  private YosegiSchemaSpreadReader(
      final Spread spread, Function<IExpressionNode,IExpressionNode> filter) throws IOException {
    SpreadColumn spreadColumn = new SpreadColumn("root");
    spreadColumn.setSpread(spread);

    currentIndex = 0;
    IExpressionNode node = new AndExpressionNode();
    currentIndexList = IndexFactory.toExpressionIndex(spread, filter.apply(node).exec(spread));
    currentParser = YosegiParserFactory.get(spreadColumn, currentIndexList.get(currentIndex));
  }

  /**
   * Set Spread to read and initialize.
   */
  public YosegiSchemaSpreadReader(final Spread spread) throws IOException {
    this(spread, node -> node);
  }

  /**
   * Set Spread to read and initialize.
   */
  public YosegiSchemaSpreadReader(
      final Spread spread, final IExpressionNode filterNode) throws IOException {
    this(spread, node -> Objects.nonNull(filterNode) ? filterNode : node);
  }

  @Override
  public boolean hasNext() throws IOException {
    return currentIndex != currentIndexList.size();
  }

  @Override
  public IParser next() throws IOException {
    if ( currentIndex == currentIndexList.size() ) {
      return null;
    }
    currentParser.setIndex( currentIndexList.get( currentIndex ) );
    currentIndex++;
    return currentParser;
  }

  @Override
  public void close() throws IOException {
    /* nothing to do */
  }
}

