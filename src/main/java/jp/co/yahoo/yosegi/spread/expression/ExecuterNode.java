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

package jp.co.yahoo.yosegi.spread.expression;

import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.io.IOException;
import java.util.List;

public class ExecuterNode implements IExpressionNode {

  private final IExtractNode columnExtractNode;
  private final IFilter filter;

  /**
   * Initialize object to access column and set filter.
   */
  public ExecuterNode( final IExtractNode columnExtractNode , final IFilter filter ) {
    if ( columnExtractNode == null ) {
      throw new IllegalArgumentException( "column extract node is null." );
    }
    if ( filter == null ) {
      throw new IllegalArgumentException( "filter is null." );
    }
    this.columnExtractNode = columnExtractNode;
    this.filter = filter;
  }

  @Override
  public void addChildNode( final IExpressionNode node ) {
    throw new UnsupportedOperationException( "Executer node can not have child node." );
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final BlockIndexNode indexNode ) throws IOException {
    BlockIndexNode currentNode = columnExtractNode.get( indexNode );
    return currentNode.getBlockIndex().getBlockSpreadIndex( filter );
  }

}
