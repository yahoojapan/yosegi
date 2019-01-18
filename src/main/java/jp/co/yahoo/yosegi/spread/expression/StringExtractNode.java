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

public class StringExtractNode implements IExtractNode {

  private final String columnName;
  private IExtractNode childColumnNode;

  public StringExtractNode( final String columnName ) {
    this.columnName = columnName;
    childColumnNode = null;
  }

  public StringExtractNode( final String columnName , final IExtractNode childColumnNode ) {
    this.columnName = columnName;
    this.childColumnNode = childColumnNode;
  }

  @Override
  public IColumn get(final IColumn column ) {
    IColumn currentColumn = column.getColumn( columnName );
    if ( childColumnNode != null ) {
      return childColumnNode.get( currentColumn );
    }
    return currentColumn;
  }

  @Override
  public IColumn get( final Spread spread ) {
    IColumn currentColumn = spread.getColumn( columnName );
    if ( childColumnNode != null ) {
      return childColumnNode.get( currentColumn );
    }
    return currentColumn;
  }

  @Override
  public BlockIndexNode get( final BlockIndexNode indexNode ) {
    BlockIndexNode currentIndexNode = indexNode.getChildNode( columnName );
    if ( childColumnNode != null ) {
      return childColumnNode.get( currentIndexNode );
    }
    return currentIndexNode;
  }

  @Override
  public void pushChild( final IExtractNode childColumnNode ) {
    if ( this.childColumnNode == null ) {
      this.childColumnNode = childColumnNode;
    } else {
      this.childColumnNode.pushChild( childColumnNode );
    }
  }

}
