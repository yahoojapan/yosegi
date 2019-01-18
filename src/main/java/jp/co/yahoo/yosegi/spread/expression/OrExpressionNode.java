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
import jp.co.yahoo.yosegi.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrExpressionNode implements IExpressionNode {

  private final List<IExpressionNode> childNode = new ArrayList<IExpressionNode>();

  @Override
  public void addChildNode( final IExpressionNode node ) {
    if ( node != null ) {
      childNode.add( node );
    }
  }

  @Override
  public boolean[] exec( final Spread spread ) throws IOException {
    boolean[] union = null;
    for ( IExpressionNode node : childNode ) {
      boolean[] result = node.exec( spread );
      if ( result == null ) {
        return null;
      }
      if ( union == null ) {
        union = result;
      } else {
        boolean isAll = true;
        for ( int i = 0 ; i < union.length ; i++ ) {
          union[i] = union[i] || result[i];
          if ( ! union[i] ) {
            isAll = false;
          }
        }
        if ( isAll ) {
          return union;
        }
      }
    }

    return union;
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final BlockIndexNode indexNode ) throws IOException {
    if ( childNode.isEmpty() ) {
      return null;
    }
    List<Integer> currentNode = null;
    for ( IExpressionNode node : childNode ) {
      List<Integer> childResult = node.getBlockSpreadIndex( indexNode );
      if ( childResult == null ) {
        return null;
      }

      if ( currentNode == null ) {
        currentNode = childResult;
      } else {
        currentNode = CollectionUtils.unionFromSortedCollection( currentNode , childResult );
      }
    }

    return currentNode;
  }

}
