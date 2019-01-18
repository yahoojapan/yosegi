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

package jp.co.yahoo.yosegi.spread.expand;

import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.Spread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpandFunction implements IExpandFunction {

  private final List<String[]> columnNameArrayList;
  private final Map<String,String[]> linkColumnNameMap;
  private final ExpandNode expandNode;
  private final ExpandColumnLink expandColumnLink;

  /**
   * Initialize by setting the column to be expanded and the reference name of the expanded column.
   */
  public ExpandFunction(
      final ExpandNode expandNode ,
      final ExpandColumnLink expandColumnLink ,
      final List<String> columnNameList ,
      final List<String> linkColumnNameList ) {
    this.expandNode = expandNode;
    this.expandColumnLink = expandColumnLink;
    columnNameArrayList = new ArrayList<String[]>();

    linkColumnNameMap = new HashMap<String,String[]>();
    for ( int i = 0 ; i < columnNameList.size() ; i++ ) {
      String linkName = linkColumnNameList.get(i);
      if ( linkName != null ) {
        String[] columnNameArray = new String[i + 1];
        for ( int n = 0 ; n < i + 1 ; n++ ) {
          columnNameArray[n] = columnNameList.get(n);
        }
        linkColumnNameMap.put( linkName , columnNameArray );
        columnNameArrayList.add( columnNameArray );
      }
    }
  }

  @Override
  public Spread expand( final Spread spread ) throws IOException {
    ExpandSpread expandSpread = expandNode.get( spread );
    expandColumnLink.createLink( expandSpread );
    return expandSpread;
  }

  @Override
  public void expandIndexNode( final BlockIndexNode rootNode ) throws IOException {
    expandNode.setIndexNode( rootNode );
    expandColumnLink.createLinkIndexNode( rootNode );
  }

  @Override
  public String[] getExpandLinkColumnName( final String linkName ) {
    if ( linkColumnNameMap.containsKey( linkName ) ) {
      return linkColumnNameMap.get( linkName );
    }
    return expandColumnLink.getNeedColumnName( linkName );
  }

  @Override
  public List<String[]> getExpandColumnName() {
    List<String[]> needColumnNameList = new ArrayList<String[]>();
    needColumnNameList.addAll( expandColumnLink.getNeedColumnNameList() );
    needColumnNameList.addAll( columnNameArrayList );
    return needColumnNameList;
  }

}
