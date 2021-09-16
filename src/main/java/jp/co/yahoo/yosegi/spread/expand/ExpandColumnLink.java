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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpandColumnLink {

  private final List<LinkColumn> linkColumnList = new ArrayList<LinkColumn>();
  private final Map<String,LinkColumn> linkColumnMap = new HashMap<String,LinkColumn>();

  /**
   * Add column reference information.
   */
  public void addLinkColumn( final LinkColumn linkColumn ) throws IOException {
    linkColumnList.add( linkColumn );
    if ( linkColumnMap.containsKey( linkColumn.getLinkName() ) ) {
      throw new IOException( "Expand link name is already exists. " + linkColumn.getLinkName() );
    }
    linkColumnMap.put( linkColumn.getLinkName() , linkColumn );
  }

  /**
   * Create column reference information.
   */
  public void createLink( final ExpandSpread expandSpread ) {
    for ( LinkColumn linkColumn : linkColumnList ) {
      linkColumn.createLink( expandSpread );
    }
  }

  /**
   * Create column reference information.
   */
  public void createLinkFromColumnBinary(
      final List<ColumnBinary> columnBinaryList ,
      final List<ColumnBinary> linkColumnBinaryList ) throws IOException {
    for ( LinkColumn linkColumn : linkColumnList ) {
      linkColumn.createLinkFromColumnBinary( columnBinaryList , linkColumnBinaryList );
    }
  }

  /**
   * Create column reference information.
   */
  public void createLinkIndexNode( final BlockIndexNode rootNode ) {
    for ( LinkColumn linkColumn : linkColumnList ) {
      linkColumn.createLinkIndexNode( rootNode );
    }
  }

  /**
   * Obtain column name before expansion from column name of expanded Spread.
   */
  public String[] getNeedColumnName( final String linkName ) {
    LinkColumn linkColumn = linkColumnMap.get( linkName );
    if ( linkColumn == null ) {
      return new String[0];
    }

    return linkColumn.getNeedColumnNameArray();
  }

  /**
   * Obtain column name before expansion from column name of expanded Spread.
   */
  public List<String[]> getNeedColumnNameList() {
    List<String[]> needColumnNameList = new ArrayList<String[]>();
    for ( LinkColumn linkColumn : linkColumnList ) {
      needColumnNameList.add( linkColumn.getNeedColumnNameArray() );
    }
    return needColumnNameList;
  }

}
