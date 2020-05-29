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

package jp.co.yahoo.yosegi.spread.flatten;

import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlattenFunction implements IFlattenFunction {

  private final List<FlattenColumn> flattenColumnList = new ArrayList<FlattenColumn>();
  private final Map<String,FlattenColumn> flattenColumnMap = new HashMap<String,FlattenColumn>();
  private final Map<String,Integer> duplicateKeyMap = new HashMap<String,Integer>();
  private final List<String> filterColumnList = new ArrayList<String>();
  private String delimiter = "_";

  public void setDelimiter( final String delimiter ) {
    this.delimiter = delimiter;
  }

  public boolean isEmpty() {
    return flattenColumnList.isEmpty();
  }

  public int size() {
    return flattenColumnList.size();
  }

  /**
   * Add an object with flattening information.
   */
  public void add( final FlattenColumn flattenColumn ) {
    if ( ! flattenColumnMap.containsKey( flattenColumn.getLinkName() ) ) {
      flattenColumnList.add( flattenColumn );
      flattenColumnMap.put( flattenColumn.getLinkName() , flattenColumn );
    } else {
      if ( ! duplicateKeyMap.containsKey( flattenColumn.getLinkName() ) ) {
        duplicateKeyMap.put( flattenColumn.getLinkName() , Integer.valueOf( 0 ) );
      }
      int num = duplicateKeyMap.get( flattenColumn.getLinkName() ).intValue();
      String newLinkKey = String.format(
          "%s%s%d" ,
          flattenColumn.getLinkName() ,
          delimiter ,
          num );
      duplicateKeyMap.put( flattenColumn.getLinkName() , Integer.valueOf( num + 1 ) );
      if ( flattenColumnMap.containsKey( newLinkKey ) ) {
        add( flattenColumn );
      } else {
        FlattenColumn newFlattenColumn = new FlattenColumn(
            newLinkKey , flattenColumn.getFilterColumnNameArray() );
        flattenColumnList.add( newFlattenColumn );
        flattenColumnMap.put( newLinkKey , newFlattenColumn );
      }
    }
  }

  private Spread allRead( final Spread spread ) {
    Spread newSpread = new Spread();
    for ( FlattenColumn flattenColumn : flattenColumnList ) {
      IColumn column = flattenColumn.getColumn( spread );
      newSpread.addColumn( column );
    }
    newSpread.setRowCount( spread.size() );

    return newSpread;
  }

  private Spread filterRead( final Spread spread ) {
    Spread newSpread = new Spread();
    for ( String linkName : filterColumnList ) {
      IColumn column = flattenColumnMap.get( linkName ).getColumn( spread );
      newSpread.addColumn( column );
    }
    newSpread.setRowCount( spread.size() );

    return newSpread;
  }

  @Override
  public boolean isFlatten() {
    return true;
  }

  @Override
  public Spread flatten( final Spread spread ) {
    if ( filterColumnList.isEmpty() ) {
      return allRead( spread );
    } else {
      return filterRead( spread );
    }
  }

  @Override
  public void flattenIndexNode( final BlockIndexNode rootNode ) {
    if ( filterColumnList.isEmpty() ) {
      for ( FlattenColumn flattenColumn : flattenColumnList ) {
        flattenColumn.flattenIndexNode( rootNode );
      }
    } else {
      for ( String linkName : filterColumnList ) {
        flattenColumnMap.get( linkName ).flattenIndexNode( rootNode );
      }
    }
  }

  @Override
  public String[] getFlattenColumnName( final String linkColumnName ) {
    if ( flattenColumnMap.containsKey( linkColumnName ) ) {
      filterColumnList.add( linkColumnName );
      return flattenColumnMap.get( linkColumnName).getFilterColumnNameArray();
    }
    return new String[0];
  }

}
