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
import jp.co.yahoo.yosegi.binary.ColumnBinaryUtil;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;
import java.util.List;

public class LinkColumn {

  private final String baseColumnName;
  private final String linkName;
  private final String[] nodeNameArray;

  /**
   * Set the column reference information and initialize it.
   */
  public LinkColumn(
      final String baseColumnName , final String linkName , final String[] nodeNameArray ) {
    this.baseColumnName = baseColumnName;
    this.linkName = linkName;
    this.nodeNameArray = nodeNameArray;
  }

  public String getLinkName() {
    return linkName;
  }

  public String[] getNeedColumnNameArray() {
    return nodeNameArray;
  }

  /**
   * Create column reference information.
   */
  public void createLink( final ExpandSpread expandSpread ) {
    IColumn linkTargetColumn = null;
    for ( String nodeName : nodeNameArray ) {
      if ( linkTargetColumn == null ) {
        linkTargetColumn = expandSpread.getColumn( nodeName );
      } else {
        if ( linkTargetColumn.getColumnType() == ColumnType.UNION ) {
          linkTargetColumn = linkTargetColumn.getColumn( ColumnType.SPREAD );
        }
        linkTargetColumn = linkTargetColumn.getColumn( nodeName );
      }
    }
    if ( linkTargetColumn.getColumnType() != ColumnType.ARRAY ) {
      return;
    }

    IColumn column = expandSpread.getColumn( baseColumnName );
    if ( column instanceof ExpandColumn ) {
      ExpandColumn expandColumn = (ExpandColumn)column;
      int[] indexArray = expandColumn.getColumnIndexArray();
      expandSpread.addExpandColumn( linkName , linkTargetColumn.getColumn(0) , indexArray );
    } else {
      expandSpread.addExpandLeafColumn( linkName , linkTargetColumn.getColumn(0) );
    }
  }

  /**
   * Create column reference information.
   */
  public void createLinkFromColumnBinary(
      final List<ColumnBinary> columnBinaryList ,
      final List<ColumnBinary> linkBinaryList ) throws IOException {
    ColumnBinary baseColumn =
        ColumnBinaryUtil.getFromColumnName( baseColumnName , linkBinaryList );
    if ( baseColumn == null ) {
      return;
    }
    List<ColumnBinary> currentBinaryList = null;
    ColumnBinary result = null;
    for ( String nodeName : nodeNameArray ) {
      if ( result == null ) {
        currentBinaryList = columnBinaryList;
        result = ColumnBinaryUtil.getFromColumnName( nodeName , currentBinaryList );
      } else {
        currentBinaryList = result.columnBinaryList;
        if ( result.columnType == ColumnType.UNION ) {
          result = ColumnBinaryUtil.getFromColumnType( ColumnType.SPREAD , currentBinaryList );
          if ( result == null ) {
            break;
          }
          currentBinaryList = result.columnBinaryList;
        }
        result = ColumnBinaryUtil.getFromColumnName( nodeName , currentBinaryList );
      }
      if ( result == null ) {
        break;
      }
    }
    if ( result == null || result.columnType != ColumnType.ARRAY ) {
      return;
    }
    ColumnBinary newColumnBinary =
        result.columnBinaryList.get(0).createRenameColumnBinary( linkName );
    ColumnBinaryUtil.removeFromColumnName(
        nodeNameArray[nodeNameArray.length - 1] , currentBinaryList );

    if ( baseColumn.loadIndex != null ) {
      newColumnBinary.setLoadIndex( baseColumn.loadIndex );
      ColumnBinaryUtil.setLoadIndex( newColumnBinary.columnBinaryList , baseColumn.loadIndex );
    }
    linkBinaryList.add( newColumnBinary );
  }

  /**
   * Refer to the column before expansion in BlockIndex.
   */
  public void createLinkIndexNode( final BlockIndexNode rootNode ) {
    BlockIndexNode currentNode = rootNode;
    for ( String nodeName : nodeNameArray ) {
      currentNode = currentNode.getChildNode( nodeName );
    }
    rootNode.putChildNode( linkName , currentNode );
  }

}
