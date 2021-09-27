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
import jp.co.yahoo.yosegi.binary.RepetitionAndLoadSize;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExpandNode {

  private final String columnName;
  private String linkColumnName;
  private boolean isArray;
  private ExpandNode childNode;

  /**
   * Initialize by setting column name and expanded column name.
   */
  public ExpandNode(
      final List<String> columnNameList ,
      final List<String> linkColumnNameList ) throws IOException {
    if ( columnNameList.isEmpty() ) {
      throw new IOException( "Expand node is empty." );
    }
    columnName = columnNameList.get(0);
    if ( linkColumnNameList.get(0) != null ) {
      setArrayNode( linkColumnNameList.get(0) );
    }
    if ( 1 < columnNameList.size() ) {
      childNode = new ExpandNode( columnNameList , linkColumnNameList , 1 );
    } else if ( ! isArray ) {
      throw new IOException( "End node is array only." );
    }
  }

  /**
   * Initialize by setting the column to be expanded and the reference name of the expanded column.
   */
  public ExpandNode(
      final List<String> columnNameList ,
      final List<String> linkColumnNameList ,
      final int depth ) throws IOException {
    columnName = columnNameList.get( depth );
    if ( linkColumnNameList.get(depth) != null ) {
      setArrayNode( linkColumnNameList.get(depth) );
    }
    int nextDepth = depth + 1;
    if ( nextDepth < columnNameList.size() ) {
      childNode = new ExpandNode( columnNameList , linkColumnNameList , nextDepth );
    } else if ( ! isArray ) {
      throw new IOException( "End node is array only." );
    }
  }

  public String getNodeName() {
    return columnName;
  }

  public void setArrayNode( final String linkColumnName ) {
    this.linkColumnName = linkColumnName;
    isArray = true;
  }

  public boolean isEndNode() {
    return childNode == null;
  }

  public boolean isArrayNode() {
    return isArray;
  }

  public void setIndexNode( final BlockIndexNode rootNode ) throws IOException {
    setIndexNode( rootNode , rootNode );
  }

  /**
   * Set BlockIndexNode.
   */
  public void setIndexNode( final BlockIndexNode rootNode , final BlockIndexNode parentNode ) {
    BlockIndexNode currentNode = parentNode.getChildNode( columnName );
    if ( childNode != null ) {
      childNode.setIndexNode( rootNode , currentNode );
    }
    if ( isArray ) {
      rootNode.putChildNode( linkColumnName , currentNode );
    }
  }

  private ColumnBinary getChildAndRemoveIfChildIsArray(
        final ExpandNode node , final List<ColumnBinary> binaryList ) {
    ColumnBinary result = null;
    if ( binaryList == null ) {
      return result;
    }
    for ( int i = 0 ; i < binaryList.size() ; i++ ) {
      ColumnBinary child = binaryList.get(i);
      if ( node.getNodeName().equals( child.columnName ) ) {
        result = child;
        if ( node.isArrayNode() ) {
          binaryList.remove(i);
        }
        break;
      }
    }
    return result;
  }

  /**
   * Set Column in Array expanded to Spread and Create index of parent Array.
   * The structure of the entered list is changed.
   */
  public int createExpandColumnBinary(
      final List<ColumnBinary> root , ExpandColumnLink expandColumnLink ) throws IOException {
    ColumnBinary current = getChildAndRemoveIfChildIsArray( this , root );
    if ( current == null ) {
      ColumnBinaryUtil.setLoadIndex( root , new int[0] , new int[0] , 0 );
      return 0;
    }
    List<ColumnBinary> linkColumnList = new ArrayList<ColumnBinary>();
    RepetitionAndLoadSize rootRepetitionAndLoadSize = setExpandIndex( linkColumnList , current );
    int[] rootIndex = rootRepetitionAndLoadSize.getLoadIndex();
    int[] rootRepetitions = rootRepetitionAndLoadSize.getRepetitions();
    int rootLoadSize = rootRepetitionAndLoadSize.getLoadSize();
    expandColumnLink.createLinkFromColumnBinary( root , linkColumnList );
    ColumnBinaryUtil.setLoadIndex( root , rootIndex , rootRepetitions , rootLoadSize );
    root.addAll( linkColumnList );
    return rootLoadSize;
  }

  private RepetitionAndLoadSize setExpandIndex(
      final List<ColumnBinary> linkColumnList ,
      final ColumnBinary currentColumnBinary ) throws IOException {
    if ( childNode == null ) {
      ColumnBinary arrayTarget = null;
      if ( currentColumnBinary.columnType == ColumnType.UNION ) {
        for ( ColumnBinary child : currentColumnBinary.columnBinaryList ) {
          if ( child.columnType == ColumnType.ARRAY ) {
            arrayTarget = child;
            break;
          }
        }
      } else {
        arrayTarget = currentColumnBinary;
      }
      if ( arrayTarget == null || arrayTarget.columnType != ColumnType.ARRAY ) {
        return new RepetitionAndLoadSize( new int[0] , new int[0] , 0 );
      }
      IColumn arrayColumnTarget = ColumnBinaryUtil.createArrayIndexColumn( arrayTarget );
      int innerColumnLength = 0;
      int[] parentRepetitions = new int[arrayColumnTarget.size()];
      for ( int i = 0; i < arrayColumnTarget.size() ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        innerColumnLength += arrayCell.getEnd() - arrayCell.getStart();
        parentRepetitions[i] = arrayCell.getEnd() - arrayCell.getStart();
      }

      int[] parentIndexArray = new int[innerColumnLength];
      for ( int i = 0; i < arrayColumnTarget.size() ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() && childIndex < innerColumnLength ; childIndex++ ) {
          parentIndexArray[childIndex] = i;
        }
      }
      ColumnBinary arrayInnerColumnBinary = arrayTarget.columnBinaryList.get(0);
      ColumnBinary linkColumnBinary =
          arrayInnerColumnBinary.createRenameColumnBinary( linkColumnName );
      linkColumnList.add( linkColumnBinary );

      return new RepetitionAndLoadSize( parentIndexArray , parentRepetitions , innerColumnLength );
    } else if ( isArray ) {
      ColumnBinary arrayTarget = null;
      if ( currentColumnBinary.columnType == ColumnType.UNION ) {
        for ( ColumnBinary child : currentColumnBinary.columnBinaryList ) {
          if ( child.columnType == ColumnType.ARRAY ) {
            arrayTarget = child;
            break;
          }
        }
      } else {
        arrayTarget = currentColumnBinary;
      }
      if ( arrayTarget == null || arrayTarget.columnType != ColumnType.ARRAY ) {
        return new RepetitionAndLoadSize( new int[0] , new int[0] , 0 );
      }
      ColumnBinary arrayInnerColumnBinary = arrayTarget.columnBinaryList.get(0);
      ColumnBinary childColumnBinary =
          getChildAndRemoveIfChildIsArray( childNode , arrayInnerColumnBinary.columnBinaryList );
      if ( childColumnBinary == null
          || ( childColumnBinary.columnType == ColumnType.ARRAY && ! childNode.isArrayNode() ) ) {
        ColumnBinary linkColumnBinary =
            arrayInnerColumnBinary.createRenameColumnBinary( linkColumnName );
        linkColumnList.add( linkColumnBinary );
        ColumnBinaryUtil.setLoadIndex(
            linkColumnBinary.columnBinaryList , new int[0]  , new int[0] , 0 );
        linkColumnBinary.setLoadIndex( new int[0] );
        linkColumnBinary.setRepetitions( new int[0] , 0 );
        return new RepetitionAndLoadSize( new int[0] , new int[0] , 0 );
      }
      RepetitionAndLoadSize childRepetitionAndIndex =
          childNode.setExpandIndex( linkColumnList , childColumnBinary );
      int[] childIndexArray = childRepetitionAndIndex.getLoadIndex();
      int[] childRepetitions = childRepetitionAndIndex.getRepetitions();
      int childLoadSize = childRepetitionAndIndex.getLoadSize();

      IColumn arrayColumnTarget = ColumnBinaryUtil.createArrayIndexColumn( arrayTarget );
      IColumn arrayColumn = arrayColumnTarget.getColumn(0);
      int[] parentRepetitions = new int[arrayColumnTarget.size()];
      int[] parentIndexArray = new int[childIndexArray.length];
      int parentCount = 0;
      for ( int i = 0 ; i < arrayColumnTarget.size() ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() ; childIndex++ ) {
          if ( childIndex < childRepetitions.length ) {
            parentRepetitions[i] += childRepetitions[childIndex];
          }
          
          while ( parentCount < childIndexArray.length
              && childIndexArray[parentCount] == childIndex ) {
            parentIndexArray[parentCount] = i;
            parentCount++;
          }
        }
      }

      ColumnBinary linkColumnBinary =
          arrayInnerColumnBinary.createRenameColumnBinary( linkColumnName );
      ColumnBinaryUtil.setLoadIndex(
          linkColumnBinary.columnBinaryList ,
          childIndexArray ,
          childRepetitions ,
          childLoadSize );
      linkColumnBinary.setLoadIndex( childIndexArray );
      linkColumnBinary.setRepetitions( childRepetitions , childLoadSize );
      linkColumnList.add( linkColumnBinary );

      return new RepetitionAndLoadSize( parentIndexArray , parentRepetitions , childLoadSize );
    } else {
      ColumnBinary current = currentColumnBinary;
      if ( current != null && current.columnType == ColumnType.UNION ) {
        ColumnBinary newCurrent = null;
        ColumnType findTarget = ColumnType.SPREAD;
        for ( ColumnBinary unionChild : current.columnBinaryList ) {
          if ( unionChild.columnType == findTarget ) {
            newCurrent = unionChild;
            break;
          }
        }
        current = newCurrent;
      }
      ColumnBinary child =
          getChildAndRemoveIfChildIsArray( childNode , current.columnBinaryList );
      return childNode.setExpandIndex( linkColumnList , child );
    }
  }
}
