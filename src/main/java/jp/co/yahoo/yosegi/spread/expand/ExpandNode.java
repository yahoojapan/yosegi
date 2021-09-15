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
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.NullColumn;

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

  /**
   * Get ExpandSpread from Spread.
   */
  public ExpandSpread get( final Spread spread ) throws IOException {
    ExpandSpread expandSpread = new ExpandSpread();
    int[] rootIndexArray = set( spread , expandSpread );
    expandSpread.setOriginalSpread( spread , rootIndexArray );

    return expandSpread;
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

  public int[] set( final Spread original , final ExpandSpread expandSpread ) throws IOException {
    IColumn currentColumn = original.getColumn( columnName );
    return setExpandColumn( currentColumn , expandSpread );
  }

  public int[] set( final IColumn original , final ExpandSpread expandSpread )throws IOException {
    IColumn currentColumn = original.getColumn( columnName );
    return setExpandColumn( currentColumn , expandSpread );
  }

  /**
   * Set Column in Array expanded to Spread and Create index of parent Array.
   */
  public int[] setExpandColumn(
      final IColumn original , final ExpandSpread expandSpread ) throws IOException {
    if ( childNode == null ) {
      IColumn arrayColumnTarget = original;
      if ( original.getColumnType() == ColumnType.UNION ) {
        arrayColumnTarget = original.getColumn( ColumnType.ARRAY );
      }
      if ( arrayColumnTarget.getColumnType() != ColumnType.ARRAY ) {
        expandSpread.addExpandColumn( linkColumnName , NullColumn.getInstance() , new int[0] );
        return new int[0];
      }
      IColumn innerColumn = arrayColumnTarget.getColumn(0);
      int[] parentIndexArray = new int[innerColumn.size()];
      int loopCount = arrayColumnTarget.size();
      for ( int i = 0; i < loopCount ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() && childIndex < innerColumn.size() ; childIndex++ ) {
          parentIndexArray[childIndex] = i;
        }
      }

      expandSpread.addExpandLeafColumn( linkColumnName , innerColumn );

      return parentIndexArray;
    } else if ( isArray ) {
      IColumn arrayColumnTarget = original;
      if ( original.getColumnType() == ColumnType.UNION ) {
        arrayColumnTarget = original.getColumn( ColumnType.ARRAY );
      }
      if ( arrayColumnTarget.getColumnType() != ColumnType.ARRAY ) {
        expandSpread.addExpandColumn( linkColumnName , NullColumn.getInstance() , new int[0] );
        return new int[0];
      }
      IColumn arrayColumn = arrayColumnTarget.getColumn(0);

      int[] childIndexArray = childNode.set( arrayColumn , expandSpread );
      int[] parentIndexArray = new int[childIndexArray.length];
      int parentCount = 0;
      int loopCount = arrayColumnTarget.size();
      for ( int i = 0 ; i < loopCount ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() ; childIndex++ ) {
          while ( parentCount < childIndexArray.length  
              && childIndexArray[parentCount] == childIndex ) {
            parentIndexArray[parentCount] = i;
            parentCount++;
          }
        }
      }

      expandSpread.addExpandColumn( linkColumnName , arrayColumn , childIndexArray );

      return parentIndexArray;
    } else {
      if ( original.getColumnType() == ColumnType.UNION ) {
        return childNode.set( original.getColumn( ColumnType.SPREAD ) , expandSpread );
      } else {
        return childNode.set( original , expandSpread );
      }
    }
  }

  private IColumn toColumn( final ColumnBinary binary ) throws IOException {
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( binary.makerClassName );
    return maker.toColumn( binary );
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

  private ColumnBinary createLinkColumnBinary( final String linkName , final ColumnBinary binary ) {
    return new ColumnBinary(
        binary.makerClassName,
        binary.compressorClassName,
        linkName,
        binary.columnType,
        binary.rowCount,
        binary.rawDataSize,
        binary.logicalDataSize,
        binary.cardinality,
        binary.binary,
        binary.binaryStart,
        binary.binaryLength,
        binary.columnBinaryList );
  }

  private int[] createArrayChildIndex(
      final ColumnBinary arrayColumn , final int[] arrayLoadIndex ) throws IOException {
    if ( arrayLoadIndex.length == 0 ) {
      return new int[0];
    }
    IColumn arrayColumnTarget = toColumn( arrayColumn );
    int childLoadIndexLength = 0;
    int currentIndex = -1;
    for ( int i = 0; i < arrayLoadIndex.length ; i++ ) {
      if ( currentIndex == arrayLoadIndex[i] ) {
        continue;
      }
      currentIndex = arrayLoadIndex[i];
      ICell cell = arrayColumnTarget.get( arrayLoadIndex[i] );
      if ( cell.getType() != ColumnType.ARRAY ) {
        continue;
      }
      ArrayCell arrayCell = (ArrayCell)( cell );
      childLoadIndexLength += arrayCell.getEnd() - arrayCell.getStart();
    }
    int[] childLoadIndex = new int[childLoadIndexLength];
    int currentArrayIndex = 0;
    currentIndex = -1;
    for ( int i = 0; i < arrayLoadIndex.length ; i++ ) {
      if ( currentIndex == arrayLoadIndex[i] ) {
        continue;
      }
      currentIndex = arrayLoadIndex[i];
      ICell cell = arrayColumnTarget.get( arrayLoadIndex[i] );
      if ( cell.getType() != ColumnType.ARRAY ) {
        continue;
      }
      ArrayCell arrayCell = (ArrayCell)( cell );
      for ( int childIndex = arrayCell.getStart()
          ; childIndex < arrayCell.getEnd() ; childIndex++ ) {
        childLoadIndex[currentArrayIndex] = childIndex;
        currentArrayIndex++;
      }
    }
    return childLoadIndex;
  }

  private void setLoadIndex(
      final List<ColumnBinary> binaryList , final int[] loadIndex ) throws IOException {
    if ( binaryList != null ) {
      for ( ColumnBinary child : binaryList ) {
        int[] childLoadIndex = loadIndex;
        if ( child.columnType == ColumnType.ARRAY ) {
          childLoadIndex = createArrayChildIndex( child , loadIndex ); 
        }
        setLoadIndex( child.columnBinaryList , childLoadIndex );
        child.setLoadIndex( loadIndex );
      }
    }
  }

  /**
   * Set Column in Array expanded to Spread and Create index of parent Array.
   * The structure of the entered list is changed.
   */
  public void createExpandColumnBinary( final List<ColumnBinary> root ) throws IOException {
    ColumnBinary current = getChildAndRemoveIfChildIsArray( this , root );
    if ( current == null ) {
      setLoadIndex( root , new int[0] );
      return;
    }
    List<ColumnBinary> linkColumnList = new ArrayList<ColumnBinary>();
    int[] rootIndex = setExpandIndex( linkColumnList , current );
    setLoadIndex( root , rootIndex );
    root.addAll( linkColumnList );
  }

  private int[] setExpandIndex(
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
        return new int[0];
      }
      IColumn arrayColumnTarget = toColumn( arrayTarget );
      IColumn innerColumn = arrayColumnTarget.getColumn(0);
      int[] parentIndexArray = new int[innerColumn.size()];
      int loopCount = arrayColumnTarget.size();
      for ( int i = 0; i < loopCount ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() && childIndex < innerColumn.size() ; childIndex++ ) {
          parentIndexArray[childIndex] = i;
        }
      }
      ColumnBinary arrayInnerColumnBinary = arrayTarget.columnBinaryList.get(0);
      ColumnBinary linkColumnBinary =
          createLinkColumnBinary( linkColumnName , arrayInnerColumnBinary );
      linkColumnList.add( linkColumnBinary );

      return parentIndexArray;
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
        return new int[0];
      }
      ColumnBinary arrayInnerColumnBinary = arrayTarget.columnBinaryList.get(0);
      ColumnBinary childColumnBinary =
          getChildAndRemoveIfChildIsArray( childNode , arrayInnerColumnBinary.columnBinaryList );
      if ( childColumnBinary == null
          || ( childColumnBinary.columnType == ColumnType.ARRAY && ! childNode.isArrayNode() ) ) {
        ColumnBinary linkColumnBinary =
            createLinkColumnBinary( linkColumnName , arrayInnerColumnBinary );
        linkColumnList.add( linkColumnBinary );
        setLoadIndex( linkColumnBinary.columnBinaryList , new int[0] );
        linkColumnBinary.setLoadIndex( new int[0] );
        return new int[0];
      }
      int[] childIndexArray = childNode.setExpandIndex( linkColumnList , childColumnBinary );

      IColumn arrayColumnTarget = toColumn( currentColumnBinary );
      IColumn arrayColumn = arrayColumnTarget.getColumn(0);
      int[] parentIndexArray = new int[childIndexArray.length];
      int parentCount = 0;
      int loopCount = arrayColumnTarget.size();
      for ( int i = 0 ; i < loopCount ; i++ ) {
        ICell cell = arrayColumnTarget.get( i );
        if ( cell.getType() != ColumnType.ARRAY ) {
          continue;
        }
        ArrayCell arrayCell = (ArrayCell)( cell );
        for ( int childIndex = arrayCell.getStart()
            ; childIndex < arrayCell.getEnd() ; childIndex++ ) {
          while ( parentCount < childIndexArray.length
              && childIndexArray[parentCount] == childIndex ) {
            parentIndexArray[parentCount] = i;
            parentCount++;
          }
        }
      }

      ColumnBinary linkColumnBinary =
          createLinkColumnBinary( linkColumnName , arrayInnerColumnBinary );
      setLoadIndex( linkColumnBinary.columnBinaryList , childIndexArray );
      linkColumnBinary.setLoadIndex( childIndexArray );
      linkColumnList.add( linkColumnBinary );

      return parentIndexArray;
    } else {
      ColumnBinary current = currentColumnBinary;
      if ( current != null && current.columnType == ColumnType.UNION ) {
        ColumnBinary newCurrent = null;
        ColumnType findTarget = ColumnType.ARRAY;
        if ( ! isArrayNode() ) {
          findTarget = ColumnType.SPREAD;
        }
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
