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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressorNameShortCut;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.GzipCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.inmemory.SpreadRawConverter;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.expand.ExpandFunctionFactory;
import jp.co.yahoo.yosegi.spread.expand.IExpandFunction;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.flatten.FlattenFunctionFactory;
import jp.co.yahoo.yosegi.spread.flatten.IFlattenFunction;
import jp.co.yahoo.yosegi.stats.SummaryStats;
import jp.co.yahoo.yosegi.util.io.InputStreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PushdownSupportedBlockReader implements IBlockReader {

  private final Block block;
  private final ColumnBinaryTree columnBinaryTree = new ColumnBinaryTree();
  private final List<Integer> spreadSizeList = new ArrayList<Integer>();
  private final SummaryStats readSummaryStats = new SummaryStats();

  private ColumnNameNode columnFilterNode;
  private int readCount;
  private ICompressor compressor = new GzipCompressor();
  private IExpandFunction expandFunction;
  private IFlattenFunction flattenFunction;
  private BlockIndexNode blockIndexNode = new BlockIndexNode();
  private IExpressionNode blockSkipIndex;
  private long readBytes = 0;

  public PushdownSupportedBlockReader() {
    block = new Block();
  }

  private String[] mergeLinkColumnName( final String[] original , final String[] merge ) {
    if ( merge.length == 0 ) {
      return original;
    }
    String[] result = new String[ merge.length + original.length - 1];
    System.arraycopy( merge , 0 ,result  , 0 , merge.length );
    System.arraycopy( original , 1 , result , merge.length , original.length - 1 );

    return result;
  }

  @Override
  public void setup( final Configuration config ) throws IOException {
    expandFunction = ExpandFunctionFactory.get( config );
    flattenFunction = FlattenFunctionFactory.get( config );

    columnFilterNode = new ColumnNameNode( "root" );
    List<String[]> needColumnList =
        ReadColumnUtil.readColumnSetting( config.get( "spread.reader.read.column.names" ) );
    for ( String[] needColumn : needColumnList ) {
      String[] flattenColumnNameArray =
          flattenFunction.getFlattenColumnName( needColumn[0] );
      String[] flattenMergeNeedColumn =
          mergeLinkColumnName( needColumn , flattenColumnNameArray );

      String[] expandColumnNameArray =
          expandFunction.getExpandLinkColumnName( flattenMergeNeedColumn[0] );
      String[] mergeNeedColumn =
          mergeLinkColumnName( flattenMergeNeedColumn , expandColumnNameArray );

      ColumnNameNode currentColumnNameNode = columnFilterNode;
      for ( int i = 0 ; i < mergeNeedColumn.length ; i++ ) {
        String columnName = mergeNeedColumn[i];
        ColumnNameNode columnNameNode = currentColumnNameNode.getChild( columnName );
        if ( columnNameNode == null ) {
          columnNameNode = new ColumnNameNode( columnName );
        }
        if ( i == ( mergeNeedColumn.length - 1 ) ) {
          columnNameNode.setNeedAllChild( true );
        }
        currentColumnNameNode.addChild( columnNameNode );
        currentColumnNameNode = columnNameNode;
      }
    }
    if ( columnFilterNode.isChildEmpty() ) {
      columnFilterNode.setNeedAllChild( true );
    } else {
      List<String[]> expandNeedColumnList = expandFunction.getExpandColumnName();
      for ( String[] needColumn : expandNeedColumnList ) {
        ColumnNameNode currentColumnNameNode = columnFilterNode;
        for ( int i = 0 ; i < needColumn.length ; i++ ) {
          String columnName = needColumn[i];
          ColumnNameNode columnNameNode = currentColumnNameNode.getChild( columnName );
          if ( columnNameNode == null ) {
            columnNameNode = new ColumnNameNode( columnName );
            currentColumnNameNode.addChild( columnNameNode );
          }
          currentColumnNameNode = columnNameNode;
        }
      }
    }
  }

  @Override
  public void setBlockSize( final int blockSize ) {}

  @Override
  public void setBlockSkipIndex( final IExpressionNode blockSkipIndex ) {
    this.blockSkipIndex = blockSkipIndex;
  }

  @Override
  public void setStream( final InputStream in , final int blockSize ) throws IOException {
    clear();
    byte[] compressorClassLengthBytes = new byte[Integer.BYTES];
    InputStreamUtils.read( in , compressorClassLengthBytes , 0 , Integer.BYTES );
    int compressorClassLength = ByteBuffer.wrap( compressorClassLengthBytes ).getInt();
    byte[] compressorClassBytes = new byte[ compressorClassLength ];
    InputStreamUtils.read( in , compressorClassBytes , 0 , compressorClassBytes.length );
    compressor = FindCompressor.get(
        CompressorNameShortCut.getClassName( new String( compressorClassBytes , "UTF-8" ) ) );

    byte[] blockIndexLengthBytes = new byte[Integer.BYTES];
    readBytes += InputStreamUtils.read( in , blockIndexLengthBytes , 0 , Integer.BYTES );
    int blockIndexLength = ByteBuffer.wrap( blockIndexLengthBytes ).getInt();
    byte[] blockIndexBinary = new byte[ blockIndexLength ];
    readBytes += InputStreamUtils.read( in , blockIndexBinary , 0 , blockIndexBinary.length );

    blockIndexNode = BlockIndexNode.createFromBinary( blockIndexBinary , 0 );
    expandFunction.expandIndexNode( blockIndexNode );
    flattenFunction.flattenIndexNode( blockIndexNode );

    List<Integer> blockIndexList = null;
    if ( blockSkipIndex != null ) {
      blockIndexList = blockSkipIndex.getBlockSpreadIndex( blockIndexNode );
    }
    if ( blockIndexList != null && blockIndexList.isEmpty() ) {
      InputStreamUtils.skip(
          in , blockSize - ( 4 + compressorClassLength + 4 + blockIndexBinary.length ) );
    } else {
      Set<Integer> spreadIndexDict = null;
      if ( blockIndexList != null ) {
        spreadIndexDict = new HashSet<Integer>( blockIndexList );
      }
      setStream(
          in ,
          blockSize - ( 4 + compressorClassLength + 4 + blockIndexBinary.length ) ,
          spreadIndexDict );
    }
  }

  private void setStream(
      final InputStream in ,
      final int blockSize ,
      final Set<Integer> spreadIndexDict ) throws IOException {
    spreadSizeList.clear();
    columnBinaryTree.clear();
    columnBinaryTree.setColumnFilter( columnFilterNode );

    byte[] spreadSizeLengthBytes = new byte[Integer.BYTES];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( spreadSizeLengthBytes );
    readBytes += InputStreamUtils.read( in , spreadSizeLengthBytes , 0 , Integer.BYTES );
    int spreadSizeLength = wrapBuffer.getInt(0);

    byte[] spreadSizeBytes = new byte[ Integer.BYTES * spreadSizeLength ];
    wrapBuffer = ByteBuffer.wrap( spreadSizeBytes );
    readBytes += InputStreamUtils.read(
        in , spreadSizeBytes , 0 , Integer.BYTES * spreadSizeLength );
    for ( int i = 0 ; i < spreadSizeLength ; i++ ) {
      spreadSizeList.add( wrapBuffer.getInt() );
    }

    byte[] lengthBytes = new byte[Integer.BYTES];
    wrapBuffer = ByteBuffer.wrap( lengthBytes );
    readBytes += InputStreamUtils.read( in , lengthBytes , 0 , Integer.BYTES );

    int metaLength = wrapBuffer.getInt( 0 );
    byte[] metaBytes = new byte[metaLength];

    readBytes += InputStreamUtils.read( in , metaBytes , 0 , metaLength );

    int decompressSize = compressor.getDecompressSize( metaBytes , 0 , metaLength );
    byte[] metaBinary = new byte[decompressSize];
    int binaryLength = compressor.decompressAndSet(  metaBytes , 0 , metaLength , metaBinary );
    columnBinaryTree.toColumnBinaryTree( metaBinary , 0 , spreadIndexDict );

    block.setColumnBinaryTree( columnBinaryTree );

    int dataBufferLength =
        blockSize
        - metaLength
        - Integer.BYTES
        - Integer.BYTES
        - Integer.BYTES * spreadSizeLength;
    List<BlockReadOffset> readOffsetList = columnBinaryTree.getBlockReadOffset();
    Collections.sort( readOffsetList );

    int inOffset = 0;
    for ( BlockReadOffset blockReadOffset : readOffsetList ) {
      inOffset += InputStreamUtils.skip( in , blockReadOffset.streamStart - inOffset );
      inOffset = blockReadOffset.streamStart;
      inOffset += InputStreamUtils.read(
          in , blockReadOffset.buffer , blockReadOffset.bufferStart , blockReadOffset.length );
      readBytes += blockReadOffset.length;
    }
    if ( inOffset < dataBufferLength ) {
      inOffset += InputStreamUtils.skip( in , dataBufferLength - inOffset );
    }

    readCount = 0;
  }

  @Override
  public boolean hasNext() throws IOException {
    return readCount < block.size();
  }

  @Override
  public Spread next() throws IOException {
    SpreadRawConverter converter = new SpreadRawConverter();

    List<ColumnBinary> raw = nextRaw();
    int loadSize = getCurrentSpreadSize();
    for ( ColumnBinary columnBinary : raw ) {
      if ( columnBinary.loadIndex != null ) {
        loadSize = columnBinary.loadIndex.length;
        break;
      }
    }
    return converter.convert( raw , loadSize );
  }

  @Override
  public List<ColumnBinary> nextRaw() throws IOException {
    List<ColumnBinary> columnBinaryList = block.get( readCount );
    readCount++;
    expandFunction.expandFromColumnBinary( columnBinaryList );
    return flattenFunction.flattenFromColumnBinary( columnBinaryList );
  }

  @Override
  public int getBlockReadCount() {
    return readCount;
  }

  @Override
  public long getReadBytes() {
    return readBytes;
  }

  @Override
  public int getBlockCount() {
    return block.size();
  }

  @Override
  public SummaryStats getReadStats() {
    return readSummaryStats;
  }

  @Override
  public Integer getCurrentSpreadSize() {
    return spreadSizeList.get( readCount - 1 );
  }

  @Override
  public void close() throws IOException {}

  /**
   * Clear the information of the set block.
   */
  public void clear() {
    spreadSizeList.clear();
    columnBinaryTree.clear();
    readCount = 0;
    readBytes = 0;
    block.setColumnBinaryTree( null );
  }

}
