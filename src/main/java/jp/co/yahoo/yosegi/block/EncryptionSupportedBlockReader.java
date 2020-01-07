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
import jp.co.yahoo.yosegi.blockindex.EncryptionSupportedBlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressorNameShortCut;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.config.YosegiConfiguration;
import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;
import jp.co.yahoo.yosegi.encryptor.EncryptorFactoryNameShortCut;
import jp.co.yahoo.yosegi.encryptor.FindEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.keystore.KeyStore;
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

public class EncryptionSupportedBlockReader implements IBlockReader {

  private final EncryptionSupportedBlock block;
  private final EncryptionSupportedColumnBinaryTree columnBinaryTree =
      new EncryptionSupportedColumnBinaryTree();
  private final List<Integer> spreadSizeList = new ArrayList<Integer>();
  private final SummaryStats readSummaryStats = new SummaryStats();

  private ColumnNameNode columnFilterNode;
  private int readCount;
  private ICompressor compressor;
  private IExpandFunction expandFunction;
  private IFlattenFunction flattenFunction;
  private BlockIndexNode blockIndexNode = new BlockIndexNode();
  private IExpressionNode blockSkipIndex;
  private byte[] userAadPrefix;
  private EncryptionKey[] keys;
  private KeyStore keyStore;
  private AdditionalAuthenticationData aad;
  private IEncryptorFactory encryptorFactory;

  public EncryptionSupportedBlockReader() {
    block = new EncryptionSupportedBlock();
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
    keys = YosegiConfiguration.getEncryptionKeys( config );
    userAadPrefix = YosegiConfiguration.getAadPrefix( config );

    expandFunction = ExpandFunctionFactory.get( config );
    flattenFunction = FlattenFunctionFactory.get( config );

    columnFilterNode = new ColumnNameNode( "root" );
    List<String[]> needColumnList = YosegiConfiguration.getReadColumnName( config );
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
    byte[] blockHeaderSizeBytes = new byte[Integer.BYTES];
    InputStreamUtils.read( in , blockHeaderSizeBytes , 0 , blockHeaderSizeBytes.length );
    int blockHeaderSize = ByteBuffer.wrap( blockHeaderSizeBytes ).getInt();

    byte[] blockHeaderBinary = new byte[blockHeaderSize];
    InputStreamUtils.read( in , blockHeaderBinary , 0 , blockHeaderBinary.length );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( blockHeaderBinary );
    int compressorClassLength = wrapBuffer.getInt();
    compressor = FindCompressor.get( CompressorNameShortCut.getClassName( new String(
        blockHeaderBinary ,
        wrapBuffer.position() ,
        compressorClassLength ,
        "UTF-8" ) ) );
    wrapBuffer.position( wrapBuffer.position() + compressorClassLength );
    final int columnMetaBinaryLength = wrapBuffer.getInt();

    byte[] aadFileUnique = new byte[wrapBuffer.get()];
    wrapBuffer.get( aadFileUnique );
    byte[] aadPrefix = new byte[wrapBuffer.get()];
    wrapBuffer.get( aadPrefix );
    if ( userAadPrefix.length != 0 ) {
      aad = new AdditionalAuthenticationData( aadFileUnique , userAadPrefix );
    } else {
      if ( aadPrefix.length == 0 ) {
        aad = new AdditionalAuthenticationData( aadFileUnique );
      } else {
        aad = new AdditionalAuthenticationData( aadFileUnique , aadPrefix );
      }
    }
    aad.setBlockOrdinal( wrapBuffer.getShort() );

    int encryptorFactoryBinarySize = wrapBuffer.getInt();
    String encryptorFactoryClassName = EncryptorFactoryNameShortCut.getClassName(
        new String(
          wrapBuffer.array(),
          wrapBuffer.position() ,
          encryptorFactoryBinarySize , "UTF-8" ) );
    encryptorFactory = FindEncryptorFactory.get( encryptorFactoryClassName );
    wrapBuffer.position( wrapBuffer.position() + encryptorFactoryBinarySize );

    int checkKeyBinarySize = wrapBuffer.getInt();
    keyStore = new KeyStore();
    aad.setOrdinal( 0 );
    keyStore.registKeyFromBinary(
        keys ,
        aad ,
        encryptorFactory ,
        wrapBuffer.array() ,
        wrapBuffer.position() ,
        checkKeyBinarySize );
    wrapBuffer.position( wrapBuffer.position() + checkKeyBinarySize );

    boolean isEncrypt = wrapBuffer.get() == 1 ? true : false;
    int keyBinaryLength = wrapBuffer.getInt();
    String blockMetaKey = new String(
        wrapBuffer.array() , wrapBuffer.position() , keyBinaryLength );
    wrapBuffer.position( wrapBuffer.position() + keyBinaryLength );
    int encryptBinaryLength = wrapBuffer.getInt();
    aad.setOrdinal( 0 );
    byte[] blockMetaBinary;
    if ( isEncrypt ) {
      IEncryptor encryptor = encryptorFactory.createEncryptor(
          keyStore.getKey( blockMetaKey ),
          Module.BLOCK_META,
          aad );
      byte[] decryptMetaData =
          encryptor.decrypt( wrapBuffer.array() , wrapBuffer.position() , encryptBinaryLength );
      blockMetaBinary = compressor.decompress(
          decryptMetaData , 0 , decryptMetaData.length );
    } else {
      blockMetaBinary = compressor.decompress(
          wrapBuffer.array() , wrapBuffer.position() , encryptBinaryLength );
    }

    wrapBuffer = ByteBuffer.wrap( blockMetaBinary );
    spreadSizeList.clear();
    int spreadSize = wrapBuffer.getInt();
    for ( int i = 0 ; i < spreadSize ; i++ ) {
      spreadSizeList.add( wrapBuffer.getInt() );
    }

    int indexBinarySize = wrapBuffer.getInt();
    aad.setOrdinal( 0 );
    blockIndexNode = EncryptionSupportedBlockIndexNode.createFromBinary(
        wrapBuffer.array() ,
        wrapBuffer.position(),
        aad,
        keyStore,
        encryptorFactory );
    wrapBuffer.position( wrapBuffer.position() + indexBinarySize );

    expandFunction.expandIndexNode( blockIndexNode );
    flattenFunction.flattenIndexNode( blockIndexNode );

    List<Integer> blockIndexList = null;
    if ( blockSkipIndex != null ) {
      blockIndexList = blockSkipIndex.getBlockSpreadIndex( blockIndexNode );
    }

    if ( blockIndexList != null && blockIndexList.isEmpty() ) {
      InputStreamUtils.skip(
          in , blockSize - ( blockHeaderSizeBytes.length + blockHeaderBinary.length ) );
      return;
    }

    Set<Integer> spreadIndexDict = null;
    if ( blockIndexList != null ) {
      spreadIndexDict = new HashSet<Integer>( blockIndexList );
    }

    columnBinaryTree.clear();
    columnBinaryTree.setColumnFilter( columnFilterNode );
    aad.setOrdinal( 0 );
    columnBinaryTree.toColumnBinaryTree(
        wrapBuffer.array() ,
        wrapBuffer.position() ,
        spreadIndexDict ,
        aad ,
        keyStore ,
        encryptorFactory );
    block.setColumnBinaryTree( columnBinaryTree );

    int dataBufferLength =
        blockSize
        - blockHeaderSizeBytes.length
        - blockHeaderBinary.length;
    List<EncryptionSupportedBlockReadOffset> readOffsetList = columnBinaryTree.getBlockReadOffset();
    Collections.sort( readOffsetList );

    int inOffset = 0;
    for ( EncryptionSupportedBlockReadOffset blockReadOffset : readOffsetList ) {
      inOffset += InputStreamUtils.skip( in , blockReadOffset.streamStart - inOffset );
      inOffset += blockReadOffset.read( in , aad , keyStore , encryptorFactory );
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
    Spread spread = new Spread();
    int spreadSize = spreadSizeList.get( readCount ).intValue();
    for ( ColumnBinary columnBinary : block.get( readCount ) ) {
      if ( columnBinary != null ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        spread.addColumn( maker.toColumn( columnBinary ) );
        readSummaryStats.merge( columnBinary.toSummaryStats() );
      }
    }
    spread.setRowCount( spreadSize );

    readCount++;
    Spread expandSpread = expandFunction.expand( spread );
    return flattenFunction.flatten( expandSpread );
  }

  @Override
  public List<ColumnBinary> nextRaw() throws IOException {
    List<ColumnBinary> columnBinaryList = block.get( readCount );
    readCount++;
    return columnBinaryList;
  }

  @Override
  public int getBlockReadCount() {
    return readCount;
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
    aad = null;
    encryptorFactory = null;
    spreadSizeList.clear();
    columnBinaryTree.clear();
    readCount = 0;
    block.setColumnBinaryTree( null );
  }

}
