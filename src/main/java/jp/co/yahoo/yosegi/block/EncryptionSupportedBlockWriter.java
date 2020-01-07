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
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.optimizer.BinaryMakerOptimizer;
import jp.co.yahoo.yosegi.binary.optimizer.IOptimizerFactory;
import jp.co.yahoo.yosegi.blockindex.EncryptionSupportedBlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressorNameShortCut;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.config.YosegiConfiguration;
import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionSettingNode;
import jp.co.yahoo.yosegi.encryptor.EncryptorFactoryNameShortCut;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.keystore.KeyStore;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.Analyzer;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class EncryptionSupportedBlockWriter implements IBlockWriter {

  private static final int AAD_RND_LENGTH = 8;
  private static final int AAD_FILE_UNIQUE_LENGTH = 16;
  private static final int META_BUFFER_SIZE = 1024 * 1024 * 1;

  private final List<Integer> spreadSizeList = new ArrayList<Integer>();
  private final EncryptionSupportedBlockIndexNode blockIndexNode =
      new EncryptionSupportedBlockIndexNode();

  private byte[] aadIdentifier;
  private byte[] aadPrefix;
  private boolean disableWriteAadPrefix;
  private KeyStore keyStore;
  private IEncryptorFactory encryptorFactory;
  private EncryptionSettingNode blockEncryptionSettingNode;
  private EncryptionSettingNode columnEncryptionSettingNode;
  private ColumnBinaryMakerCustomConfigNode configNode;
  private CompressResultNode compressResultNode;
  private ByteArrayData metaBuffer;
  private int blockSize;
  private EncryptionSupportedColumnBinaryTree columnTree;
  private boolean makeCustomConfig;
  private IOptimizerFactory optimizerFactory;
  private ICompressor compressor;
  private byte[] compressorClassNameBytes;
  private byte[] headerBytes;
  private AdditionalAuthenticationData aad;

  @Override
  public void setup( final int blockSize , final Configuration config ) throws IOException {
    this.blockSize = blockSize;
    spreadSizeList.clear();

    compressor = YosegiConfiguration.getBlockCompressor( config );
    compressorClassNameBytes = CompressorNameShortCut.getShortCutName(
        compressor.getClass().getName() ).getBytes( "UTF-8" );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    defaultConfig.compressorClass =
        YosegiConfiguration.getDefaultCompressorForColumnMaker( config );
    defaultConfig.allowedRatio = YosegiConfiguration.getCompressOptimizeAllowedRatio( config );

    makeCustomConfig = YosegiConfiguration.useBinaryAutoOptimizer( config );
    optimizerFactory =
        YosegiConfiguration.getBinaryAutoOptimizerFactory( config );

    configNode = YosegiConfiguration.getUserColumnMakerSetting( config , defaultConfig );
    if ( YosegiConfiguration.useUserColumnMakerSetting( config ) ) {
      makeCustomConfig = false;
    }

    keyStore = YosegiConfiguration.getKeyStore( config );
    encryptorFactory = YosegiConfiguration.getEncryptorFactory( config );
    blockEncryptionSettingNode =
        YosegiConfiguration.getBlockEncryptionSettingNode( config , keyStore );
    columnEncryptionSettingNode =
        YosegiConfiguration.getColumnEncryptionSettingNode( config , keyStore );
    aadPrefix = YosegiConfiguration.getAadPrefix( config );
    disableWriteAadPrefix = YosegiConfiguration.isWritingAadPrefixDisabled( config );

    SecureRandom rnd = new SecureRandom();
    byte[] rndBinary =  new byte[AAD_RND_LENGTH];
    rnd.nextBytes( rndBinary );
    byte[] currentTimestampBinary = ByteBuffer.allocate( Long.BYTES )
        .putLong( System.currentTimeMillis() ).array();
    aadIdentifier = new byte[AAD_FILE_UNIQUE_LENGTH];
    ByteBuffer aadIdentifierBuffer = ByteBuffer.wrap( aadIdentifier );
    aadIdentifierBuffer.put( rndBinary );
    aadIdentifierBuffer.put( currentTimestampBinary );
    if ( aadPrefix.length == 0 ) {
      aad = new AdditionalAuthenticationData( aadIdentifier );
    } else {
      aad = new AdditionalAuthenticationData( aadIdentifier , aadPrefix );
    }

    compressResultNode = new CompressResultNode();

    metaBuffer = new ByteArrayData( META_BUFFER_SIZE );
    columnTree = new EncryptionSupportedColumnBinaryTree();

    headerBytes = new byte[0];
  }

  @Override
  public void appendHeader( final byte[] headerBytes ) {
    if ( this.headerBytes.length == 0 ) {
      this.headerBytes = headerBytes;
    } else {
      byte[] mergeByte = new byte[ this.headerBytes.length + headerBytes.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( mergeByte );
      wrapBuffer.put( this.headerBytes );
      wrapBuffer.put( headerBytes );
      this.headerBytes = mergeByte;
    }
  }

  @Override
  public void append(
        final int spreadSize , final List<ColumnBinary> binaryList ) throws IOException {
    for ( ColumnBinary columnBinary : binaryList ) {
      if ( columnBinary != null ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary , getRegisterSpreadCount() );
      }
    }
    spreadSizeList.add( spreadSize );

    columnTree.addChild( binaryList );
    if ( blockSize < size() ) {
      throw new IOException( "Buffer overflow." );
    }
  }

  @Override
  public List<ColumnBinary> convertRow( final Spread spread ) throws IOException {
    if ( makeCustomConfig ) {
      List<IColumnAnalizeResult> analizeResultList = Analyzer.analize( spread );
      BinaryMakerOptimizer optimizer = new BinaryMakerOptimizer( analizeResultList );
      configNode = optimizer.createConfigNode( configNode.getCurrentConfig() , optimizerFactory );
      makeCustomConfig = false;
    }
    List<ColumnBinary> result = new ArrayList<ColumnBinary>();
    for ( int i = 0 ; i < spread.getColumnSize() ; i++ ) {
      IColumn column = spread.getColumn( i );
      ColumnBinaryMakerConfig commonConfig = configNode.getCurrentConfig();
      ColumnBinaryMakerCustomConfigNode childConfigNode =
          configNode.getChildConfigNode( column.getColumnName() );
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( column.getColumnType() );
      if ( childConfigNode != null ) {
        maker = childConfigNode.getCurrentConfig().getColumnMaker( column.getColumnType() );
      }
      result.add( maker.toBinary(
          commonConfig ,
          childConfigNode ,
          compressResultNode.getChild( column.getColumnName() ) ,
          column ) );
    }
    return result;
  }

  @Override
  public boolean canAppend( final List<ColumnBinary> binaryList ) throws IOException {
    boolean result = sizeAfterAppend( binaryList ) <= blockSize;
    if ( ! result && spreadSizeList.isEmpty() ) {
      throw new IOException( "Can not write Spread larger than block size."
          + "Increase the block size or reduce the size of the Spread." );
    }
    return result;
  }

  /**
   * Calculate the data size after addition.
   */
  public int sizeAfterAppend( final List<ColumnBinary> binaryList ) throws IOException {
    EncryptionSupportedBlockIndexNode cloneBlockIndexNode = blockIndexNode.clone();
    for ( ColumnBinary columnBinary : binaryList ) {
      if ( columnBinary != null ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( cloneBlockIndexNode , columnBinary , getRegisterSpreadCount() );
      }
    }
    int appendSpreadSizeBinary = Integer.BYTES;
    int blockIndexBinaryLength = cloneBlockIndexNode.getBinarySize(
          "root" , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
    int metaBinaryLength = columnTree.metaSizeAfterAppend(
            binaryList , "root" , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
    return blockMetaSize( blockIndexBinaryLength , metaBinaryLength )
        + appendSpreadSizeBinary
        + columnTree.dataSizeAfterAppend(
            binaryList , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
  }

  /**
   * get block meta size.
   */
  public int blockMetaSize(
      final int blockIndexBinaryLength , final int metaBinaryLength ) throws IOException {
    byte[] encryptorFactoryBinary = EncryptorFactoryNameShortCut.getShortCutName(
        encryptorFactory.getClass().getName() ).getBytes( "UTF-8" );
    aad.setOrdinal( 0 );
    int checkKeyBinarySize = keyStore.createCheckKeyBinarySize( aad , encryptorFactory );
    int blockHeaderBinarySize =
        Integer.BYTES
        + compressorClassNameBytes.length
        + Integer.BYTES
        + Byte.BYTES
        + aadIdentifier.length
        + Byte.BYTES
        + ( disableWriteAadPrefix ? 0 : aadPrefix.length )
        + Short.BYTES
        + Integer.BYTES
        + encryptorFactoryBinary.length
        + Integer.BYTES
        + checkKeyBinarySize;

    EncryptionSettingNode blockMetaBlockEncryptionSettingNode =
        blockEncryptionSettingNode.getChildNode( "meta" );
    byte isBlockMetaEncrypt = blockMetaBlockEncryptionSettingNode
        .isEncryptNode() ? (byte)1 : (byte)0;
    int encryptMetaBinaryLength = 0;
    int spreadSizeBinaryLength = Integer.BYTES + ( Integer.BYTES * spreadSizeList.size() );
    int blockIndexLength = Integer.BYTES + blockIndexBinaryLength;
    int metaLength = spreadSizeBinaryLength + blockIndexLength + metaBinaryLength;
    if ( isBlockMetaEncrypt == 1 ) {
      String blockMetaKey = blockMetaBlockEncryptionSettingNode.getKeyName();
      byte[] blockMetaKeyBinary = blockMetaKey.getBytes( "UTF-8" );
      encryptMetaBinaryLength = Byte.BYTES
          + Integer.BYTES * 2
          + blockMetaKeyBinary.length
          + encryptorFactory.getEncryptSize( metaLength , Module.BLOCK_META , aad  );
    } else {
      encryptMetaBinaryLength = Byte.BYTES + Integer.BYTES * 2 + metaLength;
    }
    return headerBytes.length
        // metaLength
        + Integer.BYTES
        // header binary length
        + blockHeaderBinarySize
        // meta length
        + encryptMetaBinaryLength;
  }

  @Override
  public int size() {
    try {
      int blockIndexLength = blockIndexNode.getBinarySize(
          "root" , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
      int metaBinaryLength = columnTree.metaSize(
          "root" , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
      return blockMetaSize( blockIndexLength , metaBinaryLength )
          + columnTree.dataSize(
              aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
    } catch ( IOException ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public void writeFixedBlock( final OutputStream out ) throws IOException {
    write( out , blockSize );
  }

  @Override
  public void writeVariableBlock( final OutputStream out ) throws IOException {
    write( out , -1 );
  }

  @Override
  public void write( final OutputStream out , final int dataSize ) throws IOException {
    int writeDataSize = 0;
    out.write( headerBytes , 0 , headerBytes.length );
    writeDataSize += headerBytes.length;

    byte[] spreadSizeBinary = new byte[Integer.BYTES + ( Integer.BYTES * spreadSizeList.size() )];
    ByteBuffer spreadSizeBuffer = ByteBuffer.wrap( spreadSizeBinary );
    spreadSizeBuffer.putInt( spreadSizeList.size() );
    for ( Integer spreadSize : spreadSizeList ) {
      spreadSizeBuffer.putInt( spreadSize.intValue() );
    }

    int indexBinarySize = blockIndexNode.getBinarySize(
        "root" , aad , keyStore , encryptorFactory , columnEncryptionSettingNode );
    byte[] indexBinary = new byte[Integer.BYTES + indexBinarySize];
    ByteBuffer.wrap( indexBinary ).putInt( indexBinarySize );
    aad.setOrdinal( 0 );
    blockIndexNode.toBinary(
        "root" ,
        indexBinary ,
        Integer.BYTES,
        aad,
        keyStore,
        encryptorFactory,
        columnEncryptionSettingNode );
    blockIndexNode.clear();

    metaBuffer.append( spreadSizeBinary , 0 , spreadSizeBinary.length );
    metaBuffer.append( indexBinary , 0 , indexBinary.length );

    aad.setOrdinal( 0 );
    columnTree.createMeta(
        "root" ,
        metaBuffer ,
        0 ,
        aad ,
        keyStore ,
        encryptorFactory ,
        columnEncryptionSettingNode );

    byte[] metaBinary = compressor.compress( metaBuffer.getBytes() , 0 , metaBuffer.getLength() );

    EncryptionSettingNode blockMetaBlockEncryptionSettingNode =
        blockEncryptionSettingNode.getChildNode( "meta" );
    byte isBlockMetaEncrypt = blockMetaBlockEncryptionSettingNode
        .isEncryptNode() ? (byte)1 : (byte)0;
    aad.setOrdinal( 0 );
    byte[] encryptMetaBinary;
    if ( isBlockMetaEncrypt == 1 ) {
      String blockMetaKey = blockMetaBlockEncryptionSettingNode.getKeyName();
      byte[] blockMetaKeyBinary = blockMetaKey.getBytes( "UTF-8" );
      IEncryptor encryptor = encryptorFactory.createEncryptor(
          keyStore.getKey( blockMetaKey ),
          Module.BLOCK_META,
          aad );
      byte[] encryptMetaData =
          encryptor.encrypt( metaBinary , 0 , metaBinary.length );
      encryptMetaBinary = new byte[
          Byte.BYTES + Integer.BYTES * 2 + blockMetaKeyBinary.length + encryptMetaData.length ];
      ByteBuffer encryptMetaBuffer = ByteBuffer.wrap( encryptMetaBinary );
      encryptMetaBuffer.put( isBlockMetaEncrypt );
      encryptMetaBuffer.putInt( blockMetaKeyBinary.length );
      encryptMetaBuffer.put( blockMetaKeyBinary );
      encryptMetaBuffer.putInt( encryptMetaData.length );
      encryptMetaBuffer.put( encryptMetaData );
    } else {
      encryptMetaBinary = new byte[
          Byte.BYTES + Integer.BYTES * 2 + metaBinary.length ];
      ByteBuffer encryptMetaBuffer = ByteBuffer.wrap( encryptMetaBinary );
      encryptMetaBuffer.put( isBlockMetaEncrypt );
      encryptMetaBuffer.putInt( 0 );
      encryptMetaBuffer.putInt( metaBinary.length );
      encryptMetaBuffer.put( metaBinary );
    }

    byte[] encryptorFactoryBinary = EncryptorFactoryNameShortCut.getShortCutName(
        encryptorFactory.getClass().getName() ).getBytes( "UTF-8" );

    aad.setOrdinal( 0 );
    byte[] checkKeyBinary = keyStore.createCheckKeyBinary( aad , encryptorFactory );
    byte[] blockHeaderBinary = new byte[
        Integer.BYTES   
        + compressorClassNameBytes.length
        + Integer.BYTES
        + Byte.BYTES
        + aadIdentifier.length
        + Byte.BYTES
        + ( disableWriteAadPrefix ? 0 : aadPrefix.length )
        + Short.BYTES
        + Integer.BYTES
        + encryptorFactoryBinary.length
        + Integer.BYTES
        + checkKeyBinary.length ];
    ByteBuffer blockIndexBuffer = ByteBuffer.wrap( blockHeaderBinary );
    blockIndexBuffer.putInt( compressorClassNameBytes.length );
    blockIndexBuffer.put( compressorClassNameBytes );
    blockIndexBuffer.putInt( encryptMetaBinary.length );

    blockIndexBuffer.put( (byte)aadIdentifier.length );
    blockIndexBuffer.put( aadIdentifier );
    blockIndexBuffer.put( (byte)( disableWriteAadPrefix ? 0 : aadPrefix.length ) );
    if ( ! disableWriteAadPrefix ) {
      blockIndexBuffer.put( aadPrefix );
    }
    blockIndexBuffer.putShort( (byte)aad.getBlockOrdinal() );
    blockIndexBuffer.putInt( encryptorFactoryBinary.length );
    blockIndexBuffer.put( encryptorFactoryBinary );
    blockIndexBuffer.putInt( checkKeyBinary.length );
    blockIndexBuffer.put( checkKeyBinary );

    int blockMetaLength = blockHeaderBinary.length + encryptMetaBinary.length;
    out.write( ByteBuffer.allocate(Integer.BYTES).putInt( blockMetaLength ).array() );
    writeDataSize += Integer.BYTES;
    out.write( blockHeaderBinary );
    writeDataSize += blockHeaderBinary.length;
    out.write( encryptMetaBinary , 0 , encryptMetaBinary.length );
    writeDataSize += encryptMetaBinary.length;
    aad.setOrdinal( 0 );
    writeDataSize += columnTree.writeData(
        out ,
        aad ,
        keyStore ,
        encryptorFactory ,
        columnEncryptionSettingNode );
    if ( blockSize < writeDataSize ) {
      throw new IOException(
          "The exception is that the metasize after compression gets larger."
          + "Please turn off the option for meta compression." );
    }

    if ( dataSize != -1 ) {
      int nullLength = dataSize - writeDataSize;
      out.write( new byte[nullLength] );
    } 

    aad.nextBlock();
    spreadSizeList.clear();
    metaBuffer.clear();
    columnTree.clear();
    headerBytes = new byte[0];
  }

  @Override
  public String getReaderClassName() {
    return EncryptionSupportedBlockReader.class.getName();
  }

  @Override
  public void close() throws IOException {
    spreadSizeList.clear();
    metaBuffer.clear();
    columnTree.clear();
  }

  /**
   * This method is for testing if AAD is unique and should not be used.
   */
  public AdditionalAuthenticationData getAad() {
    return aad;
  }

  /**
   * This method is for testing if AAD is unique and should not be used.
   */
  public void setAad( AdditionalAuthenticationData aad ) {
    this.aad = aad;
  }

  private int getRegisterSpreadCount() {
    return spreadSizeList.size();
  }

}
