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

package jp.co.yahoo.yosegi.blockindex;

import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionSettingNode;
import jp.co.yahoo.yosegi.encryptor.EncryptorFactoryNameShortCut;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.keystore.KeyStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class EncryptionSupportedBlockIndexNode extends BlockIndexNode {

  private String nodeName;

  public EncryptionSupportedBlockIndexNode() {
    super();
  }

  public void setNodeName( final String nodeName ) {
    this.nodeName = nodeName;
  }

  public String getNodeName() {
    return nodeName;
  }

  @Override
  public EncryptionSupportedBlockIndexNode clone() {
    EncryptionSupportedBlockIndexNode result = new EncryptionSupportedBlockIndexNode();
    result.childContainer = new HashMap<String,BlockIndexNode>();
    for ( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ) {
      result.childContainer.put( entry.getKey() , entry.getValue().clone() );
    }
    if ( blockIndex != null ) { 
      result.blockIndex = blockIndex.clone();
    }
    result.isDisable = isDisable;
    return result;
  }

  @Override
  public int getBinarySize() throws IOException {
    throw new IOException("This method is not supported.");
  }
  
  /**
   * Calculate the size of the binary.
   */
  public int getBinarySize(
      final String nodeName,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    if ( isDisable ) {
      return 0;
    }
    int length = 0;

    // encript length
    byte isEncript = encryptionSettingNode.isEncryptNode() ? (byte)1 : (byte)0;
    length += Byte.BYTES + Integer.BYTES;
    if ( isEncript != 0 ) {
      byte[] keyName = encryptionSettingNode.getKeyName().getBytes( "UTF-8" );
      length += keyName.length;
    }
    // add child count
    length += Integer.BYTES;

    if ( blockIndex == null ) {
      length += Integer.BYTES;
    } else {
      byte[] nodeNameBytes = nodeName.getBytes( "UTF-8" );
      byte[] rangeClassNameBytes = RangeBlockIndexNameShortCut.getShortCutName(
          blockIndex.getClass().getName() ).getBytes( "UTF-8" );
      byte[] indexBinary = blockIndex.toBinary();
      int newIndexBinarySize = Integer.BYTES * 3
          + nodeNameBytes.length
          + rangeClassNameBytes.length
          + indexBinary.length;

      length += Integer.BYTES;
      if ( isEncript == 1 ) {
        length += factory.getEncryptSize( newIndexBinarySize , Module.COLUMN_INDEX , aad );
      } else {
        length += newIndexBinarySize;
      }
    }
    for ( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ) {
      String childKeyName = entry.getKey();
      EncryptionSettingNode childEncryptionSettingNode =
          childEncryptionSettingNode = encryptionSettingNode.getChildNode( childKeyName );
      EncryptionSupportedBlockIndexNode childBlockIndexNode =
          (EncryptionSupportedBlockIndexNode)( entry.getValue() );
      int childLength = childBlockIndexNode.getBinarySize(
          childKeyName , aad , keyStore , factory , childEncryptionSettingNode );
      if ( childLength != 0 ) {
        length += Integer.BYTES + childLength;
      }
    }
    return length;
  }

  @Override
  public BlockIndexNode getChildNode( final String nodeName ) {
    if ( ! childContainer.containsKey( nodeName ) ) {
      childContainer.put( nodeName , new EncryptionSupportedBlockIndexNode() );
    }
    return childContainer.get( nodeName );
  }

  @Override
  public int toBinary( final byte[] buffer , final int start ) throws IOException {
    throw new IOException("This method is not supported.");
  }

  /**
   * Create binary.
   */
  public int toBinary(
      final String nodeName,
      final byte[] buffer ,
      final int start ,
      final AdditionalAuthenticationData aad, 
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    if ( isDisable ) {
      return start;
    }
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    wrapBuffer.position( start );

    byte isEncript = encryptionSettingNode.isEncryptNode() ? (byte)1 : (byte)0;
    wrapBuffer.put( isEncript );
    if ( isEncript == 0 ) {
      wrapBuffer.putInt(0);
    } else {
      byte[] keyName = encryptionSettingNode.getKeyName().getBytes( "UTF-8" );
      wrapBuffer.putInt( keyName.length );
      wrapBuffer.put( keyName );
    }
    final int childCountOffset = wrapBuffer.position();
    wrapBuffer.putInt( 0 );

    if ( blockIndex == null ) {
      wrapBuffer.putInt( 0 );
    } else {
      byte[] nodeNameBinary = nodeName.getBytes( "UTF-8" );
      byte[] rangeClassNameBytes = RangeBlockIndexNameShortCut.getShortCutName(
          blockIndex.getClass().getName() ).getBytes( "UTF-8" );
      byte[] indexBinary = blockIndex.toBinary();
      byte[] newIndexBinary = new byte[ Integer.BYTES * 3
          + nodeNameBinary.length
          + rangeClassNameBytes.length
          + indexBinary.length ];

      ByteBuffer newIndexBuffer = ByteBuffer.wrap( newIndexBinary );
      newIndexBuffer.putInt( nodeNameBinary.length );
      newIndexBuffer.put( nodeNameBinary );
      newIndexBuffer.putInt( rangeClassNameBytes.length );
      newIndexBuffer.put( rangeClassNameBytes );
      newIndexBuffer.putInt( indexBinary.length );
      newIndexBuffer.put( indexBinary );

      if ( isEncript == 1 ) {
        String keyName = encryptionSettingNode.getKeyName();
        IEncryptor encryptor = factory.createEncryptor(
            keyStore.getKey( keyName ),
            Module.COLUMN_INDEX,
            aad );
        byte[] encriptBinary = encryptor.encrypt( newIndexBinary , 0 , newIndexBinary.length );
        wrapBuffer.putInt( encriptBinary.length );
        wrapBuffer.put( encriptBinary );
        aad.nextOrdinal();
      } else {
        wrapBuffer.putInt( newIndexBinary.length );
        wrapBuffer.put( newIndexBinary );
      }
    }
    int childCount = 0;
    for ( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ) {
      String childKeyName = entry.getKey();
      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( childKeyName );
      EncryptionSupportedBlockIndexNode childBlockIndexNode =
          (EncryptionSupportedBlockIndexNode)( entry.getValue() );

      int childEndOffset = childBlockIndexNode.toBinary(
          childKeyName ,
          buffer ,
          wrapBuffer.position() + Integer.BYTES ,
          aad ,
          keyStore ,
          factory ,
          childEncryptionSettingNode );
      if ( ( childEndOffset - Integer.BYTES ) != wrapBuffer.position() ) {
        wrapBuffer.putInt( childEndOffset - wrapBuffer.position() - Integer.BYTES );
        wrapBuffer.position( childEndOffset );
        childCount++;
      } 
    }
    wrapBuffer.putInt( childCountOffset , childCount );
    return wrapBuffer.position();
  }

  private static IBlockIndex createBlockIndexFromByteBuffer(
      final ByteBuffer decriptBuffer ,
      final EncryptionSupportedBlockIndexNode node ) throws IOException {
    int nodeNameBinaryLength = decriptBuffer.getInt();
    String nodeName = new String(
        decriptBuffer.array() , decriptBuffer.position() , nodeNameBinaryLength , "UTF-8" );
    node.setNodeName( nodeName );
    decriptBuffer.position( decriptBuffer.position() + nodeNameBinaryLength );

    int blockIndexClassNameBinaryLength = decriptBuffer.getInt();
    String blockIndexClassName = new String(
        decriptBuffer.array() ,
        decriptBuffer.position() ,
        blockIndexClassNameBinaryLength ,
        "UTF-8" );
    decriptBuffer.position( decriptBuffer.position() + blockIndexClassNameBinaryLength );
    IBlockIndex blockIndex = FindBlockIndex.get(
        RangeBlockIndexNameShortCut.getClassName( blockIndexClassName ) );
    int indexBinaryLength = decriptBuffer.getInt();
    blockIndex.setFromBinary(
        decriptBuffer.array() , decriptBuffer.position() , indexBinaryLength );
    return blockIndex;
  }

  /**
   * Create BlockIndexNode.
   */
  public static EncryptionSupportedBlockIndexNode createFromBinary(
      final byte[] buffer,
      final int start,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ) throws IOException {
    EncryptionSupportedBlockIndexNode result = new EncryptionSupportedBlockIndexNode();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    wrapBuffer.position( start );

    boolean isEncript = wrapBuffer.get() == 1 ? true : false;
    int keyNameBinarySize = wrapBuffer.getInt();
    String keyName = new String( buffer , wrapBuffer.position() , keyNameBinarySize , "UTF-8" );
    wrapBuffer.position( wrapBuffer.position() + keyNameBinarySize );

    int childCount = wrapBuffer.getInt();

    int blockIndexLength = wrapBuffer.getInt();
    if ( blockIndexLength != 0 ) {
      IBlockIndex blockIndex;
      if ( isEncript ) {
        IEncryptor encryptor = factory.createEncryptor(
            keyStore.getKey( keyName ),
            Module.COLUMN_INDEX,
            aad );
        try {
          byte[] plainText = encryptor.decrypt(
              buffer , wrapBuffer.position() , blockIndexLength );
          ByteBuffer decriptBuffer = ByteBuffer.wrap( plainText );
          blockIndex = createBlockIndexFromByteBuffer( decriptBuffer , result );
        } catch ( IOException ex ) {
          blockIndex = null;
        } finally {
          aad.nextOrdinal();
        }
      } else {
        ByteBuffer decriptBuffer = ByteBuffer.wrap(
            buffer , wrapBuffer.position() , blockIndexLength );
        blockIndex = createBlockIndexFromByteBuffer( decriptBuffer , result );
      }

      result.setBlockIndex( blockIndex );
    }
    wrapBuffer.position( wrapBuffer.position() + blockIndexLength );
    for ( int i = 0 ; i < childCount ; i++ ) {
      int childBinaryLength = wrapBuffer.getInt();

      EncryptionSupportedBlockIndexNode childNode = createFromBinary(
          buffer , wrapBuffer.position() , aad , keyStore , factory );
      if ( childNode.getNodeName() != null ) {
        result.putChildNode( childNode.getNodeName() , childNode );
      }
      wrapBuffer.position( wrapBuffer.position() + childBinaryLength );
    }
    return result;
  }

}
