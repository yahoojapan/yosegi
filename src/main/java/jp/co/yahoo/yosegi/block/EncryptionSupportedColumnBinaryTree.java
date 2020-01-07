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
import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionSettingNode;
import jp.co.yahoo.yosegi.encryptor.EncryptorFactoryNameShortCut;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.keystore.KeyStore;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ColumnTypeFactory;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EncryptionSupportedColumnBinaryTree {

  private final List<ColumnBinary> currentColumnBinaryList = new ArrayList<ColumnBinary>();
  private final Map<String,EncryptionSupportedColumnBinaryTree> childTreeMap =
      new HashMap<String,EncryptionSupportedColumnBinaryTree>();
  private final List<String> childKeyList = new ArrayList();
  private final List<EncryptionSupportedBlockReadOffset> blockReadOffsetList =
      new ArrayList<EncryptionSupportedBlockReadOffset>();

  private ColumnNameNode columnNameNode;
  private String nodeName;
  private int currentCount;
  private int childCount;
  private int metaLength;
  private int allBinaryStart;

  public EncryptionSupportedColumnBinaryTree() {
    columnNameNode = new ColumnNameNode( "root" );
    columnNameNode.setNeedAllChild( true );
  }

  public ColumnBinary getColumnBinary( final int index ) {
    return currentColumnBinaryList.get( index );
  }

  /**
   * Gets a columns of the child of the specified index.
   */
  public List<ColumnBinary> getChildColumnBinary( final int index ) {
    List<ColumnBinary> result = new ArrayList<ColumnBinary>();
    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      ColumnBinary childColumnBinary = entry.getValue().getColumnBinary( index );
      if ( childColumnBinary != null ) {
        result.add( childColumnBinary );
      }
    }
    return result;
  }

  /**
   * Add column data to this Node.
   */
  public void add( final ColumnBinary columnBinary ) throws IOException {
    currentCount++;
    currentColumnBinaryList.add( columnBinary );
    if ( columnBinary == null ) {
      addChild( null );
    } else {
      metaLength += columnBinary.getMetaSize();
      addChild( columnBinary.columnBinaryList );
    }
  }

  /**
   * Add a child columns.
   */
  public void addChild( final List<ColumnBinary> columnBinaryList ) throws IOException {
    childCount++;
    if ( columnBinaryList != null ) {
      for ( ColumnBinary childColumnBinary : columnBinaryList ) {
        EncryptionSupportedColumnBinaryTree childTree =
            childTreeMap.get( childColumnBinary.columnName );
        if ( childTree == null ) {
          childTree = new EncryptionSupportedColumnBinaryTree();
          while ( childTree.size() < ( getChildSize() - 1 ) ) {
            childTree.add( null );
          }
          childTreeMap.put( childColumnBinary.columnName , childTree );
          childKeyList.add( childColumnBinary.columnName );
        }
        childTree.add( childColumnBinary );
      }
    }

    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      EncryptionSupportedColumnBinaryTree childTree = entry.getValue();
      while ( childTree.size() < getChildSize() ) {
        childTree.add( null );
      }
    }
  }

  public int size() {
    return currentCount;
  }

  public int getChildSize() {
    return childCount;
  }

  public String getNodeName() {
    return nodeName;
  }

  /**
   * Obtain the offset that needs to be read.
   */
  public List<EncryptionSupportedBlockReadOffset> getBlockReadOffset() {
    List<EncryptionSupportedBlockReadOffset> result =
        new ArrayList<EncryptionSupportedBlockReadOffset>();
    result.addAll( blockReadOffsetList );
    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      result.addAll( entry.getValue().getBlockReadOffset() );
    }

    return result;
  }

  /**
   * Set a filter to determine if it is necessary to read column data.
   */
  public void setColumnFilter( final ColumnNameNode columnNameNode ) {
    if ( columnNameNode == null ) {
      return;
    }
    this.columnNameNode = columnNameNode;
  }

  /**
   * Set column data from meta byte array.
   */
  public int toColumnBinaryTree(
      final byte[] metaBinary ,
      final int start ,
      final Set<Integer> spreadIndexDict ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ) throws IOException {
    return toColumnBinaryTree(
        metaBinary ,
        start ,
        columnNameNode.isNeedAllChild() ,
        spreadIndexDict ,
        aad ,
        keyStore ,
        factory );
  }

  /**
   * Set column data from meta byte array.
   */
  public int toColumnBinaryTree(
      final byte[] metaBinary ,
      final int start ,
      final boolean isNeedAllChild ,
      final Set<Integer> spreadIndexDict ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap( metaBinary );
    byteBuffer.position( start );
    int childSize =  byteBuffer.getInt();
    int arraySize =  byteBuffer.getInt();
    for ( int i = 0 ; i < childSize ; i++ ) {
      EncryptionSupportedColumnBinaryTree childColumnBinary =
          new EncryptionSupportedColumnBinaryTree();
      boolean isAppend = true;
      byteBuffer.position( childColumnBinary.toColumnBinaryTree(
          metaBinary ,
          byteBuffer.position() ,
          spreadIndexDict ,
          aad ,
          keyStore ,
          factory ) );
      if ( isNeedAllChild ) {
        isAppend = true;
      } else {
        if ( columnNameNode.containsChild( childColumnBinary.getNodeName() ) ) {
          childColumnBinary.setColumnFilter(
              columnNameNode.getChild( childColumnBinary.getNodeName() ) );
          isAppend = true;
        } else if ( ColumnTypeFactory.getColumnTypeFromName(
            childColumnBinary.getNodeName() ) == ColumnType.ARRAY ) {
          // array column childName is "ARRAY"
          childColumnBinary.setColumnFilter( columnNameNode );
          isAppend = true;
        } else {
          ColumnNameNode childColumnNameNode =
              new ColumnNameNode( childColumnBinary.getNodeName() , true );
          childColumnNameNode.setNeedAllChild( false );
          childColumnBinary.setColumnFilter( childColumnNameNode );
          isAppend = false;
        }
      }
      if ( childCount < childColumnBinary.size() ) {
        childCount = childColumnBinary.size();
      }
      if ( isAppend ) {
        childTreeMap.put( childColumnBinary.getNodeName() , childColumnBinary );
        childKeyList.add( childColumnBinary.getNodeName() );
      }
    }

    boolean isEncrypt = byteBuffer.get() == 1 ? true : false;
    int keyNameLength = byteBuffer.getInt();
    String keyName = new String( byteBuffer.array() , byteBuffer.position() , keyNameLength );
    byteBuffer.position( byteBuffer.position() + keyNameLength );
    int encriptBinaryLength = byteBuffer.getInt();

    ByteBuffer currentBuffer = ByteBuffer.wrap( byteBuffer.array() );
    currentBuffer.position( byteBuffer.position() );
    if ( isEncrypt ) {
      IEncryptor encryptor = factory.createEncryptor(
          keyStore.getKey( keyName ),
          Module.COLUMN_META,
          aad );
      try {
        currentBuffer = ByteBuffer.wrap( encryptor.decrypt(
            byteBuffer.array() ,
            byteBuffer.position() ,
            encriptBinaryLength ) );
      } catch ( IOException ex ) {
        for ( int i = 0 ; i < arraySize ; i++ ) {
          currentColumnBinaryList.add( null );
        }
        return byteBuffer.position() + encriptBinaryLength;
      } finally {
        aad.nextOrdinal();
      }
    }
    byteBuffer.position( byteBuffer.position() + encriptBinaryLength );

    int nodeNameLength = currentBuffer.getInt();
    nodeName = new String( currentBuffer.array() , currentBuffer.position() , nodeNameLength );
    currentBuffer.position( currentBuffer.position() + nodeNameLength );
    allBinaryStart = currentBuffer.getInt();
    int allBinaryLength =  currentBuffer.getInt();
    int currentMetaBinaryLength =  currentBuffer.getInt();
    if ( currentMetaBinaryLength != 0 ) {
      byte[] childBuffer = null;
      int childStartDataOffset = 0;
      if ( ! columnNameNode.isDisable() ) {
        childBuffer = new byte[allBinaryLength];
      }
      for ( int startOffset = currentBuffer.position() ;
          currentBuffer.position() < startOffset + currentMetaBinaryLength ; ) {
        int index = currentBuffer.getInt();
        int metaBinaryLength = currentBuffer.getInt();
        int encryptBinaryLength = currentBuffer.getInt();
        if ( columnNameNode.isDisable() || metaBinaryLength == 0 ) {
          currentColumnBinaryList.add( null );
        } else {
          List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
          for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry :
              childTreeMap.entrySet() ) {
            ColumnBinary childColumnBinary = entry.getValue().getColumnBinary( index );
            if ( childColumnBinary != null ) {
              childList.add( childColumnBinary );
            }
          }
          ColumnBinary childColumnBinary =
              ColumnBinary.newInstanceFromMetaBinary(
                currentBuffer.array() ,
                currentBuffer.position() ,
                metaBinaryLength ,
                childBuffer ,
                childList );
          if ( spreadIndexDict == null
              || spreadIndexDict.contains( Integer.valueOf( currentCount ) ) ) {
            if ( allBinaryLength != 0 ) {
              blockReadOffsetList.add( new EncryptionSupportedBlockReadOffset(
                  childColumnBinary.binaryStart ,
                  encryptBinaryLength ,
                  childStartDataOffset ,
                  childBuffer ,
                  isEncrypt ,
                  keyName ) );
            }
            currentColumnBinaryList.add( childColumnBinary );
          } else {
            currentColumnBinaryList.add( null );
          }
          childColumnBinary.binaryStart = childStartDataOffset;
          childStartDataOffset += childColumnBinary.binaryLength;
        }
        currentBuffer.position( currentBuffer.position() + metaBinaryLength );
        currentCount++;
      }
      if ( ! isEncrypt && allBinaryLength != 0 && currentCount == blockReadOffsetList.size() ) {
        blockReadOffsetList.clear();
        blockReadOffsetList.add( new EncryptionSupportedBlockReadOffset(
            allBinaryStart , allBinaryLength , 0 , childBuffer , isEncrypt , keyName ) );
      }
    }

    return byteBuffer.position();
  }

  /**
   * Convert the column data held by this object into a byte array
   * and assign it to the byte buffer of the argument.
   */
  public int createMeta(
      final String nodeName ,
      final ByteArrayData metaBuffer ,
      final int dataStartOffset ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    metaBuffer.append( ByteBuffer.allocate( Integer.BYTES ).putInt( childTreeMap.size() ).array() );
    metaBuffer.append(
        ByteBuffer.allocate( Integer.BYTES ).putInt( currentColumnBinaryList.size() ).array() );

    byte isEncrypt = encryptionSettingNode.isEncryptNode() ? (byte)1 : (byte)0;
    String keyName = new String();
    if ( isEncrypt != 0 ) {
      keyName = encryptionSettingNode.getKeyName();
    }

    int binaryOffset = dataStartOffset;
    byte[] nodeNameBytes = nodeName.getBytes( "UTF-8" );
    byte[] binaryOffsetMetaData;
    if ( currentColumnBinaryList.isEmpty() ) {
      binaryOffsetMetaData = new byte[Integer.BYTES * 4 + nodeNameBytes.length];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryOffsetMetaData );
      wrapBuffer.putInt( nodeNameBytes.length );
      wrapBuffer.put( nodeNameBytes );
      wrapBuffer.putInt( 0 );
      wrapBuffer.putInt( 0 );
      wrapBuffer.putInt( 0 );
    } else  {
      byte[] currentMetaBinary =
          new byte[ ( Integer.BYTES * 3 ) * currentColumnBinaryList.size() + metaLength ];
      ByteBuffer wrapMetaBinaryBuffer = ByteBuffer.wrap( currentMetaBinary );

      allBinaryStart = binaryOffset;
      for ( int i = 0 ; i < currentColumnBinaryList.size() ; i++ ) {
        ColumnBinary columnBinary = currentColumnBinaryList.get(i);
        wrapMetaBinaryBuffer.putInt( i );
        if ( columnBinary == null ) {
          wrapMetaBinaryBuffer.putInt( 0 );
          wrapMetaBinaryBuffer.putInt( 0 );
        } else {
          int binaryStart = binaryOffset;
          int originalBinaryStart = columnBinary.binaryStart;
          columnBinary.binaryStart = binaryStart;
          byte[] metaBinary = columnBinary.toMetaBinary();
          columnBinary.binaryStart = originalBinaryStart;
          wrapMetaBinaryBuffer.putInt( metaBinary.length );
          if ( isEncrypt == 1 ) {
            int encryptLength = Integer.BYTES * 2 + factory.getEncryptSize(
                columnBinary.binaryLength , Module.COLUMN_DATA , aad );
            wrapMetaBinaryBuffer.putInt( encryptLength );
            binaryOffset += encryptLength;
          } else {
            wrapMetaBinaryBuffer.putInt( columnBinary.binaryLength );
            binaryOffset += columnBinary.binaryLength;
          }
          
          wrapMetaBinaryBuffer.put( metaBinary );
        }
      }
      binaryOffsetMetaData =
          new byte[ Integer.BYTES * 4 + nodeNameBytes.length + currentMetaBinary.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryOffsetMetaData );
      wrapBuffer.putInt( nodeNameBytes.length );
      wrapBuffer.put( nodeNameBytes );
      wrapBuffer.putInt( allBinaryStart );
      wrapBuffer.putInt( binaryOffset - allBinaryStart );
      wrapBuffer.putInt( currentMetaBinary.length );
      System.arraycopy(
          currentMetaBinary ,
          0 ,
          binaryOffsetMetaData ,
          wrapBuffer.position() ,
          currentMetaBinary.length );
    }

    for ( String columnName : childKeyList ) {
      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( columnName );
      if ( ( ColumnTypeFactory.getColumnTypeFromName( columnName ) == ColumnType.ARRAY ) ) {
        childEncryptionSettingNode = encryptionSettingNode;
      }
      EncryptionSupportedColumnBinaryTree childTree = childTreeMap.get( columnName );
      binaryOffset = childTree.createMeta(
          columnName ,
          metaBuffer ,
          binaryOffset ,
          aad ,
          keyStore ,
          factory ,
          childEncryptionSettingNode );
    }

    if ( isEncrypt == 1 ) {
      IEncryptor encryptor = factory.createEncryptor(
          keyStore.getKey( keyName ),
          Module.COLUMN_META,
          aad );
      binaryOffsetMetaData =
          encryptor.encrypt( binaryOffsetMetaData , 0 , binaryOffsetMetaData.length );
      aad.nextOrdinal();
    }

    byte[] keyNameBinary = keyName.getBytes();
    ByteBuffer encriptBinary = ByteBuffer.allocate(
        Byte.BYTES + Integer.BYTES * 2 + keyNameBinary.length );
    encriptBinary.put( isEncrypt );
    encriptBinary.putInt( keyNameBinary.length );
    encriptBinary.put( keyNameBinary );
    encriptBinary.putInt( binaryOffsetMetaData.length );

    metaBuffer.append( encriptBinary.array() );
    metaBuffer.append( binaryOffsetMetaData );

    return binaryOffset;
  }

  /**
   * Get current metadata size.
   */
  public int metaSize(
      final String nodeName,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    int result = 0;
    result += Integer.BYTES * 2;

    if ( encryptionSettingNode.isEncryptNode() ) {
      result += encryptionSettingNode.getKeyName().getBytes().length;
    }

    byte[] nodeNameBytes = nodeName.getBytes( "UTF-8" );

    int binaryOffsetMetaDataLength;
    if ( currentColumnBinaryList.isEmpty() ) {
      binaryOffsetMetaDataLength = Integer.BYTES * 4 + nodeNameBytes.length;
    } else {
      int binaryLength = ( Integer.BYTES * 3 ) * currentColumnBinaryList.size() + metaLength;
      binaryOffsetMetaDataLength = Integer.BYTES * 4 + nodeNameBytes.length + binaryLength;
      
    }

    for ( String columnName : childKeyList ) {
      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( columnName );
      if ( ( ColumnTypeFactory.getColumnTypeFromName( columnName ) == ColumnType.ARRAY ) ) {
        childEncryptionSettingNode = encryptionSettingNode;
      }
      EncryptionSupportedColumnBinaryTree childTree = childTreeMap.get( columnName );
      result += childTree.metaSize(
          columnName , aad ,  keyStore , factory , childEncryptionSettingNode );
    }

    if ( encryptionSettingNode.isEncryptNode() ) {
      result += factory.getEncryptSize(
          binaryOffsetMetaDataLength,
          Module.COLUMN_META,
          aad );
    } else {
      result += binaryOffsetMetaDataLength;
    }

    result += Byte.BYTES + Integer.BYTES * 2;

    return result;
  }

  private int metaSizeAfterAppendFromColumnBinary(
      final ColumnBinary columnBinary,
      final String nodeName,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    int result = 0;
    result += Integer.BYTES * 2;
    if ( encryptionSettingNode.isEncryptNode() ) {
      result += encryptionSettingNode.getKeyName().getBytes().length;
    }

    byte[] nodeNameBytes = nodeName.getBytes( "UTF-8" );
    int binaryLength =
          ( Integer.BYTES * 3 ) * ( currentColumnBinaryList.size() + 1 ) + metaLength;
    int binaryOffsetMetaDataLength = Integer.BYTES * 4 + nodeNameBytes.length + binaryLength;

    Set<String> childKeySet = new HashSet<String>();
    if ( columnBinary != null ) {
      binaryOffsetMetaDataLength += columnBinary.getMetaSize();
      if ( columnBinary.columnBinaryList != null ) {
        for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
          EncryptionSupportedColumnBinaryTree childTree =
              childTreeMap.get( childColumnBinary.columnName );
          if ( childTree == null ) {
            childTree = new EncryptionSupportedColumnBinaryTree();
            while ( childTree.size() < ( getChildSize() ) ) {
              childTree.add( null );
            }
          }

          EncryptionSettingNode childEncryptionSettingNode =
              encryptionSettingNode.getChildNode( childColumnBinary.columnName );
          if ( ( ColumnTypeFactory.getColumnTypeFromName( childColumnBinary.columnName )
              == ColumnType.ARRAY ) ) {
            childEncryptionSettingNode = encryptionSettingNode;
          }
          result += childTree.metaSizeAfterAppendFromColumnBinary(
              childColumnBinary ,
              childColumnBinary.columnName ,
              aad ,
              keyStore ,
              factory ,
              childEncryptionSettingNode );
          childKeySet.add( childColumnBinary.columnName );
        }
      }
    }

    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      if ( ! childKeySet.contains( entry.getKey() ) ) {
        EncryptionSettingNode childEncryptionSettingNode =
            encryptionSettingNode.getChildNode( entry.getKey() );
        if ( ( ColumnTypeFactory.getColumnTypeFromName( entry.getKey() ) == ColumnType.ARRAY ) ) {
          childEncryptionSettingNode = encryptionSettingNode;
        }
        result += entry.getValue().metaSizeAfterAppendFromColumnBinary(
            null,
            entry.getKey() ,
            aad ,
            keyStore ,
            factory ,
            childEncryptionSettingNode );
      }
    }

    if ( encryptionSettingNode.isEncryptNode() ) {
      result += factory.getEncryptSize(
          binaryOffsetMetaDataLength,
          Module.COLUMN_META,
          aad );
    } else {
      result += binaryOffsetMetaDataLength;
    }

    result += Byte.BYTES + Integer.BYTES * 2;

    return result;
  }

  /**
   * append size.
   */
  public int metaSizeAfterAppend(
      final List<ColumnBinary> rootBinaryList,
      final String nodeName,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    int result = 0;
    result += Integer.BYTES * 2;
    byte[] nodeNameBytes = nodeName.getBytes( "UTF-8" );
    result += Integer.BYTES * 4 + nodeNameBytes.length;

    Set<String> childKeySet = new HashSet<String>();
    for ( ColumnBinary childColumnBinary : rootBinaryList ) {
      EncryptionSupportedColumnBinaryTree childTree =
          childTreeMap.get( childColumnBinary.columnName );
      if ( childTree == null ) {
        childTree = new EncryptionSupportedColumnBinaryTree();
        while ( childTree.size() < ( getChildSize() ) ) {
          childTree.add( null );
        }
      }

      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( childColumnBinary.columnName );
      if ( ( ColumnTypeFactory.getColumnTypeFromName( childColumnBinary.columnName )
          == ColumnType.ARRAY ) ) {
        childEncryptionSettingNode = encryptionSettingNode;
      }
      result += childTree.metaSizeAfterAppendFromColumnBinary(
          childColumnBinary ,
          childColumnBinary.columnName , 
          aad ,
          keyStore ,
          factory ,
          childEncryptionSettingNode );
      childKeySet.add( childColumnBinary.columnName );
    }

    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      if ( ! childKeySet.contains( entry.getKey() ) ) {
        EncryptionSettingNode childEncryptionSettingNode =
            encryptionSettingNode.getChildNode( entry.getKey() );
        if ( ( ColumnTypeFactory.getColumnTypeFromName( entry.getKey() ) == ColumnType.ARRAY ) ) {
          childEncryptionSettingNode = encryptionSettingNode;
        }
        result += entry.getValue().metaSizeAfterAppendFromColumnBinary(
            null,
            entry.getKey() ,
            aad ,
            keyStore ,
            factory ,
            childEncryptionSettingNode );
      }
    }
    
    result += Byte.BYTES + Integer.BYTES * 2;

    return result;
  }

  /**
   * Data size after append.
   */
  public int dataSizeAfterAppend(
      final List<ColumnBinary> rootBinaryList ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    int result = dataSize( aad , keyStore , factory , encryptionSettingNode );
    for ( ColumnBinary childColumnBinary : rootBinaryList ) {
      EncryptionSupportedColumnBinaryTree childTree =
          childTreeMap.get( childColumnBinary.columnName );
      if ( childTree == null ) {
        childTree = new EncryptionSupportedColumnBinaryTree();
        while ( childTree.size() < ( getChildSize() ) ) {
          childTree.add( null );
        }
        EncryptionSettingNode childEncryptionSettingNode =
            encryptionSettingNode.getChildNode( childColumnBinary.columnName );
        if ( ( ColumnTypeFactory.getColumnTypeFromName( childColumnBinary.columnName )
            == ColumnType.ARRAY ) ) {
          childEncryptionSettingNode = encryptionSettingNode;
        }
        result += childTree.dataSizeAfterAppend(
            childColumnBinary , aad , keyStore , factory , childEncryptionSettingNode );
      }
    }
    return result;
  }

  /**
   * Data size after append.
   */
  public int dataSizeAfterAppend(
      final ColumnBinary columnBinary ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {
    int result = 0;
    if ( encryptionSettingNode.isEncryptNode() ) {
      result += factory.getEncryptSize(
          columnBinary.binaryLength , Module.COLUMN_DATA , aad ) + Integer.BYTES * 2;
    } else {
      result += columnBinary.binaryLength;
    }
    if ( columnBinary.columnBinaryList == null ) {
      return result;
    }
    for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
      EncryptionSupportedColumnBinaryTree childTree =
          childTreeMap.get( childColumnBinary.columnName );
      if ( childTree == null ) {
        childTree = new EncryptionSupportedColumnBinaryTree();
        while ( childTree.size() < ( getChildSize() ) ) {
          childTree.add( null );
        }
        EncryptionSettingNode childEncryptionSettingNode =
            encryptionSettingNode.getChildNode( childColumnBinary.columnName );
        if ( ( ColumnTypeFactory.getColumnTypeFromName( childColumnBinary.columnName )
            == ColumnType.ARRAY ) ) {
          childEncryptionSettingNode = encryptionSettingNode;
        }
        result += childTree.dataSizeAfterAppend(
            childColumnBinary , aad , keyStore , factory , childEncryptionSettingNode );
      }
    }
    return result;
  }

  /**
   * Data size.
   */
  public int dataSize(
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ,
      final EncryptionSettingNode encryptionSettingNode ) {
    int result = 0;
    for ( int i = 0 ; i < currentColumnBinaryList.size() ; i++ ) {
      ColumnBinary columnBinary = currentColumnBinaryList.get(i);
      if ( columnBinary == null ) {
        continue;
      }
      if ( encryptionSettingNode.isEncryptNode() ) {
        result += factory.getEncryptSize(
            columnBinary.binaryLength , Module.COLUMN_DATA , aad ) + Integer.BYTES * 2;
      } else {
        result += columnBinary.binaryLength;
      }
    }
    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( entry.getKey() );
      if ( ( ColumnTypeFactory.getColumnTypeFromName( entry.getKey() ) == ColumnType.ARRAY ) ) {
        childEncryptionSettingNode = encryptionSettingNode;
      }
      result += entry.getValue().dataSize( aad , keyStore , factory , childEncryptionSettingNode );
    }
    return result;
  }

  /**
   * Write data.
   */
  public int writeData(
      final OutputStream out ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ,
      final EncryptionSettingNode encryptionSettingNode ) throws IOException {

    byte isEncrypt = encryptionSettingNode.isEncryptNode() ? (byte)1 : (byte)0;
    String keyName = new String();
    if ( isEncrypt != 0 ) {
      keyName = encryptionSettingNode.getKeyName();
    }

    int writeDataSize = 0;
    if ( ! currentColumnBinaryList.isEmpty() ) {
      for ( int i = 0 ; i < currentColumnBinaryList.size() ; i++ ) {
        ColumnBinary columnBinary = currentColumnBinaryList.get(i);
        if ( columnBinary == null ) {
          continue;
        }
        if ( isEncrypt == 1 ) {
          byte[] metaData = new byte[Integer.BYTES * 2];
          ByteBuffer metaBuffer = ByteBuffer.wrap( metaData );
          metaBuffer.putInt( aad.getOrdinal() );

          IEncryptor encryptor = factory.createEncryptor(
              keyStore.getKey( keyName ),
              Module.COLUMN_DATA,
              aad );
          byte[] data = encryptor.encrypt(
              columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
          aad.nextOrdinal();

          metaBuffer.putInt( data.length );
          out.write( metaData );
          out.write( data );
          writeDataSize += metaData.length + data.length;
        } else {
          out.write( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
          writeDataSize += columnBinary.binaryLength;
        }
      }
    }
    for ( String columnName : childKeyList ) {
      EncryptionSettingNode childEncryptionSettingNode =
          encryptionSettingNode.getChildNode( columnName );
      if ( ( ColumnTypeFactory.getColumnTypeFromName( columnName ) == ColumnType.ARRAY ) ) {
        childEncryptionSettingNode = encryptionSettingNode;
      }
      EncryptionSupportedColumnBinaryTree childTree = childTreeMap.get( columnName );
      writeDataSize += childTree.writeData(
          out , aad , keyStore , factory , childEncryptionSettingNode );
    }
    return writeDataSize;
  }

  /**
   * Clear column data.
   */
  public void clear() {
    for ( Map.Entry<String,EncryptionSupportedColumnBinaryTree> entry : childTreeMap.entrySet() ) {
      entry.getValue().clear();
    }
    currentColumnBinaryList.clear();
    childTreeMap.clear();
    childKeyList.clear();
    columnNameNode = null;
    blockReadOffsetList.clear();
    currentCount = 0;
    childCount = 0;
    metaLength = 0;
  }

}
