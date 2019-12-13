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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BlockIndexNode {

  protected Map<String,BlockIndexNode> childContainer;
  protected IBlockIndex blockIndex;
  protected boolean isDisable;

  public BlockIndexNode() {
    childContainer = new HashMap<String,BlockIndexNode>();
  }

  @Override
  public BlockIndexNode clone() {
    BlockIndexNode result = new BlockIndexNode();
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

  /**
   * Set the Index of block to this Node.
   */
  public void setBlockIndex( final IBlockIndex blockIndex ) {
    if ( isDisable ) {
      return;
    }
    if ( this.blockIndex == null ) {
      this.blockIndex = blockIndex;
    } else {
      if ( ! this.blockIndex.merge( blockIndex ) ) {
        disable();
      }
    }
  }

  /**
   * Invalidate Index of this Node.
   */
  public void disable() {
    childContainer.clear();
    blockIndex = null;
    isDisable = true;
  }

  /**
   * Get the IBlockIndex of this Node.
   */
  public IBlockIndex getBlockIndex() {
    if ( blockIndex == null ) {
      return UnsupportedBlockIndex.INSTANCE;
    }
    return blockIndex;
  }

  /**
   * Get the child 's BlockIndexNode from the column name.
   */
  public BlockIndexNode getChildNode( final String nodeName ) {
    if ( ! childContainer.containsKey( nodeName ) ) {
      childContainer.put( nodeName , new BlockIndexNode() );
    }
    return childContainer.get( nodeName );
  }

  /**
   * Remove child node.
   */
  public void deleteChildNode( final String nodeName ) {
    if ( childContainer.containsKey( nodeName ) ) {
      childContainer.remove( nodeName );
    }
  }

  public boolean containsKey( final String nodeName ) {
    return childContainer.containsKey( nodeName );
  }

  public void putChildNode( final String nodeName , final BlockIndexNode node ) {
    childContainer.put( nodeName , node );
  }

  /**
   * Get the size of the converted byte array.
   */
  public int getBinarySize() throws IOException {
    if ( isDisable ) {
      return 0;
    }
    int length = 0;
    length += Integer.BYTES;
    if ( blockIndex != null ) {
      length += Integer.BYTES;
      length += RangeBlockIndexNameShortCut.getShortCutName(
          blockIndex.getClass().getName() ).getBytes( "UTF-8" ).length;
      length += Integer.BYTES;
      length += blockIndex.getBinarySize();
    }
    length += Integer.BYTES;
    for ( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ) {
      int childLength = entry.getValue().getBinarySize();
      if ( childLength != 0 ) {
        length += Integer.BYTES;
        length += Integer.BYTES;
        length += entry.getKey().getBytes( "UTF-8" ).length;
        length += Integer.BYTES;
        length += childLength;
      }
    }
    return length;
  }

  /**
   * Convert this object to a byte array.
   */
  public int toBinary( final byte[] buffer , final int start ) throws IOException {
    if ( isDisable ) {
      return start;
    }
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    if ( blockIndex == null ) {
      wrapBuffer.putInt( offset , 0 );
      offset += Integer.BYTES;
    } else {
      wrapBuffer.putInt( offset , 1 );
      offset += Integer.BYTES;
      byte[] rangeClassNameBytes = RangeBlockIndexNameShortCut.getShortCutName(
          blockIndex.getClass().getName() ).getBytes( "UTF-8" );
      wrapBuffer.putInt( offset , rangeClassNameBytes.length );
      offset += Integer.BYTES;
      wrapBuffer.position( offset );
      wrapBuffer.put( rangeClassNameBytes );
      offset += rangeClassNameBytes.length;
      byte[] indexBinary = blockIndex.toBinary();
      wrapBuffer.putInt( offset , indexBinary.length );
      offset += Integer.BYTES;
      wrapBuffer.position( offset );
      wrapBuffer.put( indexBinary );
      offset += indexBinary.length;
    }
    int childCountOffset = offset;
    int childCount = 0;
    offset += Integer.BYTES;
    for ( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ) {
      byte[] childKeyNameBytes = entry.getKey().getBytes( "UTF-8" );
      int childBinaryLengthOffset = offset;
      offset += Integer.BYTES;
      int childKeyNameLengthOffset = offset;
      offset += Integer.BYTES;
      int childKeyNameOffset = offset;
      offset += childKeyNameBytes.length;
      int childEndOffset = entry.getValue().toBinary( buffer , offset );
      if ( childEndOffset == offset ) {
        offset = childBinaryLengthOffset;
      } else {
        wrapBuffer.putInt( childBinaryLengthOffset , childEndOffset - offset );
        wrapBuffer.putInt( childKeyNameLengthOffset , childKeyNameBytes.length );
        wrapBuffer.position( childKeyNameOffset );
        wrapBuffer.put( childKeyNameBytes );
        childCount++;
        offset = childEndOffset;
      }
    }
    wrapBuffer.putInt( childCountOffset , childCount );
    return offset;
  }

  /**
   * Clear internal data.
   */
  public void clear() {
    childContainer.clear();
    blockIndex = null;
    isDisable = false;
  }

  /**
   * Creates its own object from the byte array.
   */
  public static BlockIndexNode createFromBinary(
      final byte[] buffer , final int start ) throws IOException {
    BlockIndexNode result = new BlockIndexNode();
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    int currentBlockIndexExists = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    if ( currentBlockIndexExists == 1 ) {
      int classNameLength = wrapBuffer.getInt( offset );
      offset += Integer.BYTES;
      byte[] classNameBytes = new byte[ classNameLength ];
      wrapBuffer.position( offset );
      wrapBuffer.get( classNameBytes , 0 , classNameLength );
      offset += classNameLength;
      int indexBinaryLength = wrapBuffer.getInt( offset );
      offset += Integer.BYTES;
      byte[] indexBinary = new byte[indexBinaryLength];
      wrapBuffer.position( offset );
      wrapBuffer.get( indexBinary , 0 , indexBinaryLength );
      offset += indexBinaryLength;
      IBlockIndex blockIndex = FindBlockIndex.get(
          RangeBlockIndexNameShortCut.getClassName( new String( classNameBytes , "UTF-8" ) ) );
      blockIndex.setFromBinary( indexBinary , 0 , indexBinary.length );
      result.setBlockIndex( blockIndex );
    }
    int childCount = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    for ( int i = 0 ; i < childCount ; i++ ) {
      wrapBuffer.position( offset );
      int childBinaryLength = wrapBuffer.getInt();
      int childNameLength = wrapBuffer.getInt();
      byte[] childNameBytes = new byte[childNameLength];
      wrapBuffer.get( childNameBytes , 0 , childNameBytes.length );
      BlockIndexNode childNode = createFromBinary( buffer , wrapBuffer.position() );
      result.putChildNode( new String( childNameBytes , "UTF-8" ) , childNode );
      offset = wrapBuffer.position() + childBinaryLength;
    }
    return result;
  }

}
