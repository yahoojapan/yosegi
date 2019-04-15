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

import jp.co.yahoo.yosegi.blockindex.BlockIndexType;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class FullRangeBlockIndex implements IBlockIndex {
  private interface DispatchedFunc extends Supplier<IBlockIndex> {}

  private static Set<BlockIndexType> fullRangeBlockIndexList;
  private static EnumDispatcherFactory.Func<BlockIndexType, Byte> getTypeToByteDispatcher;
  private static SwitchDispatcherFactory.Func<Byte, DispatchedFunc> getByteToBlockIndexDispacher;

  static {
    EnumDispatcherFactory<BlockIndexType, Byte> getTypeToByteDispatcherFactory =
        new EnumDispatcherFactory<>(BlockIndexType.class);
    getTypeToByteDispatcher = getTypeToByteDispatcherFactory
        .set(BlockIndexType.RANGE_STRING,  (byte)0)
        .set(BlockIndexType.RANGE_BYTE,    (byte)1)
        .set(BlockIndexType.RANGE_SHORT,   (byte)2)
        .set(BlockIndexType.RANGE_INTEGER, (byte)3)
        .set(BlockIndexType.RANGE_LONG,    (byte)4)
        .set(BlockIndexType.RANGE_FLOAT,   (byte)5)
        .set(BlockIndexType.RANGE_DOUBLE,  (byte)6)
        .create();
    fullRangeBlockIndexList = getTypeToByteDispatcherFactory.keySet();

    SwitchDispatcherFactory<Byte, DispatchedFunc> getByteToBlockIndexDispacherFactory =
        (new SwitchDispatcherFactory<Byte, DispatchedFunc>());
    getByteToBlockIndexDispacherFactory.set((byte)0, () -> new StringRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)1, () -> new ByteRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)2, () -> new ShortRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)3, () -> new IntegerRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)4, () -> new LongRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)5, () -> new FloatRangeBlockIndex());
    getByteToBlockIndexDispacherFactory.set((byte)6, () -> new DoubleRangeBlockIndex());
    getByteToBlockIndexDispacher = getByteToBlockIndexDispacherFactory.create();
  }

  public List<RangeBlockIndex> childList = new ArrayList<RangeBlockIndex>();

  public FullRangeBlockIndex() {}

  /**
   * Set an initial value to create a new object.
   */
  public FullRangeBlockIndex( final int spreadIndex , final IBlockIndex index ) {
    if (!fullRangeBlockIndexList.contains(index.getBlockIndexType())) {
      throw new UnsupportedOperationException(
          "Unsupport index type : " + index.getBlockIndexType());
    }
    childList.add(new RangeBlockIndex(spreadIndex, index));
  }

  private final class RangeBlockIndex {

    private final int index;
    private final IBlockIndex blockIndex;

    public RangeBlockIndex( final int index , final IBlockIndex blockIndex ) {
      this.index = index;
      this.blockIndex = blockIndex;
    }

    public int getIndex() {
      return index;
    }

    public IBlockIndex getBlockIndex() {
      return blockIndex;
    }
  }

  /**
   * Determine the type of block index and convert it to bytes.
   */
  public byte getTypeToByte(final BlockIndexType type) {
    Byte res = getTypeToByteDispatcher.get(type);
    if (Objects.isNull(res)) {
      throw new UnsupportedOperationException("Unsupport index type : " + type);
    }
    return res;
  }

  /**
   * Determine byte and obtain IBlockIndex.
   */
  public IBlockIndex getByteToBlockIndex(final byte type) {
    DispatchedFunc res = getByteToBlockIndexDispacher.get(type);
    if (Objects.isNull(res)) {
      throw new UnsupportedOperationException("Unsupport index type");
    }
    return res.get();
  }

  public List<RangeBlockIndex> getBlockIndexList() {
    return childList;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.FULL_RANGE;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( ! ( blockIndex instanceof FullRangeBlockIndex ) ) {
      return false;
    }
    FullRangeBlockIndex fullRangeBlockIndex = (FullRangeBlockIndex)blockIndex;
    childList.addAll( fullRangeBlockIndex.getBlockIndexList() );
    return true;
  }

  @Override
  public int getBinarySize() {
    int total = 0;
    for ( RangeBlockIndex index : childList ) {
      total += index.getBlockIndex().getBinarySize();
    }
    return Integer.BYTES + ( ( Byte.BYTES + Integer.BYTES * 2 ) * childList.size() ) + total;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( childList.size() );
    for ( RangeBlockIndex index : childList ) {
      byte[] childBinary = index.getBlockIndex().toBinary();
      wrapBuffer.put( getTypeToByte( index.getBlockIndex().getBlockIndexType() ) );
      wrapBuffer.putInt( index.getIndex() );
      wrapBuffer.putInt( childBinary.length );
      wrapBuffer.put( childBinary );
    }
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
    int num = wrapBuffer.getInt();

    for ( int i = 0 ; i < num ; i++ ) {
      byte type = wrapBuffer.get();
      int spreadIndex = wrapBuffer.getInt();
      int binaryLength = wrapBuffer.getInt();
      byte[] childBinary = new byte[binaryLength];
      wrapBuffer.get( childBinary , 0 , childBinary.length );
      IBlockIndex blockIndex = getByteToBlockIndex( type );
      blockIndex.setFromBinary( childBinary , 0 , childBinary.length );
      childList.add( new RangeBlockIndex( spreadIndex , blockIndex ) );
    }
  }

  /**
   * Get the index of Spread that needs to be read.
   */
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    List<Integer> result = new ArrayList<Integer>();
    for ( RangeBlockIndex index : childList ) {
      List<Integer> childResult = index.getBlockIndex().getBlockSpreadIndex( filter );
      if ( Objects.isNull(childResult)) {
        result.add( index.getIndex() );
      }
    }
    return result;
  }

  @Override
  public IBlockIndex getNewInstance() {
    return new FullRangeBlockIndex();
  }
}

