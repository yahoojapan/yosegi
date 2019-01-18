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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

public class TestBlockIndexNode{

  @Test
  public void T_newInstance_1(){
    BlockIndexNode b = new BlockIndexNode();
  }

  @Test
  public void T_setBlockIndex_1(){
    BlockIndexNode b = new BlockIndexNode();
    b.setBlockIndex( new DummyBlockIndex( false ) );
    IBlockIndex bIndex = b.getBlockIndex();
    assertTrue( bIndex instanceof DummyBlockIndex );
  }

  @Test
  public void T_setBlockIndex_2(){
    BlockIndexNode b = new BlockIndexNode();
    b.setBlockIndex( new DummyBlockIndex( true ) );
    b.setBlockIndex( new DummyBlockIndex( true ) );
    IBlockIndex bIndex = b.getBlockIndex();
    assertTrue( bIndex instanceof DummyBlockIndex );
  }

  @Test
  public void T_setBlockIndex_3(){
    BlockIndexNode b = new BlockIndexNode();
    b.setBlockIndex( new DummyBlockIndex( false ) );
    b.setBlockIndex( new DummyBlockIndex( false ) );
    IBlockIndex bIndex = b.getBlockIndex();
    assertTrue( bIndex instanceof UnsupportedBlockIndex );
  }

  @Test
  public void T_setBlockIndex_4(){
    BlockIndexNode b = new BlockIndexNode();
    b.setBlockIndex( new DummyBlockIndex( false ) );
    b.disable();
    IBlockIndex bIndex = b.getBlockIndex();
    assertTrue( bIndex instanceof UnsupportedBlockIndex );
  }

  @Test
  public void T_getChildNode_1(){
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = b.getChildNode( "hoge" );
    IBlockIndex bIndex = b2.getBlockIndex();
    assertTrue( bIndex instanceof UnsupportedBlockIndex );

    b2.setBlockIndex( new DummyBlockIndex( false ) );
    b2 = b.getChildNode( "hoge" );
    bIndex = b2.getBlockIndex();
    assertTrue( bIndex instanceof DummyBlockIndex );
  }

  @Test
  public void T_putChildNode_1(){
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = new BlockIndexNode();
    b2.setBlockIndex( new DummyBlockIndex( false ) );
    b.putChildNode( "hoge" , b2 );

    BlockIndexNode b3 = b.getChildNode( "hoge" );
    IBlockIndex bIndex = b3.getBlockIndex();
    assertTrue( bIndex instanceof DummyBlockIndex );
  }

  @Test
  public void T_getBinarySize_1() throws IOException{
    BlockIndexNode b = new BlockIndexNode();
    assertEquals( 8 , b.getBinarySize() );
  }

  @Test
  public void T_getBinarySize_2() throws IOException{
    BlockIndexNode b = new BlockIndexNode();
    b.setBlockIndex( new DummyBlockIndex( false ) );
    assertEquals( 8 + 8 + 2 + DummyBlockIndex.class.getName().getBytes().length , b.getBinarySize() );
  }

  @Test
  public void T_getBinarySize_3() throws IOException{
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = b.getChildNode( "hoge" );
    assertEquals( 8 + 16 + 8 , b.getBinarySize() );
  }

  @Test
  public void T_binary_1() throws IOException{
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = new BlockIndexNode();
    b2.setBlockIndex( new DummyBlockIndex( false ) );
    b.putChildNode( "hoge" , b2 );

    byte[] binary = new byte[b.getBinarySize()];
    b.toBinary( binary , 0 );
    BlockIndexNode b3 = BlockIndexNode.createFromBinary( binary , 0 );
    BlockIndexNode b4 = b3.getChildNode( "hoge" );
    IBlockIndex bIndex = b4.getBlockIndex();
    assertTrue( bIndex instanceof DummyBlockIndex );
  }

  @Test
  public void T_binary_2() throws IOException{
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = new BlockIndexNode();
    b2.setBlockIndex( new DummyBlockIndex( false ) );
    b.putChildNode( "hoge" , b2 );

    byte[] binary = new byte[1];
    assertThrows( IndexOutOfBoundsException.class ,
      () -> {
        b.toBinary( binary , 0 );
      }
    );
  }

  @Test
  public void T_clear_1(){
    BlockIndexNode b = new BlockIndexNode();
    BlockIndexNode b2 = new BlockIndexNode();
    b2.setBlockIndex( new DummyBlockIndex( false ) );
    b.putChildNode( "hoge" , b2 );
    b.clear();
    IBlockIndex bIndex = b.getBlockIndex();
    assertTrue( bIndex instanceof UnsupportedBlockIndex );
  }

}
