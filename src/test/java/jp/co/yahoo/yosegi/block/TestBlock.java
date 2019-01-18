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

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.ColumnBinary;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestBlock{

  @Test
  public void T_setColumnBinaryTree_1(){
    Block block = new Block();
    block.setColumnBinaryTree( null );
  }

  @Test
  public void T_size_1(){
    Block block = new Block();
    block.setColumnBinaryTree( null );
    assertEquals( block.size() , 0 );
  }

  @Test
  public void T_size_2() throws IOException{
    Block block = new Block();
    ColumnBinaryTree tree = new ColumnBinaryTree();
    block.setColumnBinaryTree( tree );
    assertEquals( block.size() , 0 );

    List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
    childList.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null ) );
    tree.addChild( childList );
    tree.addChild( childList );

    assertEquals( block.size() , 2 );
  }

  @Test
  public void T_get_1(){
    Block block = new Block();
    block.setColumnBinaryTree( null );
    assertEquals( block.get( 0 ) , null );
  }

  @Test
  public void T_get_2() throws IOException{
    Block block = new Block();
    ColumnBinaryTree tree = new ColumnBinaryTree();
    block.setColumnBinaryTree( tree );
    assertEquals( block.size() , 0 );

    List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
    childList.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null ) );
    tree.addChild( childList );
    tree.addChild( childList );

    assertEquals( block.get(0).size() , 1 );
    assertEquals( block.get(0).get(0).columnName , "test" );
  }

}
