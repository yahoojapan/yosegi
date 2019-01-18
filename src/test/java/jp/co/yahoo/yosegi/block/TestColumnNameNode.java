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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestColumnNameNode{

  @Test
  public void T_newInstance_1(){
    ColumnNameNode columnNameNode = new ColumnNameNode( "root" );
    assertEquals( columnNameNode.getNodeName() , "root" ); 
  }

  @Test
  public void T_addChild_1(){
    ColumnNameNode columnNameNode = new ColumnNameNode( "root" );
    assertTrue( columnNameNode.isChildEmpty() );
    assertEquals( columnNameNode.getChildSize() , 0 );
    columnNameNode.addChild( new ColumnNameNode( "child" ) );
    assertEquals( columnNameNode.getChildSize() , 1 );
    assertTrue( columnNameNode.containsChild( "child" ) );
    assertFalse( columnNameNode.containsChild( "child2" ) );

    ColumnNameNode childNode = columnNameNode.getChild( "child" );
    assertEquals( childNode.getNodeName() , "child" );
  }

  @Test
  public void T_isNeedAllChild_1(){
    ColumnNameNode columnNameNode = new ColumnNameNode( "root" );
    assertFalse( columnNameNode.isNeedAllChild() );
    columnNameNode.setNeedAllChild( true );
    assertTrue( columnNameNode.isNeedAllChild() );
    columnNameNode.setNeedAllChild( false );
    assertFalse( columnNameNode.isNeedAllChild() );
  }

  @Test
  public void T_toString_1(){
    ColumnNameNode columnNameNode = new ColumnNameNode( "root" );
    columnNameNode.addChild( new ColumnNameNode( "child" ) );

    System.out.println( columnNameNode.toString() );
  }

}
