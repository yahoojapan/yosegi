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
package jp.co.yahoo.yosegi.message.design;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestArrayContainerField {

  @Test
  public void T_newInstance(){
    String schemaName = "test";
    IField childField = new BooleanField( "boolean" );

    ArrayContainerField arrayContainerField = new ArrayContainerField( schemaName , childField );
  }

  @Test
  public void T_newInstance_2(){
    String childSchemaName = "child";
    IField childField = new BooleanField( "boolean" );

    ArrayContainerField childArrayContainerField = new ArrayContainerField( childSchemaName , childField );

    String schemaName = "parent";
    ArrayContainerField arrayContainerField = new ArrayContainerField( schemaName , childArrayContainerField );

  }

  @Test
  public void T_getField(){
    String schemaName = "test";
    IField childField = new BooleanField( "boolean" );

    ArrayContainerField arrayContainerField = new ArrayContainerField( schemaName , childField );

    assertEquals( arrayContainerField.getField().getName() , "boolean" );
  }

  @Test
  public void T_getName(){
    String schemaName = "test";
    IField childField = new BooleanField( "boolean" );

    ArrayContainerField arrayContainerField = new ArrayContainerField( schemaName , childField );

    assertEquals( "test" , arrayContainerField.getName() );
  }

}
