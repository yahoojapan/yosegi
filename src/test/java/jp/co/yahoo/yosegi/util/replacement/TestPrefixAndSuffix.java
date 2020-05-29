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
package jp.co.yahoo.yosegi.util.replacement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestPrefixAndSuffix {

  @Test
  public void T_append_equalsSetValue_withNoPrefixAndSuffix(){
    PrefixAndSuffix pas = new PrefixAndSuffix( null , null , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( original , result );
  }

  @Test
  public void T_append_equalsSetValue_withPrefixOnly(){
    PrefixAndSuffix pas = new PrefixAndSuffix( "PREFIX" , null , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "PREFIX_%s" , original ) , result );
  }

  @Test
  public void T_append_equalsSetValue_withEmptyPrefixOnly(){
    PrefixAndSuffix pas = new PrefixAndSuffix( "" , null , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "_%s" , original ) , result );
  }

  @Test
  public void T_append_equalsSetValue_withSuffixOnly(){
    PrefixAndSuffix pas = new PrefixAndSuffix( null , "SUFFIX" , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "%s_SUFFIX" , original ) , result );
  }

  @Test
  public void T_append_equalsSetValue_withEmptySuffixOnly(){
    PrefixAndSuffix pas = new PrefixAndSuffix( null , "" , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "%s_" , original ) , result );
  }

  @Test
  public void T_append_equalsSetValue_withPrefixAndSuffix(){
    PrefixAndSuffix pas = new PrefixAndSuffix( "PREFIX" , "SUFFIX" , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "PREFIX_%s_SUFFIX" , original ) , result );
  }

  @Test
  public void T_append_equalsSetValue_withEmptyPrefixAndEmptySuffix(){
    PrefixAndSuffix pas = new PrefixAndSuffix( "" , "" , "_" );
    String original = "test";
    String result = pas.append( original );
    assertEquals( String.format( "_%s_" , original ) , result );
  }


  @Test
  public void T_newInstance_throwsException_withNullDelimiter(){
    assertThrows( IllegalArgumentException.class ,
      () -> {
        PrefixAndSuffix pas = new PrefixAndSuffix( "PREFIX" , "SUFFIX" , null );
      }
    );
  }

}
