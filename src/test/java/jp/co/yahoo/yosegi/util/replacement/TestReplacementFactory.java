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

public class TestReplacementFactory {

  @Test
  public void T_getInstance_equalsDefaultReplacement_withNull(){
    IReplacement result = ReplacementFactory.get( null );
    assertTrue( ( result instanceof DefaultReplacement ) );
  }

  @Test
  public void T_getInstance_equalsDefaultReplacement_withEmptyString(){
    IReplacement result = ReplacementFactory.get( "" );
    assertTrue( ( result instanceof DefaultReplacement ) );
  }

  @Test
  public void T_getInstance_equalsDefaultReplacement_withLower(){
    IReplacement result = ReplacementFactory.get( "lower" );
    assertTrue( ( result instanceof LowerReplacement ) );
  }

  @Test
  public void T_getInstance_equalsDefaultReplacement_withUnknownString(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        IReplacement result = ReplacementFactory.get( "XXXXX" );
      }
    );
  }

}
