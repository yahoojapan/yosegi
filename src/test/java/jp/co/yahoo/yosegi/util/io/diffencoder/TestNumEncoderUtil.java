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

package jp.co.yahoo.yosegi.util.io.diffencoder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestNumEncoderUtil {

  @Test
  public void T_createEncoder_equalsLongNumEncoder_withLongMinAndMax() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( Long.MIN_VALUE , Long.MAX_VALUE );
    assertTrue( ( encoder instanceof LongNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsByteNumEncoder_withByteRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( (long)Byte.MIN_VALUE , (long)Byte.MAX_VALUE );
    assertTrue( ( encoder instanceof ByteNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsShortNumEncoder_withShortRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( (long)Short.MIN_VALUE , (long)Short.MAX_VALUE );
    assertTrue( ( encoder instanceof ShortNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsIntegerNumEncoder_withIntRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( (long)Integer.MIN_VALUE , (long)Integer.MAX_VALUE );
    assertTrue( ( encoder instanceof IntegerNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsDiffNumEncoder_withByteRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( Long.MAX_VALUE - (long)Byte.MAX_VALUE , Long.MAX_VALUE );
    assertTrue( ( encoder instanceof DiffLongNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsDiffNumEncoder_withShortRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( Long.MAX_VALUE - (long)Short.MAX_VALUE , Long.MAX_VALUE );
    assertTrue( ( encoder instanceof DiffLongNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsDiffNumEncoder_withIntRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( Long.MAX_VALUE - (long)Integer.MAX_VALUE , Long.MAX_VALUE );
    assertTrue( ( encoder instanceof DiffLongNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsDiffLongNumEncoder_withLongRange() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( 0L , (long)Integer.MAX_VALUE + 1 );
    assertTrue( ( encoder instanceof DiffLongNumEncoder) );
  }

  @Test
  public void T_createEncoder_equalsFixedNumEncoder() throws IOException {
    INumEncoder encoder = NumEncoderUtil.createEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    assertTrue( ( encoder instanceof FixedNumEncoder) );
  }

}
