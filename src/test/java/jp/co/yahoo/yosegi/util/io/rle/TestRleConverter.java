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

package jp.co.yahoo.yosegi.util.io.rle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestRleConverter {

  @Test
  public void T_addAndGet_equalsSetValue() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 2 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 2 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.add( Integer.valueOf( 1 ) );
    rle.finish();
    assertEquals( rle.getRowGroupCount() , 5 );
    assertEquals( rle.getMaxGroupLength() , 3 );

    int[] lengthArray = rle.getLengthArray();
    assertEquals( lengthArray[0] , 2  );
    assertEquals( lengthArray[1] , 1  );
    assertEquals( lengthArray[2] , 3  );
    assertEquals( lengthArray[3] , 1  );
    assertEquals( lengthArray[4] , 2  );

    Integer[] valueArray = rle.getValueArray();
    assertEquals( valueArray[0] , Integer.valueOf( 1 ) );
    assertEquals( valueArray[1] , Integer.valueOf( 2 ) );
    assertEquals( valueArray[2] , Integer.valueOf( 1 ) );
    assertEquals( valueArray[3] , Integer.valueOf( 2 ) );
    assertEquals( valueArray[4] , Integer.valueOf( 1 ) );
  }

  @Test
  public void T_addAndGet_equalsSetValue_withEmpty() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.finish();
    assertEquals( rle.getRowGroupCount() , 0 );
    assertEquals( rle.getMaxGroupLength() , 0 );
  }

  @Test
  public void T_getValueArray_throwsException_whenNotFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    assertThrows( IOException.class ,
      () -> {
        rle.getValueArray();
      }
    );
  }

  @Test
  public void T_getLengthArray_throwsException_whenNotFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    assertThrows( IOException.class ,
      () -> {
        rle.getLengthArray();
      }
    );
  }

  @Test
  public void T_getRowGroupCount_throwsException_whenNotFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    assertThrows( IOException.class ,
      () -> {
        rle.getRowGroupCount();
      }
    );
  }

  @Test
  public void T_getMaxGroupLength_throwsException_whenNotFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    assertThrows( IOException.class ,
      () -> {
        rle.getMaxGroupLength();
      }
    );
  }

  @Test
  public void T_add_throwsException_whenFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    rle.finish();
    assertThrows( IOException.class ,
      () -> {
        rle.add( Integer.valueOf( 1 ) );
      }
    );
  }

  @Test
  public void T_finish_throwsException_whenFinish() throws IOException {
    RleConverter<Integer> rle = new RleConverter<Integer>( Integer.valueOf( 1 ) , new Integer[100] );
    rle.add( Integer.valueOf( 1 ) );
    rle.finish();
    assertThrows( IOException.class ,
      () -> {
        rle.finish();
      }
    );
  }

}
