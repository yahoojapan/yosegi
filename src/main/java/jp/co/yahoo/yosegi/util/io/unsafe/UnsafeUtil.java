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

package jp.co.yahoo.yosegi.util.io.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public final class UnsafeUtil {

  private static final Unsafe UNSAFE_OBJ;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field field = Unsafe.class.getDeclaredField( "theUnsafe" );
      field.setAccessible(true);
      unsafe = (Unsafe)field.get(null);
    } catch ( Exception ex ) {
      unsafe = null;
    }
    UNSAFE_OBJ = unsafe;
  }

  public static void putBoolean( final Object obj , final long offset , final boolean value ) {
    UNSAFE_OBJ.putBoolean( obj , offset , value );
  }

  public static boolean getBoolean( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getBoolean( obj , offset );
  }

  public static void putByte( final Object obj , final long offset , final byte value ) {
    UNSAFE_OBJ.putByte( obj , offset , value );
  }

  public static byte getByte( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getByte( obj , offset );
  }

  public static void putShort( final Object obj , final long offset , final short value ) {
    UNSAFE_OBJ.putShort( obj , offset , value );
  }

  public static short getShort( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getShort( obj , offset );
  }

  public static void putInt( final Object obj , final long offset , final int value ) {
    UNSAFE_OBJ.putInt( obj , offset , value );
  }

  public static int getInt( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getInt( obj , offset );
  }

  public static void putLong( final Object obj , final long offset , final long value ) {
    UNSAFE_OBJ.putLong( obj , offset , value );
  }

  public static long getLong( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getLong( obj , offset );
  }

  public static void putFloat( final Object obj , final long offset , final float value ) {
    UNSAFE_OBJ.putFloat( obj , offset , value );
  }

  public static float getFloat( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getFloat( obj , offset );
  }

  public static void putDouble( final Object obj , final long offset , final double value ) {
    UNSAFE_OBJ.putDouble( obj , offset , value );
  }

  public static double getDouble( final Object obj , final long offset ) {
    return UNSAFE_OBJ.getDouble( obj , offset );
  }

}
