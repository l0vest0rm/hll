/**
 * Copyright 2016 l0vest0rm.hll authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

// Created by xuning on 2016/12/19

package hll

import(
    "errors"
)

const(
    /** The initial default size of a hash table. */
    DEFAULT_INITIAL_SIZE = 16
    /** The default load factor of a hash table. */
    DEFAULT_LOAD_FACTOR = .75
    /** The load factor for a (usually small) table that is meant to be particularly fast. */
    FAST_LOAD_FACTOR = .5
    /** The load factor for a (usually very small) table that is meant to be extremely fast. */
    VERY_FAST_LOAD_FACTOR = .25
)

type LongHashSet struct {
    /** The array of keys. */
    key []uint64
    /** The array telling whether a position is used. */
    used []bool
    /** The acceptable load factor. */
    f float32
    /** The current table size. */
    n int
    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    maxFill int
    /** The mask for wrapping a position counter. */
    mask int
    /** Number of entries in the set. */
    size int
}

func NewLongHashSet() (*LongHashSet,error){
    return NewLongHashSet2(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR)
}

/** Creates a new hash set.
	 *
	 * <p>The actual table size will be the least power of two greater than <code>expected</code>/<code>f</code>.
	 *
	 * @param expected the expected number of elements in the hash set.
	 * @param f the load factor.
	 */
func NewLongHashSet2(expected int, f float32) (*LongHashSet,error){
    this := &LongHashSet{}
    if  f <= 0 || f > 1 {
        return nil, errors.New("Load factor must be greater than 0 and smaller than or equal to 1")
    }

    if  expected < 0 {
        return nil,errors.New("The expected number of elements must be nonnegative")
    }
    this.f = f
    //n = arraySize( expected, f )
    //mask = n - 1
    //maxFill = maxFill( n, f )
    //key = new long[ n ]
    //used = new boolean[ n ]

    return this,nil
}