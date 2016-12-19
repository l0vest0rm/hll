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
    f float64
    /** The current table size. */
    n uint64
    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    maxFill uint64
    /** The mask for wrapping a position counter. */
    mask uint64
    /** Number of entries in the set. */
    size uint64
}

func NewLongHashSet() (*LongHashSet,error) {
    return NewLongHashSet2(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR)
}

/** Creates a new hash set.
	 *
	 * <p>The actual table size will be the least power of two greater than <code>expected</code>/<code>f</code>.
	 *
	 * @param expected the expected number of elements in the hash set.
	 * @param f the load factor.
	 */
func NewLongHashSet2(expected int, f float64) (*LongHashSet,error){
    this := &LongHashSet{}
    if  f <= 0 || f > 1 {
        return nil, errors.New("Load factor must be greater than 0 and smaller than or equal to 1")
    }

    if  expected < 0 {
        return nil,errors.New("The expected number of elements must be nonnegative")
    }

    this.f = f
    this.n = arraySize( expected, f )
    this.mask = this.n - 1
    this.maxFill = maxFill( this.n, f )
    this.key = make([]uint64, this.n, this.n)
    this.used = make([]bool, this.n, this.n)

    return this, nil
}

func (this *LongHashSet)add(k uint64 ) bool {
    // The starting point.
    pos := murmurHash3( (k) ^ this.mask ) & this.mask;
    // There's always an unused entry.
    for ;this.used[ pos ];{
        if this.key[pos] == k {
            return false
        }
        pos = ( pos + 1 ) & this.mask
    }
    this.used[ pos ] = true
    this.key[ pos ] = k
    if this.size >= this.maxFill{
        this.rehash( arraySize( int(this.size + 1), this.f ) )
    }
    this.size += 1

    return true;
}

/** Rehashes the set.
	 *
	 * <P>This method implements the basic rehashing strategy, and may be
	 * overriden by subclasses implementing different rehashing strategies (e.g.,
	 * disk-based rehashing). However, you should not override this method
	 * unless you understand the internal workings of this class.
	 *
	 * @param newN the new size
	 */
func (this *LongHashSet) rehash(newN uint64) {
    i := 0
    used := this.used;
    key := this.key;
    mask := newN - 1 // Note that this is used by the hashing macro
    newKey := make([]uint64, newN, newN)
    newUsed := make([]bool, newN, newN)
    for j := this.size; j > 0; j--{
        for ; !used[ i ];{
            i += 1
        }

        k := key[ i ];
        pos := murmurHash3( (k) ^ mask ) & mask;
        for ;newUsed[ pos ];{
            pos = ( pos + 1 ) & mask
        }

        newUsed[ pos ] = true;
        newKey[ pos ] = k;
        i++;
    }
    this.n = newN;
    this.mask = mask;
    this.maxFill = maxFill( this.n, this.f );
    this.key = newKey;
    this.used = newUsed;
}