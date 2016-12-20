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
    n uint
    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    maxFill uint
    /** The mask for wrapping a position counter. */
    mask uint64
    /** Number of entries in the set. */
    size uint
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
func NewLongHashSet2(expected uint, f float64) (*LongHashSet,error){
    this := &LongHashSet{}
    if  f <= 0 || f > 1 {
        return nil, errors.New("Load factor must be greater than 0 and smaller than or equal to 1")
    }

    if  expected < 0 {
        return nil,errors.New("The expected number of elements must be nonnegative")
    }

    this.f = f
    this.n = arraySize( expected, f )
    this.mask = uint64(this.n - 1)
    this.maxFill = maxFill( this.n, f )
    this.key = make([]uint64, this.n)
    this.used = make([]bool, this.n)

    return this, nil
}

func (this *LongHashSet) Clone() *LongHashSet {
    c := &LongHashSet{}
    c.f = this.f
    c.n = this.n
    c.mask = this.mask
    c.maxFill = this.maxFill
    c.key = make([]uint64, c.n)
    copy(c.key, this.key)
    c.used = make([]bool, c.n)
    copy(c.used, this.used)
    return c
}

func (this *LongHashSet)Add(k uint64 ) bool {
    // The starting point.
    pos := murmur3Hash64( (k) ^ this.mask ) & this.mask;
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
        this.rehash( arraySize(this.size + 1, this.f ) )
    }
    this.size += 1

    return true;
}

func (this *LongHashSet)Size() uint {
    return this.size
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
func (this *LongHashSet) rehash(newN uint) {
    i := 0
    used := this.used;
    key := this.key;
    mask := uint64(newN - 1) // Note that this is used by the hashing macro
    newKey := make([]uint64, newN)
    newUsed := make([]bool, newN)
    for j := this.size; j > 0; j--{
        for ; !used[ i ];{
            i += 1
        }

        k := key[ i ];
        pos := murmur3Hash64( (k) ^ mask ) & mask;
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

type LongHashSetIterator struct {
    longHashSet *LongHashSet
    pos uint
    c uint
}

func NewLongHashSetIterator(longHashSet *LongHashSet) *LongHashSetIterator{
    this := &LongHashSetIterator{}
    this.longHashSet = longHashSet
    this.c = longHashSet.size
    this.pos = longHashSet.n

    used := longHashSet.used
    if this.c != 0 {
        this.pos -= 1
        for ; !used[ this.pos ]; {
            this.pos -= 1
        }
    }

    return this
}

func (this *LongHashSetIterator)HasNext() bool {
    return this.c != 0
}

func (this *LongHashSetIterator)Next() uint64 {
    if !this.HasNext(){
        panic("LongHashSetIterator,Next,no more element")
        return 0
    }

    this.c -= 1
    it := this.longHashSet
    retVal := it.key[this.pos]
    if this.c != 0 {
        for ;this.pos != 0; {
            this.pos -= 1
            if it.used[ this.pos ] {
                break
            }
        }
    }

    return retVal
}
