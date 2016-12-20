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

// Created by xuning on 2016/12/20

package hll

import (
    "errors"
)

type Int2ByteHashMap struct {
    /** The array of keys. */
    key []uint32
    /** The array of values. */
    value []byte
    /** The array telling whether a position is used. */
    used []bool
    /** The acceptable load factor. */
    f float64
    /** The current table size. */
    n uint
    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    maxFill uint
    /** The mask for wrapping a position counter. */
    mask uint32
    /** Number of entries in the set. */
    size uint
}

func NewInt2ByteHashMap() (*Int2ByteHashMap, error){
    return NewInt2ByteHashMap2(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR)
}

func NewInt2ByteHashMap2(expected uint, f float64) (*Int2ByteHashMap, error){
    this := &Int2ByteHashMap{}
    if  f <= 0 || f > 1 {
        return nil, errors.New("Load factor must be greater than 0 and smaller than or equal to 1")
    }

    if  expected < 0 {
        return nil,errors.New("The expected number of elements must be nonnegative")
    }

    this.f = f
    this.n = arraySize( expected, f )
    this.mask = uint32(this.n - 1)
    this.maxFill = maxFill( this.n, f )
    this.key = make([]uint32, this.n)
    this.value = make([]byte, this.n)
    this.used = make([]bool, this.n)

    return this,nil
}

/** Returns a deep copy of this map.
	 *
	 * <P>This method performs a deep copy of this hash map; the data stored in the
	 * map, however, is not cloned. Note that this makes a difference only for object keys.
	 *
	 *  @return a deep copy of this map.
	 */
func (this *Int2ByteHashMap) Clone() *Int2ByteHashMap {
    c := &Int2ByteHashMap{}

    c.f = this.f
    c.n = this.n
    c.mask = this.mask
    c.maxFill = this.maxFill
    c.key = make([]uint32, c.n)
    copy(c.key, this.key)
    c.value = make([]byte, c.n)
    copy(c.value, this.value)
    c.used = make([]bool, c.n)
    copy(c.used, this.used)

    return c;
}

/*
	 * The following methods implements some basic building blocks used by
	 * all accessors. They are (and should be maintained) identical to those used in OpenHashSet.drv.
	 */
func (this *Int2ByteHashMap)put(k uint32, v byte) byte {
    // The starting point.
    pos := (murmur3Hash32( (k) ^ this.mask ) ) & this.mask;
    // There's always an unused entry.
    for ;this.used[ pos ];{
        if this.key[pos] == k {
            oldValue := this.value[ pos ]
            this.value[ pos ] = v
            return oldValue
        }
        pos = ( pos + 1 ) & this.mask
    }

    this.used[ pos ] = true
    this.key[ pos ] = k
    this.value[ pos ] = v
    if this.size >= this.maxFill{
        this.rehash( arraySize( this.size + 1, this.f ) )
    }
    this.size += 1

    //defRetValue
    return 0;
}

func (this *Int2ByteHashMap) get(k uint32) byte {
    // The starting point.
    pos := (murmur3Hash32( (k) ^ this.mask ) ) & this.mask;
    // There's always an unused entry.
    for ;this.used[ pos ];{
        if this.key[pos] == k {
            return this.value[ pos ]
        }
        pos = ( pos + 1 ) & this.mask
    }

    return 0;
}

func (this *Int2ByteHashMap) Size() uint {
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
func (this *Int2ByteHashMap) rehash(newN uint) {
    i := 0
    used := this.used;
    key := this.key;
    mask := uint32(newN - 1) // Note that this is used by the hashing macro
    newKey := make([]uint32, newN)
    newValue := make([]byte, newN)
    newUsed := make([]bool, newN)
    for j := this.size; j > 0; j--{
        for ; !used[ i ];{
            i += 1
        }

        k := key[ i ];
        pos := murmur3Hash32( (k) ^ mask ) & mask
        for ;newUsed[ pos ];{
            pos = ( pos + 1 ) & mask
        }

        newUsed[ pos ] = true
        newKey[ pos ] = k
        newValue[ pos ] = this.value[ i ]
        i++;
    }
    this.n = newN
    this.mask = mask
    this.maxFill = maxFill( this.n, this.f )
    this.key = newKey
    this.value = newValue
    this.used = newUsed
}

type Int2ByteHashMapIterator struct {
    int2ByteHashMap *Int2ByteHashMap
    pos uint
    c uint
}

func NewInt2ByteHashMapIterator(int2ByteHashMap *Int2ByteHashMap) *Int2ByteHashMapIterator{
    this := &Int2ByteHashMapIterator{}
    this.int2ByteHashMap = int2ByteHashMap
    this.c = int2ByteHashMap.size
    this.pos = int2ByteHashMap.n

    used := int2ByteHashMap.used
    if this.c != 0 {
        this.pos -= 1
        for ; !used[ this.pos ]; {
            this.pos -= 1
        }
    }

    return this
}

func (this *Int2ByteHashMapIterator)HasNext() bool {
    return this.c != 0
}

func (this *Int2ByteHashMapIterator)NextKey() uint32 {
    if !this.HasNext(){
        panic("LongHashSetIterator,Next,no more element")
        return 0
    }

    this.c -= 1
    it := this.int2ByteHashMap
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