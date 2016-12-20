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

import (
    "math"
    "fmt"
)

const(
    REG_WIDTH_INDEX_MULTIPLIER = MAXIMUM_LOG2M_PARAM + 1
)

var(
    PW_MASK  = []uint64{
        0x8000000000000000,
        0xffffffffffffffff,
        0xfffffffffffffffc,
        0xffffffffffffffc0,
        0xffffffffffffc000,
        0xffffffffc0000000,
        0xc000000000000000,
        0xc000000000000000,
        0xc000000000000000}

    LEAST_SIGNIFICANT_BIT = []int{
        -1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0}

    /**
     * Precomputed <code>twoToL</code> values indexed by a linear combination of
     * <code>regWidth</code> and <code>log2m</code>.
     *
     * The array is one-dimensional and can be accessed by using index
     * <code>(REG_WIDTH_INDEX_MULTIPLIER * regWidth) + log2m</code>
     * for <code>regWidth</code> and <code>log2m</code> between the specified
     * <code>HLL.{MINIMUM,MAXIMUM}_{REGWIDTH,LOG2M}_PARAM</code> constants.
     *
     * @see #largeEstimator(int, int, double)
     * @see #largeEstimatorCutoff(int, int)
     * @see <a href='http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/'>Blog post with section on 2^L</a>
     */
    TWO_TO_L = [(MAXIMUM_REGWIDTH_PARAM + 1) * (MAXIMUM_LOG2M_PARAM + 1)]float64{}
)

func Init(){
    for regWidth := MINIMUM_REGWIDTH_PARAM; regWidth <= MAXIMUM_REGWIDTH_PARAM; regWidth++ {
        for log2m := MINIMUM_LOG2M_PARAM ; log2m <= MAXIMUM_LOG2M_PARAM; log2m++ {
            maxRegisterValue := (1 << uint(regWidth)) - 1

            // Since 1 is added to p(w) in the insertion algorithm, only
            // (maxRegisterValue - 1) bits are inspected hence the hash
            // space is one power of two smaller.
            pwBits := (maxRegisterValue - 1)
            totalBits := pwBits + log2m
            twoToL := math.Pow(2, float64(totalBits))
            TWO_TO_L[(REG_WIDTH_INDEX_MULTIPLIER * regWidth) + log2m] = twoToL
        }
    }
}

/**
     * Computes a mask that prevents overflow of HyperLogLog registers.
     *
     * @param  registerSizeInBits the size of the HLL registers, in bits.
     * @return mask a <code>long</code> mask to prevent overflow of the registers
     * @see #registerBitSize(long)
     */
func pwMaxMask(registerSizeInBits uint) uint64 {
    return PW_MASK[registerSizeInBits]
}

/**
     * Computes the 'alpha-m-squared' constant used by the HyperLogLog algorithm.
     *
     * @param  m this must be a power of two, cannot be less than
     *         16 (2<sup>4</sup>), and cannot be greater than 65536 (2<sup>16</sup>).
     * @return gamma times <code>registerCount</code> squared where gamma is
     *         based on the value of <code>registerCount</code>.
     * @throws IllegalArgumentException if <code>registerCount</code> is less
     *         than 16.
     */
func alphaMSquared(m float64) float64 {
    switch(m) {
    case 1/*2^0*/:
    case 2/*2^1*/:
    case 4/*2^2*/:
    case 8/*2^3*/:
        panic(fmt.Sprintf("'m' cannot be less than 16 (%d < 16).", m))

    case 16/*2^4*/:
        return 0.673 * m * m

    case 32/*2^5*/:
        return 0.697 * m * m

    case 64/*2^6*/:
        return 0.709 * m * m

    default/*>2^6*/:
        return (0.7213 / (1.0 + 1.079 / m)) * m * m
    }

    return 0
}

/**
     * The cutoff for using the "small range correction" formula, in the
     * HyperLogLog algorithm.
     *
     * @param  m the number of registers in the HLL. <em>m<em> in the paper.
     * @return the cutoff for the small range correction.
     * @see #smallEstimator(int, int)
     */
func smallEstimatorCutoff(m uint) float64 {
    return (float64(m) * 5) / 2
}

/**
     * The cutoff for using the "large range correction" formula, from the
     * HyperLogLog algorithm, adapted for 64 bit hashes.
     *
     * @param  log2m log-base-2 of the number of registers in the HLL. <em>b<em> in the paper.
     * @param  registerSizeInBits the size of the HLL registers, in bits.
     * @return the cutoff for the large range correction.
     * @see #largeEstimator(int, int, double)
     * @see <a href='http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/'>Blog post with section on 64 bit hashes and "large range correction" cutoff</a>
     */
func largeEstimatorCutoff(log2m uint, registerSizeInBits uint) float64 {
    return (TWO_TO_L[(REG_WIDTH_INDEX_MULTIPLIER * registerSizeInBits) + log2m]) / 30.0
}

/**
     * Computes the least-significant bit of the specified <code>long</code>
     * that is set to <code>1</code>. Zero-indexed.
     *
     * @param  value the <code>long</code> whose least-significant bit is desired.
     * @return the least-significant bit of the specified <code>long</code>.
     *         <code>-1</code> is returned if there are no bits set.
     */
// REF:  http://stackoverflow.com/questions/757059/position-of-least-significant-bit-that-is-set
// REF:  http://www-graphics.stanford.edu/~seander/bithacks.html
func  leastSignificantBit(value uint64) int {
    if(value == 0) {
        return -1
    }/*by contract*/
    if (value & 0xFF) != 0{
        return LEAST_SIGNIFICANT_BIT[(int)( (value >>  0) & 0xFF)] +  0;
    }
    if((value & 0xFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >>  8) & 0xFF)] +  8
    }
    if((value & 0xFFFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >> 16) & 0xFF)] + 16
    }
    if((value & 0xFFFFFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >> 24) & 0xFF)] + 24
    }
    if((value & 0xFFFFFFFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >> 32) & 0xFF)] + 32
    }
    if((value & 0xFFFFFFFFFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >> 40) & 0xFF)] + 40
    }
    if((value & 0xFFFFFFFFFFFFFF) != 0) {
        return LEAST_SIGNIFICANT_BIT[(int)( (value >> 48) & 0xFF)] + 48
    }
    return LEAST_SIGNIFICANT_BIT[(int)( (value >> 56) & 0xFF)] + 56;
}

/**
     * The "small range correction" formula from the HyperLogLog algorithm. Only
     * appropriate if both the estimator is smaller than <pre>(5/2) * m</pre> and
     * there are still registers that have the zero value.
     *
     * @param  m the number of registers in the HLL. <em>m<em> in the paper.
     * @param  numberOfZeroes the number of registers with value zero. <em>V</em>
     *         in the paper.
     * @return a corrected cardinality estimate.
     */
func smallEstimator(m uint, numberOfZeroes int) float64 {
    return float64(m) * math.Log(float64(m) / float64(numberOfZeroes))
}

/**
     * The "large range correction" formula from the HyperLogLog algorithm, adapted
     * for 64 bit hashes. Only appropriate for estimators whose value exceeds
     * the return of {@link #largeEstimatorCutoff(int, int)}.
     *
     * @param  log2m log-base-2 of the number of registers in the HLL. <em>b<em> in the paper.
     * @param  registerSizeInBits the size of the HLL registers, in bits.
     * @param  estimator the original estimator ("E" in the paper).
     * @return a corrected cardinality estimate.
     * @see <a href='http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/'>Blog post with section on 64 bit hashes and "large range correction"</a>
     */
func largeEstimator(log2m uint, registerSizeInBits uint, estimator float64) float64 {
    twoToL := TWO_TO_L[(REG_WIDTH_INDEX_MULTIPLIER * registerSizeInBits) + log2m];
    return -1 * twoToL * math.Log(1.0 - (estimator/twoToL));
}

/** Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
	 *
	 * <p>This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
	 * Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
	 * values returned by {@link Object#hashCode()}.
	 *
	 * @param x a long integer.
	 * @return a hash value with good avalanching properties.
	 */
func murmur3Hash64(x uint64) uint64 {
    x ^= x >> 33
    x *= 0xff51afd7ed558ccd
    x ^= x >> 33
    x *= 0xc4ceb9fe1a85ec53
    x ^= x >> 33

    return x
}

/** Avalanches the bits of an integer by applying the finalisation step of MurmurHash3.
	 *
	 * <p>This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
	 * Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
	 * values returned by {@link Object#hashCode()}.
	 *
	 * @param x an integer.
	 * @return a hash value with good avalanching properties.
	 */
func murmur3Hash32(x uint32) uint32 {
    x ^= x >> 16;
    x *= 0x85ebca6b;
    x ^= x >> 13;
    x *= 0xc2b2ae35;
    x ^= x >> 16;
    return x;
}

/** Returns the maximum number of entries that can be filled before rehashing.
	 *
	 * @param n the size of the backing array.
	 * @param f the load factor.
	 * @return the maximum number of entries before rehashing.
	 */
func maxFill(n uint, f float64) uint {
    /* We must guarantee that there is always at least
     * one free entry (even with pathological load factors). */
    return uint(math.Min(math.Ceil( float64(n) * f ), float64(n - 1) ))
}

/** Returns the least power of two smaller than or equal to 2<sup>30</sup> and larger than or equal to <code>Math.ceil( expected / f )</code>.
	 *
	 * @param expected the expected number of elements in a hash table.
	 * @param f the load factor.
	 * @return the minimum possible size for a backing array.
	 * @throws IllegalArgumentException if the necessary size is larger than 2<sup>30</sup>.
	 */
func arraySize(expected uint, f float64) uint {
    s := uint(math.Max( float64(2), float64(nextPowerOfTwo(uint64(math.Ceil( float64(expected) / f ) )) )))
    if s > (1 << 30) {
        panic(fmt.Sprintf("Too large (%d expected elements with load factor %f)", expected, f))
        return 0
    }
    return s
}

/** Return the least power of two greater than or equal to the specified value.
	 *
	 * <p>Note that this function will return 1 when the argument is 0.
	 *
	 * @param x a long integer smaller than or equal to 2<sup>62</sup>.
	 * @return the least power of two greater than or equal to the specified value.
	 */
func nextPowerOfTwo( x uint64) uint64 {
    if ( x == 0 ) {
        return 1
    }

    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return ( x | x >> 32 ) + 1
}