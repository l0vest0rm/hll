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
            maxRegisterValue := (1 << regWidth) - 1

            // Since 1 is added to p(w) in the insertion algorithm, only
            // (maxRegisterValue - 1) bits are inspected hence the hash
            // space is one power of two smaller.
            pwBits := (maxRegisterValue - 1)
            totalBits := (uint64(pwBits) + log2m)
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
func pwMaxMask(registerSizeInBits uint64) uint64 {
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
func smallEstimatorCutoff(m uint64) float64 {
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
func largeEstimatorCutoff(log2m uint64, registerSizeInBits uint64) float64 {
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