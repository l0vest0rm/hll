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

// Created by xuning on 2016/12/21

package hll

import(
    "fmt"
)

type bigEndianAscendingWordDeserializer struct {
    // The length in bits of the words to be read.
    wordLength uint

    // The byte array to which the words are serialized.
    bytes []byte

    // The number of leading padding bytes in 'bytes' to be ignored.
    bytePadding uint

    // The number of words that the byte array contains.
    wordCount uint

    // The current read state.
    currentWordIndex uint
}

func newBigEndianAscendingWordDeserializer(wordLength uint, bytePadding uint, bytes []byte) *bigEndianAscendingWordDeserializer{
    if((wordLength < 1) || (wordLength > BITS_PER_LONG)) {
        panic(fmt.Sprintf("Word length must be >= 1 and <= 64. (was: %d)", wordLength))
    }

    this := &bigEndianAscendingWordDeserializer{}
    this.wordLength = wordLength;
    this.bytes = bytes;
    this.bytePadding = bytePadding;

    dataBytes := (uint(len(bytes)) - bytePadding);
    dataBits := (dataBytes * BITS_PER_BYTE);

    this.wordCount = uint(dataBits/wordLength);

    this.currentWordIndex = 0;

    return this
}

func (this *bigEndianAscendingWordDeserializer)readWord() uint64 {
    word := this.readWord2(this.currentWordIndex);
    this.currentWordIndex++;

    return word;
}

func (this *bigEndianAscendingWordDeserializer)readWord2(position uint) uint64 {
    // First bit of the word
    firstBitIndex := (position * this.wordLength);
    firstByteIndex := (this.bytePadding + (firstBitIndex / BITS_PER_BYTE));
    firstByteSkipBits := (firstBitIndex % BITS_PER_BYTE);

    // Last bit of the word
    lastBitIndex := (firstBitIndex + this.wordLength - 1);
    lastByteIndex := (this.bytePadding + (lastBitIndex / BITS_PER_BYTE));
    var lastByteBitsToConsume uint

     bitsAfterByteBoundary := ((lastBitIndex + 1) % BITS_PER_BYTE);
    // If the word terminates at the end of the last byte, consume the whole
    // last byte.
    if(bitsAfterByteBoundary == 0) {
        lastByteBitsToConsume = BITS_PER_BYTE;
    } else {
        // Otherwise, only consume what is necessary.
        lastByteBitsToConsume = bitsAfterByteBoundary;
    }

    if lastByteIndex >= uint(len(this.bytes)) {
        panic("Word out of bounds of backing array.")
    }

    // Accumulator
    var value uint64

    // --------------------------------------------------------------------
    // First byte
    bitsRemainingInFirstByte := (BITS_PER_BYTE - firstByteSkipBits)
    var bitsToConsumeInFirstByte uint
    if bitsRemainingInFirstByte < this.wordLength {
        bitsToConsumeInFirstByte = bitsRemainingInFirstByte
    } else {
        bitsToConsumeInFirstByte = this.wordLength
    }

    firstByte := uint64(this.bytes[firstByteIndex])

    // Mask off the bits to skip in the first byte.
    firstByteMask := uint64((1 << bitsRemainingInFirstByte) - 1)
    firstByte &= firstByteMask
    // Right-align relevant bits of first byte.
    firstByte >>= (bitsRemainingInFirstByte - bitsToConsumeInFirstByte)

    value |= firstByte

    // If the first byte contains the whole word, short-circuit.
    if(firstByteIndex == lastByteIndex) {
        return value;
    }

    // --------------------------------------------------------------------
    // Middle bytes
    middleByteCount := (lastByteIndex - firstByteIndex - 1);
    for i :=uint(0); i<middleByteCount; i++ {
        middleByte := (this.bytes[firstByteIndex + i + 1] & BYTE_MASK);
        // Push middle byte onto accumulator.
        value <<= BITS_PER_BYTE;
        value |= uint64(middleByte);
    }

    // --------------------------------------------------------------------
    // Last byte
    lastByte := (this.bytes[lastByteIndex] & BYTE_MASK);
    lastByte >>= (BITS_PER_BYTE - lastByteBitsToConsume)
    value <<= lastByteBitsToConsume
    value |= uint64(lastByte)
    return value
}

func (this *bigEndianAscendingWordDeserializer)totalWordCount() uint {
    return this.wordCount;
}