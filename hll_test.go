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
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestHyperloglog(t *testing.T) {
	//count := 40000000
	count := 40000000
	var clientid uint64

	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < count; i++ {
		clientid = uint64(rand.Int63())
		binary.Write(buf, binary.LittleEndian, clientid)
	}
	b := buf.Bytes()

	h := hyperloglog(b, count)
	data := h.ToBytes()
	fmt.Printf("bslen:%d\n", len(data))

	filename := "/tmp/hyperloglog.dat"
	err := ioutil.WriteFile(filename, data, os.ModePerm)
	if err != nil {
		fmt.Printf("WriteFile fail,err:%s\n", err.Error())
	}

	h2, err := NewHllFromBytes(data)
	if err != nil {
		fmt.Printf("NewHllFromBytes,err:%s\n", err.Error())
	}

	fmt.Printf("accuracy:%f\n", float64(h2.Cardinality())/float64(count))
}

func hyperloglog(b []byte, count int) *Hll {
	t1 := time.Now().UnixNano()
	var clientid uint64
	offset := 0
	h, _ := NewHll(14, 6)
	for i := 0; i < count; i++ {
		clientid = binary.LittleEndian.Uint64(b[offset:])
		h.Add(clientid)
		offset += 8
	}
	t2 := time.Now().UnixNano()
	fmt.Printf("time:%d,accuracy:%f\n", t2-t1, float64(h.Cardinality())/float64(count))
	return h
}
