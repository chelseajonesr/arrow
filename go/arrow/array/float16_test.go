// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array_test

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/float16"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFloat16SetReflectValue(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewFloat16Builder(mem)
	defer ab.Release()
	var (
		values = []float32{1, 2, -3, 0, 5}
		valid  = []bool{true, true, true, false, true}
	)

	for i, v := range values {
		if valid[i] {
			ab.Append(float16.New(v))
		} else {
			ab.AppendNull()
		}
	}

	arr := ab.NewFloat16Array()
	defer arr.Release()

	var aFloat32 float32
	var aFloat64 float64
	var aPtr *float32

	for i, v := range values {
		arr.SetReflectValue(reflect.ValueOf(&aFloat32), i, nil)
		arr.SetReflectValue(reflect.ValueOf(&aFloat64), i, nil)
		arr.SetReflectValue(reflect.ValueOf(&aPtr), i, nil)

		assert.Equal(t, v, aFloat32)
		assert.Equal(t, float64(v), aFloat64)
		if valid[i] {
			assert.Equal(t, v, *aPtr)
		} else {
			assert.Nil(t, aPtr)
		}
	}
}
