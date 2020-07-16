/*
 * Copyright 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cebbank.airisk.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import static org.apache.flink.table.api.DataTypes.*;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.LinkedList;
import java.util.Queue;



/**
 * Previous aggregate function.
 */
public abstract class PreviousValueAggFunction<T> extends AggregateFunction<T, PreviousValueAggFunction.PreviousValue<T>> {

    public static class PreviousValue<T> {
        public Queue<T> queue;
        public int length;
        public Object defaultValue;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public PreviousValue<T> createAccumulator() {
        PreviousValue<T> acc = new PreviousValue();
        acc.queue = new LinkedList<T>();
        acc.length = 0;
        acc.defaultValue = null;
        return acc;
    }

    public void accumulate(PreviousValue<T> acc, Object value, int length , Object defaultValue) {
        acc.length = length;
        acc.defaultValue = defaultValue;
        //队列填充默认值
        int accSize = acc.queue.size();
        if(accSize<length){
            for(int i = 0 ; i < length - accSize; i++){
                acc.queue.offer((T) defaultValue);
            }
        }
        if (value != null) {
            acc.queue.offer((T) value);
        }
        else{
            acc.queue.offer((T) defaultValue);
        }
    }


    public void accumulate(PreviousValue<T> acc, Object value, int length) {
        acc.length = length;
        acc.defaultValue = value;

        int accSize = acc.queue.size();
        if(accSize<length){
            for(int i = 0 ; i < length - accSize; i++){

                acc.queue.offer((T) value);
            }
        }
        acc.queue.offer((T) value);
    }



    public void resetAccumulator(PreviousValue<Object> acc) {
        acc.length = 0;
        acc.defaultValue = null;
        acc.queue.clear();
    }

    @Override
    public T getValue(PreviousValue<T> acc) {
        return (T) acc.queue.poll();
    }



    public static class BytePreviousValueAggFunction extends PreviousValueAggFunction<Byte> {

        @Override
        public TypeInformation<Byte> getResultType() {
            return Types.BYTE;
        }
    }


    public static class LongPreviousValueAggFunction extends PreviousValueAggFunction<Long> {

        @Override
        public TypeInformation<Long> getResultType() {
            return Types.LONG;
        }
    }


    public static class ShortPreviousValueAggFunction extends PreviousValueAggFunction<Short> {

        @Override
        public TypeInformation<Short> getResultType() {
            return Types.SHORT;
        }
    }


    public static class IntPreviousValueAggFunction extends PreviousValueAggFunction<Integer> {

        @Override
        public TypeInformation<Integer> getResultType() {
            return Types.INT;
        }
    }





    public static class FloatPreviousValueAggFunction extends PreviousValueAggFunction<Float> {

        @Override
        public TypeInformation<Float> getResultType() {
            return Types.FLOAT;
        }
    }



    public static class DoublePreviousValueAggFunction extends PreviousValueAggFunction<Double> {

        @Override
        public TypeInformation<Double> getResultType() {
            return Types.DOUBLE;
        }
    }


    public static class BooleanPreviousValueAggFunction extends PreviousValueAggFunction<Boolean> {

        @Override
        public TypeInformation<Boolean> getResultType() {
            return Types.BOOLEAN;
        }
    }


//    public static class DecimalPreviousValueAggFunction extends PreviousValueAggFunction<Decimal> {
//
//        private DecimalTypeInfo decimalTypeInfo;
//
//        public DecimalPreviousValueAggFunction(DecimalTypeInfo decimalTypeInfo) {
//            this.decimalTypeInfo = decimalTypeInfo;
//        }
//
//        public DecimalPreviousValueAggFunction() {
//            this.decimalTypeInfo = new DecimalTypeInfo(20,6);
//        }
//
//        public void accumulate(PreviousValue<Decimal> acc, Decimal value , int length , Object defaultValue) {
//            super.accumulate(acc, value, length, defaultValue);
//        }
//
//        public void accumulate(PreviousValue<Decimal> acc, Decimal value , int length) {
//            super.accumulate(acc, value, length);
//        }
//
//        @Override
//        public TypeInformation<Decimal> getResultType() {
//            return decimalTypeInfo;
//        }
//    }
//
//
//
//    public static class StringPreviousValueAggFunction extends PreviousValueAggFunction<BinaryString> {
//
//        @Override
//        public TypeInformation<BinaryString> getResultType() {
//            return BinaryStringTypeInfo.INSTANCE;
//        }
//
//        public void accumulate(PreviousValue<BinaryString> acc, BinaryString value, int length , Object defaultValue) {
//            if (value != null) {
//                super.accumulate(acc, value.copy(),length, defaultValue);
//            }
//            else
//                super.accumulate(acc, null,length, defaultValue);
//        }
//
//        public void accumulate(PreviousValue<BinaryString> acc, BinaryString value, int length) {
//            if (value != null) {
//                super.accumulate(acc, value.copy(),length);
//            }
//            else
//                super.accumulate(acc, null,length);
//        }
//
//    }


}
