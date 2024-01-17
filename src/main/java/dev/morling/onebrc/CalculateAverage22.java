/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class CalculateAverage22 {

    private static final String FILE = "./measurements.txt";
    private static final int THREADS = 115;
    private static final int BUFFER_SIZE = 120_000_000;

    static class Holder {
        char[] name;
        Double value;
        int count;
        Double max;
        Double min;

        public Holder(char[] name, Double value, int count, Double max, Double min) {
            this.name = name;
            this.value = value;
            this.count = count;
            this.max = max;
            this.min = min;
        }

        public Holder() {
            this(null, 0.0, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        }

        char[] name() {
            return name;
        }

        Holder merge(Holder holder2) {
            this.name = holder2.name;
            this.count += holder2.count;
            if (holder2.max > this.max) {
                this.max = holder2.max;
            }
            if (holder2.min < this.min) {
                this.min = holder2.min;
            }
            this.value += holder2.value;
            return this;
        }

        @Override
        public String toString() {
            return new String(name) + "=" + min + "/" + String.format("%.1f", value / count) + "/" + max;
        }
    }

    static class Chars implements Comparable<Chars>{
        char[] buff;
        static Chars wrap(char[] c){
            Chars tmp = new Chars();
            tmp.buff = c;
            return tmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Chars chars = (Chars) o;
            return Arrays.equals(buff, chars.buff);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(buff);
        }

        @Override
        public int compareTo(Chars o) {
            for(int i = 0; i < Math.min(this.buff.length, o.buff.length); i++){
                if(this.buff[i] != o.buff[i]){
                    return this.buff[i] - o.buff[i];
                }
            }

            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        long testStart = System.currentTimeMillis();

        RandomAccessFile file = new RandomAccessFile(new File(FILE), "r");
        FileChannel channel = file.getChannel();
        Queue<MappedByteBuffer> chunks = new ArrayDeque<>();

        for (int i = 0; i < channel.size() / BUFFER_SIZE + 1; i++) {
            long positionStart = (long) i * BUFFER_SIZE;
            long sizeToRead = channel.size() - ((long) i) * BUFFER_SIZE;
            sizeToRead = Math.min(BUFFER_SIZE, sizeToRead);

            MappedByteBuffer buff = channel.map(FileChannel.MapMode.READ_ONLY, positionStart, sizeToRead);
            chunks.add(buff);
        }

        List<Map<Chars, Holder>> resultMaps = Collections.synchronizedList(new ArrayList<>(32));
        LinkedList<Thread> threads = new LinkedList<>();
        int index = 0;
        int size = chunks.size();
        while (chunks.size() > 0) {
            var c = chunks.poll();
            final int indexTostart = index++;
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    HashMap<Chars, Holder> r = processChunk(c, indexTostart);
                    resultMaps.add(r);
                } catch (CharacterCodingException e) {
                    throw new RuntimeException(e);
                }

            }));

            if (threads.size() == THREADS) {
                for (var t : threads) {
                    t.join();
                }
                threads.clear();
            }

        }

        for (var t : threads) {
            t.join();
        }

        //process remainders
        Map<Chars, Holder> first = resultMaps.get(0);
        for (int i = 0; i < index; i++) {
            String line = remainders[i] + (startingParts[i + 1] == null ? "" : startingParts[i + 1]);

            if (line.isBlank()) {
                continue;
            }

            String[] split = line.split(";");
            double v = Double.parseDouble(split[1]);
            Holder newHolder = new Holder(split[0].toCharArray(), v, 1, v, v);
            Holder tmp = first.get(newHolder.name);
            if (tmp != null) {
                first.put(Chars.wrap(newHolder.name), tmp.merge(newHolder));
            } else {
                first.put(Chars.wrap(newHolder.name), newHolder);
            }
        }


        ///merge all maps

        for (int i = 1; i < resultMaps.size(); i++) {
            for (var h : resultMaps.get(i).values()) {
                Holder tmp = first.get(h.name);
                if (tmp != null) {
                    first.put(Chars.wrap(h.name), tmp.merge(h));
                } else {
                    first.put(Chars.wrap(h.name), h);
                }
            }
        }




        System.out.print("{");
        String resultString = new TreeMap<>(first).values().stream().map(Holder::toString).collect(Collectors.joining(", "));
        System.out.print(resultString);
        System.out.print("}\n");

    }

    static String[] remainders = new String[5000];
    static String[] startingParts = new String[5000];

    static HashMap<Chars, Holder> processChunk(MappedByteBuffer chunk, int index) throws CharacterCodingException {
        HashMap<Chars, Holder> resultsMap = new HashMap<>();
        char[] buff = new char[128];
        int cIndex = 0;

        CharBuffer chars = StandardCharsets.UTF_8.newDecoder().decode(chunk);
        if (index != 0) {
            while (true) {
                char c = chars.get();
                if (c == '\n') {
                    break;
                }
                buff[cIndex++] = c;
            }

            startingParts[index] = String.valueOf(buff, 0, cIndex);
            cIndex = 0;
        }
        char[] tmpName = null;
        do {
            char c1 = chars.get();
            if(c1 == ';'){
                tmpName = Arrays.copyOf(buff, cIndex);
                cIndex = 0;
            }
            if (c1 == '\n') {
                Chars key = Chars.wrap(Arrays.copyOfRange(buff, 1, cIndex));
                double v = Double.parseDouble(new String(key.buff));

                Holder newHolder = new Holder(tmpName, v, 1, v, v);
                Holder tmp = resultsMap.get(key);
                if (tmp == null) {
                    resultsMap.put(key, newHolder);
                } else {
                    resultsMap.put(key, tmp.merge(newHolder));
                }

                cIndex = 0;
                continue;
            }
            buff[cIndex++] = c1;
        }while (chars.hasRemaining());

        remainders[index] = new String(Arrays.copyOf(buff, cIndex));

        return resultsMap;
    }
}
