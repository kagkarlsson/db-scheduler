package com.github.kagkarlsson.scheduler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class StreamUtils {

  public static <T> Stream<List<T>> chunkStream(Stream<T> stream, int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("Chunk size must be greater than 0");
    }

    Iterator<T> iterator = stream.iterator();
    return Stream.generate(
            () -> {
              List<T> chunk = new ArrayList<>();
              for (int i = 0; i < chunkSize && iterator.hasNext(); i++) {
                chunk.add(iterator.next());
              }
              return chunk;
            })
        .takeWhile(chunk -> !chunk.isEmpty());
  }
}
