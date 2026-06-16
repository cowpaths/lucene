package org.apache.lucene.index;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Utility for mapping segments to "time based buckets" based on the value of a temporal field in the documents they contain.
 */
public class SegmentRoutingUtil {
  static long defaultBucket() {
    return DEFAULT_BUCKET;
  }

  private static String TEMPORAL_FIELD_NAME; // e.g., EventStart

  /**
   * If {@link #TEMPORAL_FIELD_NAME} field not present, use a value from one of these fields as a fallback
   * (in descending order of priority).
   */
  private static String[] FALLBACK_FIELD_NAMES;

  static {
    setProperties(System.getProperty("lucene.temporalField.name"));
  }

  static void setProperties(String spec) {
    if (spec == null) {
      TEMPORAL_FIELD_NAME = null;
      FALLBACK_FIELD_NAMES = null;
    } else {
      String[] fields = spec.split(", *");
      TEMPORAL_FIELD_NAME = fields[0];
      if (fields.length < 2) {
        FALLBACK_FIELD_NAMES = null;
      } else {
        FALLBACK_FIELD_NAMES = new String[fields.length - 1];
        System.arraycopy(fields, 1, FALLBACK_FIELD_NAMES, 0, FALLBACK_FIELD_NAMES.length);
      }
    }
  }

  public static Long TEMPORAL_ADJUST_NOW = getTemporalAdjustNow();

  private static Long getTemporalAdjustNow() {
    String nowString = System.getProperty("lucene.temporalField.adjustNow");
    if (nowString == null) {
      return null;
    } else {
      try {
        Instant instant = Instant.parse(nowString);
        return instant.toEpochMilli();
      } catch (DateTimeParseException t) {
        throw new IllegalArgumentException("bad temporalField.adjustNow: " + nowString, t);
      }
    }
  }

  // for testing only
  static void setTemporalAdjustNow(Long now) {
    TEMPORAL_ADJUST_NOW = now;
  }

  private static final long[] BOUNDARIES;
  private static final long DEFAULT_BUCKET;

  /**
   * Default boundaries correspond to natural boundaries (-1w, 1d, 1w, 1m, 3m, 6m) with some padding.
   * The initial "-1w" is designed to catch timestamps that are pathologically far in the future.
   * These will be batched in their own bucket. Such segments may naturally age into normal buckets --
   * but this protects against the case where a single doc absurdly far in the future (like, thousands
   * of years) might prevent its associated segment (and subsequent merged segments) from <i>ever</i>
   * aging out of the "most recent" bucket.
   */
  private static final long[] DEFAULT_BOUNDARIES = new long[] {-9, 3, 9, 32, 94, 184}; // <- days (converted to millis)

  static {
    for (int i = DEFAULT_BOUNDARIES.length - 1; i >= 0; i--) {
      // convert days to millis
      DEFAULT_BOUNDARIES[i] = TimeUnit.DAYS.toMillis(DEFAULT_BOUNDARIES[i]);
    }
  }

  static {
    String boundariesSpec = System.getProperty("lucene.temporalField.boundaries");
    long[] array;
    long last;
    if (boundariesSpec == null) {
      array = DEFAULT_BOUNDARIES;
      last = array[0]; // we can guarantee no AIOOBE
    } else {
      try {
        array = Arrays.stream(boundariesSpec.split(", *")).mapToLong((v) -> TimeUnit.DAYS.toMillis(Long.parseLong(v))).toArray();
        last = array[0]; // maybe AIOOBE
      } catch (Throwable t) {
        throw new IllegalArgumentException("bad boundariesSpec: " + boundariesSpec, t);
      }
    }
    long defaultBucket = last;
    for (int i = 1; i < array.length; i++) {
      long v = array[i];
      if (v <= last) {
        throw new IllegalArgumentException("boundariesSpec must be in-order; found: " + boundariesSpec);
      }
      if (defaultBucket <= 0) {
        // ideally the default bucket will be the smallest _positive_ bucket
        defaultBucket = v;
      }
      last = v;
    }
    BOUNDARIES = array;
    DEFAULT_BUCKET = defaultBucket;
  }

  static long mapToBucket(Iterable<? extends IndexableField> doc) {
    return mapToBucket(doc, TEMPORAL_ADJUST_NOW != null ? TEMPORAL_ADJUST_NOW : System.currentTimeMillis(), defaultBucket());
  }

  private static long mapToBucket(Iterable<? extends IndexableField> doc, long now, long defaultBucket) {
    if (TEMPORAL_FIELD_NAME == null) {
      return defaultBucket;
    } else {
      int fallbackIdx;
      IndexableField[] fallbacks;
      if (FALLBACK_FIELD_NAMES == null) {
        fallbackIdx = -1;
        fallbacks = null;
      } else {
        fallbackIdx = FALLBACK_FIELD_NAMES.length - 1;
        fallbacks = new IndexableField[FALLBACK_FIELD_NAMES.length];
      }
      for (IndexableField f : doc) {
        String name = f.name();
        if (TEMPORAL_FIELD_NAME.equals(name)) {
          return mapToBucket(f.numericValue().longValue(), now);
        } else if (FALLBACK_FIELD_NAMES != null) {
          for (int i = fallbackIdx; i >= 0; i--) {
            if (FALLBACK_FIELD_NAMES[i].equals(name)) {
              fallbacks[i] = f;
              fallbackIdx = i - 1;
            }
          }
        }
      }
      if (FALLBACK_FIELD_NAMES != null) {
        for (int i = fallbackIdx + 1, lim = FALLBACK_FIELD_NAMES.length; i < lim; i++) {
          IndexableField f = fallbacks[i];
          if (f != null) {
            return mapToBucket(f.numericValue().longValue(), now);
          }
        }
      }
      return defaultBucket;
    }
  }

  public static long mapToBucket(long timestamp, long now) {
    long diff = now - timestamp;
    for (long v : BOUNDARIES) {
      if (diff <= v) {
        return v;
      }
    }
    return Long.MAX_VALUE;
  }

  private SegmentRoutingUtil() {}
}
