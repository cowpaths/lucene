package org.apache.lucene.search;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;

import java.io.IOException;
import java.util.function.Supplier;

public class PooledPostingsUtil {
  private static ThreadLocal<PostingsEnumProvider> poolPostingsEnumPerThread = new ThreadLocal<>();
  private static Supplier<PostingsEnumProvider> supplier;

  public static void setProviderSupplier(Supplier<PostingsEnumProvider> supplier) {
    PooledPostingsUtil.supplier = supplier;
  }
  public static void enablePool() {
    poolPostingsEnumPerThread.set(supplier != null ? supplier.get() : null);
  }

  public static void disablePool() {
    poolPostingsEnumPerThread.remove();
  }

  public static boolean isEnabled() {
    return poolPostingsEnumPerThread.get() != null;
  }


  public static PostingsEnum getPostings(TermsEnum termsEnum, int flags) throws IOException {
    return isEnabled() ? poolPostingsEnumPerThread.get().getPostings(termsEnum, flags) : null;
  }


  public interface PostingsEnumProvider {
    PostingsEnum getPostings(TermsEnum termsEnum, int flags) throws IOException;
  }
}
