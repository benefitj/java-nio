package com.benefitj.nio.server;

import com.benefitj.core.local.LocalCacheFactory;
import com.benefitj.core.local.LocalMapCache;
import com.benefitj.pipeline.HandlerContext;
import com.benefitj.pipeline.UnboundHandlerAdapter;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class TypeConvertHandlerAdapter extends UnboundHandlerAdapter<EventMessage<?>> {

  private final LocalMapCache<Integer, byte[]> cache = LocalCacheFactory.newWeakMapCache((Function<Integer, byte[]>) byte[]::new);

  public byte[] getBytes(int size) {
    return cache.computeIfAbsent(size);
  }

  @Override
  protected void processPrev0(HandlerContext ctx, EventMessage msg) {
    ctx.fireNext(msg);
  }

  @Override
  protected void processNext0(HandlerContext ctx, EventMessage msg) {
    if (msg.getPayload() instanceof ByteBuffer) {
      EventMessage copy = msg.copy();
      ByteBuffer payload = (ByteBuffer) msg.getPayload();
      byte[] data = getBytes(payload.remaining());
      payload.get(data);
      copy.setPayload(new String(data));
      ctx.fireNext(copy);
    } else if (msg.getPayload() instanceof byte[]) {
      ctx.fireNext(msg.copy().setPayload(new String((byte[]) msg.getPayload())));
    } else {
      ctx.fireNext(msg);
    }
  }
}