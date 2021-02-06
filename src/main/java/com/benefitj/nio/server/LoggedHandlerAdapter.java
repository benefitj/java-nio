package com.benefitj.nio.server;

import com.benefitj.pipeline.HandlerContext;
import com.benefitj.pipeline.UnboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LoggedHandlerAdapter extends UnboundHandlerAdapter<EventMessage<?>> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  protected void processPrev0(HandlerContext ctx, EventMessage msg) {
    logger.info("eventType: {}, remote: {}, payload: {}, isConnected: {}",
        msg.getEventType(), msg.getRemoteAddress(), msg.getPayload(), msg.useChannel(SocketChannel::isConnected));
  }

  @Override
  protected void processNext0(HandlerContext ctx, EventMessage msg) {
    logger.info("eventType: {}, remote: {}, payload: {}, isConnected: {}, isRegistered: {}",
        msg.getEventType(), msg.getRemoteAddress(), msg.getPayload()
        , msg.useChannel(SocketChannel::isConnected)
        , msg.useChannel(SocketChannel::isRegistered));

    if (msg.getPayload() == null) {
      return;
    }

    if (msg.getPayload() instanceof Throwable) {
      ((Throwable) msg.getPayload()).printStackTrace();
    }

    if (msg.getEventType() == EventType.READABLE) {
      // 响应
      msg.useChannel(channel -> channel.write(ByteBuffer.wrap("pong...".getBytes())));
    }
  }
}
