package com.benefitj.nio.server;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class EventMessage<T> {

  public static <T> EventMessage<T> copy(EventMessage<T> event) {
    return of(event.getEventType(), event.getChannel(), event.getPayload());
  }

  public static <T> EventMessage<T> of(EventType eventType, SocketChannel channel) {
    return of(eventType, channel, null);
  }

  public static <T> EventMessage<T> of(EventType eventType, SocketChannel channel, T payload) {
    return new EventMessage<>(eventType, channel, payload);
  }

  /**
   * 事件类型
   */
  private EventType eventType;
  /**
   * 通道
   */
  private SocketChannel channel;
  /**
   * 携带的数据
   */
  private T payload;

  public EventMessage(EventType eventType, SocketChannel channel) {
    this.eventType = eventType;
    this.channel = channel;
  }

  public EventMessage(EventType eventType, SocketChannel channel, T payload) {
    this.eventType = eventType;
    this.channel = channel;
    this.payload = payload;
  }

  public EventType getEventType() {
    return eventType;
  }

  public EventMessage<T> setEventType(EventType eventType) {
    this.eventType = eventType;
    return this;
  }

  public SocketChannel getChannel() {
    return channel;
  }

  public EventMessage<T> setChannel(SocketChannel channel) {
    this.channel = channel;
    return this;
  }

  public T getPayload() {
    return payload;
  }

  public EventMessage<T> setPayload(T payload) {
    this.payload = payload;
    return this;
  }

  public SocketAddress getRemoteAddress() {
    return useChannel(SocketChannel::getRemoteAddress);
  }

  public SocketAddress getLocalAddress() {
    return useChannel(SocketChannel::getLocalAddress);
  }

  public <R> R useChannel(SocketChannelFunction<R> func) {
    return useChannel(func, false);
  }

  public <R> R useChannel(SocketChannelFunction<R> func, boolean isThrows) {
    final SocketChannel channel = getChannel();
    if (channel != null) {
      try {
        return func.apply(channel);
      } catch (Exception e) {
        if (isThrows) {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  public EventMessage<T> copy() {
    return copy(this);
  }

  @FunctionalInterface
  public interface SocketChannelFunction<R> {

    /**
     * Applies this function to the given argument.
     *
     * @return the function result
     */
    R apply(SocketChannel channel) throws Exception;
  }
}
