package com.benefitj.nio.server;

public enum EventType {
  /**
   * 注册
   */
  REGISTER,
  /**
   * 取消注册
   */
  UNREGISTER,
  /**
   * 连接
   */
  CONNECTED,
  /**
   * 被关闭
   */
  CLOSE,
  /**
   * 可读取
   */
  READABLE,
  /**
   * 可写入
   */
  WRITABLE,
  /**
   * 异常
   */
  EXCEPTION;
}
