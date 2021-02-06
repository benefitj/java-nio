package com.benefitj.nio.server;

import com.benefitj.core.IOUtils;
import com.benefitj.core.local.LocalCache;
import com.benefitj.core.local.LocalCacheFactory;
import com.benefitj.pipeline.DefaultPipeline;
import com.benefitj.pipeline.Pipeline;
import com.benefitj.pipeline.PipelineHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NioEventLoop implements Runnable {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final AtomicInteger counter = new AtomicInteger(0);

  private final Thread executor;

  private final Queue<Runnable> q = new LinkedBlockingQueue<>();
  /**
   * 线程执行状态
   */
  private Thread.State state = Thread.State.NEW;
  /**
   * 继续执行标志
   */
  private final AtomicBoolean running = new AtomicBoolean(true);
  /**
   * pipeline
   */
  private final Pipeline pipeline = new DefaultPipeline();

  private Selector selector;

  private final LocalCache<ByteBuffer> cache;

  public NioEventLoop() {
    try {
      this.setSelector(Selector.open());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    this.executor = new Thread(this, "nioEvent-" + counter.getAndIncrement());
    this.cache = LocalCacheFactory.newCache(() -> ByteBuffer.allocateDirect(1024));
  }


  public Pipeline pipeline() {
    return pipeline;
  }

  public Selector getSelector() {
    return selector;
  }

  public void setSelector(Selector selector) {
    this.selector = selector;
  }

  public Thread.State getState() {
    return state;
  }

  private void setState(Thread.State state) {
    this.state = state;
  }

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void run() {
    // 更新执行状态
    this.setState(Thread.State.RUNNABLE);
    logger.info("start sub server......");
    try {
      for (;;) {
        if (!running.get()) {
          break;
        }

        Runnable task;
        while ((task = q.poll()) != null) {
          try {
            task.run();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

        try {
          final Selector s = getSelector();
          if (s.select() > 0) {
            loopSelectedKeys(s, s.selectedKeys());
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } finally {
      IOUtils.closeQuietly(getSelector());
      this.setState(Thread.State.TERMINATED);
      this.running.set(false);
      this.q.clear();
    }
    logger.info("stop sub server......");
  }

  private boolean inEventLoop() {
    return this.executor == Thread.currentThread();
  }

  private void checkExecutor() {
    if (getState() == Thread.State.NEW) {
      synchronized (this) {
        if (getState() == Thread.State.NEW) {
          this.setState(Thread.State.RUNNABLE);
          this.executor.start();
        }
      }
    }

    if (getState() == Thread.State.TERMINATED) {
      throw new IllegalStateException("executor terminated !");
    }
  }

  private void addTask(Runnable task) {
    if (!isRunning()) {
      throw new IllegalStateException("stopped");
    }
    checkExecutor();
    this.q.offer(task);
    getSelector().wakeup();
  }

  /**
   * 循环SelectedKey
   */
  private void loopSelectedKeys(final Selector selector, final Set<SelectionKey> keys) {
    for (;;) {
      Iterator<SelectionKey> itr = keys.iterator();
      if (!itr.hasNext()) {
        break;
      }
      SelectionKey key = itr.next();
      itr.remove();
      processSelectedKey(key, (SocketChannel) key.channel());
    }

    try {
      if (selector.selectNow() > 0) {
        loopSelectedKeys(selector, selector.selectedKeys());
      }
    } catch (IOException e) {
      pipeline().fireNext(EventMessage.of(EventType.EXCEPTION, null, e));
    }
  }

  /**
   * 处理SelectedKey
   */
  private void processSelectedKey(final SelectionKey key, SocketChannel ch) {
    if (!key.isValid()) {
      doUnregister(key, ch);
      return;
    }

    // 连接
    final Pipeline p = pipeline();
    try {
      int readyOps = key.readyOps();
      if (isConnectable(readyOps)) {
        int ops = key.interestOps();
        ops &= ~SelectionKey.OP_CONNECT;
        key.interestOps(ops);
        logger.info("-------- isConnectable");
        p.fireNext(EventMessage.of(EventType.CONNECTED, ch));
      }

      if (isReadable(readyOps)) {
        final ByteBuffer buff = key.attachment() != null ? (ByteBuffer) key.attachment() : cache.get();
        try {
          if (ch.read(buff) > 0) {
            buff.flip();
            p.fireNext(EventMessage.of(EventType.READABLE, ch, buff));
          } else {
            doUnregister(key, ch);
            doClose(ch);
          }
        } catch (SocketException e) {
          doUnregister(key, ch);
          doClose(ch);
        } finally {
          buff.clear();
        }
      }

      if (isWritable(readyOps)) {
        p.fireNext(EventMessage.of(EventType.WRITABLE, ch));
      }
    } catch (Exception e) {
      doException(ch, e);
    }
  }

  /**
   * 注册
   *
   * @param ch 通道
   */
  public void doRegister(SocketChannel ch) {
    if (inEventLoop()) {
      try {
        // 配置非阻塞式
        ch.configureBlocking(false);
        ch.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.TRUE);
        ch.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);

        // 注册操作
        ch.register(getSelector()
            , (SelectionKey.OP_READ
                //| SelectionKey.OP_WRITE // 会产生大量可写入事件
                | SelectionKey.OP_CONNECT)
            , ByteBuffer.allocateDirect(1024));
        // 发送注册事件
        pipeline().fireNext(EventMessage.of(EventType.REGISTER, ch));

        if (ch.isConnected()) {
          doConnect(ch);
        }
      } catch (IOException e) {
        // 发生错误，传递异常事件
        pipeline().fireNext(EventMessage.of(EventType.EXCEPTION, ch, e));
      }
    } else {
      addTask(() -> doRegister(ch));
    }
  }

  public void doConnect(SocketChannel ch) {
    pipeline().fireNext(EventMessage.of(EventType.CONNECTED, ch));
  }

  public void doClose(SocketChannel ch) {
    IOUtils.closeQuietly(ch.socket(), ch);
    pipeline().fireNext(EventMessage.of(EventType.CLOSE, ch));
  }

  private void doUnregister(SelectionKey key, SocketChannel ch) {
    key.cancel();
    pipeline().fireNext(EventMessage.of(EventType.UNREGISTER, ch));
  }

  private void doException(SocketChannel ch, Throwable e) {
    pipeline().fireNext(EventMessage.of(EventType.CLOSE, ch, e));
  }

  public NioEventLoop addLast(PipelineHandler handler) {
    pipeline().addLast(handler);
    return this;
  }

  public NioEventLoop remove(PipelineHandler handler) {
    pipeline().remove(handler);
    return this;
  }

  public static boolean isConnectable(int readyOps) {
    return isOp(readyOps, SelectionKey.OP_CONNECT);
  }

  public static boolean isReadable(int readyOps) {
    return isOp(readyOps, SelectionKey.OP_READ);
  }

  public static boolean isWritable(int readyOps) {
    return isOp(readyOps, SelectionKey.OP_WRITE);
  }

  public static boolean isOp(int readyOps, int validOps) {
    return (readyOps & validOps) != 0;
  }

  public Selector registerNewSelector(final Selector oldSelector, Pipeline pipeline) throws IOException {
    // 取消selector的订阅
    final Selector newSelector = Selector.open();
    Set<SelectionKey> keys = oldSelector.keys();
    for (SelectionKey key : keys) {
      final SelectableChannel channel = key.channel();
      try {
        channel.register(newSelector, key.interestOps());
      } catch (ClosedChannelException e) {
        IOUtils.closeQuietly(channel);
        pipeline.fireNext(EventMessage.of(EventType.CLOSE, (SocketChannel) channel, e));
      } finally {
        key.cancel();
      }
    }
    return newSelector;
  }

}