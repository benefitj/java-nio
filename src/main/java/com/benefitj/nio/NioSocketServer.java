package com.benefitj.nio;

import com.benefitj.pipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class NioSocketServer {

  private static final Logger logger = LoggerFactory.getLogger(NioSocketServer.class);

  public static void main(String[] args) throws IOException {

    final SelectorProvider provider = SelectorProvider.provider();
    try (final ServerSocketChannel serverChannel = provider.openServerSocketChannel();) {

      serverChannel.configureBlocking(false);
      ServerSocket socket = serverChannel.socket();
      socket.setReuseAddress(false);
      socket.setReceiveBufferSize(1024 << 4);
      socket.bind(new InetSocketAddress(8088));

      logger.info("start server......");

      final NioSocketChannelExecutor executor = new NioSocketChannelExecutor();
      executor.addLast(new LoggedHandlerAdapter());

      // 订阅selector
      Selector bossSelector = Selector.open();
      serverChannel.register(bossSelector, SelectionKey.OP_ACCEPT);

      for(;;) {
        if (!executor.isRunning()) {
          break;
        }

        if (bossSelector.select() > 0) {
          final Set<SelectionKey> keys = bossSelector.selectedKeys();
          final Iterator<SelectionKey> i = keys.iterator();
          while (i.hasNext()) {
            try {
              final SelectionKey key = i.next();
              if (key.isAcceptable()) {
                // 注册
                executor.register(((ServerSocketChannel) key.channel()).accept());
              }
            } finally {
              i.remove();
            }
          }
        }
      }
    }
  }

  public static class NioSocketChannelExecutor implements Runnable {

    private static final AtomicInteger counter = new AtomicInteger(0);

    private final Thread loop = new Thread(this, "nioEvent-" + counter.getAndIncrement());

    private final LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    private final AtomicReference<CountDownLatch> awaitLatch = new AtomicReference<>();
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
    private final DefaultPipeline pipeline = new DefaultPipeline();

    private volatile Selector selector;

    private final ThreadLocal<ByteBuffer> localCache = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(1024));

    public NioSocketChannelExecutor() {
      try {
        this.setSelector(Selector.open());
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

      // 注册
      this.pipeline().addFirst(new TypeConvertHandlerAdapter());
    }


    public DefaultPipeline pipeline() {
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
      this.awaitLatch.getAndSet(null).countDown();
      logger.info("start sub server......");
      try {
        for(;;) {
          if (!running.get()) {
            break;
          }

          Runnable task;
          while ((task = taskQueue.poll()) != null) {
            try {
              task.run();
            } catch (Exception e) {
              pipeline().fireNext(BasicEvent.of(EventType.THROWABLE, null, e));
            }
          }

          final Selector s = getSelector();
          if (s.select() > 0) {
            loopSelectedKeys(s, s.selectedKeys());
          }
        }
      } catch (ClosedSelectorException e) {
        pipeline().fireNext(BasicEvent.of(EventType.CLOSE, null, e));
      } catch (Exception e) {
        pipeline().fireNext(BasicEvent.of(EventType.THROWABLE, null, e));
      } finally {
        this.setState(Thread.State.TERMINATED);
        this.running.set(false);
        closeQuietly(getSelector());
        taskQueue.clear();
      }
      logger.info("stop sub server......");
    }

    /**
     * 注册
     *
     * @param channel 通道
     */
    public void register(SocketChannel channel) {
      checkExecutorState();
      register0(channel);
    }

    private void register0(SocketChannel channel) {
      if (this.loop != Thread.currentThread()) {
        addTask(() -> register0(channel));
      } else {
        try {
          // 配置非阻塞式
          channel.configureBlocking(false);
          channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.TRUE);
          channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);

          logger.info("register before .............");
          // 注册操作
          channel.register(getSelector()
              , (SelectionKey.OP_READ
                  //| SelectionKey.OP_WRITE // 会产生大量可写入事件
                  | SelectionKey.OP_CONNECT)
              , ByteBuffer.allocateDirect(1024));
          logger.info("register after .............");

          // 发送注册事件
          pipeline().fireNext(BasicEvent.of(EventType.REGISTER, channel));

          if (channel.isConnected()) {
            pipeline().fireNext(BasicEvent.of(EventType.CONNECTED, channel));
          }
        } catch (Exception e) {
          Throwable cause = e.getCause();
          if (cause instanceof ClosedChannelException) {
            pipeline().fireNext(BasicEvent.of(EventType.CLOSE, channel, cause));
          } else {
            // 发生错误，传递异常事件
            pipeline().fireNext(BasicEvent.of(EventType.THROWABLE, channel, e));
          }
        }
      }
    }

    private void checkExecutorState() {
      if (getState() == Thread.State.NEW) {
        synchronized (this) {
          if (getState() == Thread.State.NEW) {
            final AtomicReference<CountDownLatch> latch = this.awaitLatch;
            if (latch.compareAndSet(null, new CountDownLatch(1))) {
              try {
                this.setState(Thread.State.BLOCKED);
                this.loop.start();
                latch.get().await();
              } catch (InterruptedException ignore) {/* ignore */}
              finally {
                latch.set(null);
              }
            }
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
      taskQueue.offer(task);
      getSelector().wakeup();
    }

    public void unregister(SocketChannel channel) {
      try {
        logger.info("unregister...");
        pipeline().fireNext(BasicEvent.of(EventType.UNREGISTER, channel));
      } catch (Exception e) {
        pipeline().fireNext(BasicEvent.of(EventType.THROWABLE, channel, e));
      } finally {
        try {
          channel.socket().close();
          channel.close();
        } catch (IOException e) {
          logger.warn("unregister throw: {}", e.getMessage());
        }
      }
    }


    /**
     * 循环SelectedKey
     */
    private void loopSelectedKeys(final Selector selector, final Set<SelectionKey> keys) {
      for(;;) {
        Iterator<SelectionKey> i = keys.iterator();
        if (i.hasNext()) {
          try {
            SelectionKey key = i.next();
            processSelectedKey(selector, key, (SocketChannel) key.channel());
          } finally {
            i.remove();
          }
        } else {
          break;
        }
      }

      try {
        if (selector.selectNow() > 0) {
          loopSelectedKeys(selector, selector.selectedKeys());
        }
      } catch (IOException e) {
        pipeline().fireNext(BasicEvent.of(EventType.THROWABLE, null, e));
      }
    }

    /**
     * 处理SelectedKey
     */
    private void processSelectedKey(Selector selector, final SelectionKey key, SocketChannel channel) {
      if (!key.isValid()) {
        key.cancel();
        pipeline().fireNext(BasicEvent.of(EventType.UNREGISTER, channel));
        return;
      }

      // 连接
      final DefaultPipeline p = pipeline();
      try {
        if (key.isConnectable()) {
          int ops = key.interestOps();
          ops &= ~SelectionKey.OP_CONNECT;
          key.interestOps(ops);
          p.fireNext(BasicEvent.of(EventType.CONNECTED, channel));
        } else if (key.isReadable()) {
          final ByteBuffer buff = key.attachment() != null ? (ByteBuffer) key.attachment() : localCache.get();
          try {
            if (channel.read(buff) > 0) {
              buff.flip();
              p.fireNext(BasicEvent.of(EventType.READABLE, channel, buff));
            } else {
              logger.warn("channel.read() is empty, remote: {}, isConnected: {}, isOpen: {}" +
                      ", readyOps: {}, isBlocking: {}, selectNow: {}"
                  , channel.getRemoteAddress()
                  , channel.isConnected()
                  , channel.isOpen()
                  , key.readyOps()
                  , channel.isBlocking()
                  , selector.selectNow()
              );
              pipeline().fireNext(BasicEvent.of(EventType.CLOSE, channel, null));
              channel.close();
              pipeline().fireNext(BasicEvent.of(EventType.UNREGISTER, channel, null));
              key.cancel();
            }
          } finally {
            buff.clear();
          }
        } else if (key.isWritable()) {
          p.fireNext(BasicEvent.of(EventType.WRITABLE, channel));
        } else {
          logger.warn("其他事件, remote: {}, isConnected: {}, isOpen: {}" +
                  ", readyOps: {}, isBlocking: {}, selectNow: {}"
              , channel.getRemoteAddress()
              , channel.isConnected()
              , channel.isOpen()
              , key.readyOps()
              , channel.isBlocking()
              , selector.selectNow()
          );
        }
      } catch (IOException e) {
        try {
          p.fireNext(BasicEvent.of(EventType.CLOSE, channel, e));
        } finally {
          unregister(channel);
          closeQuietly(key.channel());
        }
      }
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
          closeQuietly(channel);
          pipeline.fireNext(BasicEvent.of(EventType.CLOSE, (SocketChannel) channel, e));
        } finally {
          key.cancel();
        }
      }
      return newSelector;
    }

    public NioSocketChannelExecutor addLast(PipelineHandler handler) {
      pipeline().addLast(handler);
      return this;
    }

    public NioSocketChannelExecutor remove(PipelineHandler handler) {
      pipeline().remove(handler);
      return this;
    }

  }


  public static class TypeConvertHandlerAdapter extends UnboundHandlerAdapter<BasicEvent> {

    private final ThreadLocal<Map<Integer, byte[]>> byteCache = ThreadLocal.withInitial(WeakHashMap::new);
    private final Function<Integer, byte[]> mappingFunction = byte[]::new;

    public byte[] getBuffer(int size) {
      return byteCache.get().computeIfAbsent(size, mappingFunction);
    }

    @Override
    protected void processPrev0(HandlerContext ctx, BasicEvent msg) {
      ctx.fireNext(msg);
    }

    @Override
    protected void processNext0(HandlerContext ctx, BasicEvent msg) {
      if (msg.getPayload() instanceof ByteBuffer) {
        BasicEvent copy = msg.copy();
        ByteBuffer payload = (ByteBuffer) msg.getPayload();
        byte[] data = getBuffer(payload.remaining());
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

  public static class LoggedHandlerAdapter extends UnboundHandlerAdapter<BasicEvent> {

    @Override
    protected void processPrev0(HandlerContext ctx, NioSocketServer.BasicEvent msg) {
      logger.info("eventType: {}, remote: {}, payload: {}, isConnected: {}",
          msg.getEventType(), msg.getRemoteAddress(), msg.getPayload(), msg.useChannel(SocketChannel::isConnected));
    }

    @Override
    protected void processNext0(HandlerContext ctx, NioSocketServer.BasicEvent msg) {
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

      if (msg.getEventType() == NioSocketServer.EventType.READABLE) {
        // 响应
        msg.useChannel(channel -> channel.write(ByteBuffer.wrap("pong...".getBytes())));
      }
    }
  }

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
    THROWABLE;
  }

  public static class BasicEvent<T> {

    public static <T> BasicEvent<T> copy(BasicEvent<T> event) {
      return of(event.getEventType(), event.getChannel(), event.getPayload());
    }

    public static <T> BasicEvent<T> of(EventType eventType, SocketChannel channel) {
      return of(eventType, channel, null);
    }

    public static <T> BasicEvent<T> of(EventType eventType, SocketChannel channel, T payload) {
      return new BasicEvent<>(eventType, channel, payload);
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

    public BasicEvent(EventType eventType, SocketChannel channel) {
      this.eventType = eventType;
      this.channel = channel;
    }

    public BasicEvent(EventType eventType, SocketChannel channel, T payload) {
      this.eventType = eventType;
      this.channel = channel;
      this.payload = payload;
    }

    public EventType getEventType() {
      return eventType;
    }

    public BasicEvent<T> setEventType(EventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public SocketChannel getChannel() {
      return channel;
    }

    public BasicEvent<T> setChannel(SocketChannel channel) {
      this.channel = channel;
      return this;
    }

    public T getPayload() {
      return payload;
    }

    public BasicEvent<T> setPayload(T payload) {
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

    public BasicEvent<T> copy() {
      return copy(this);
    }
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


  public static void closeQuietly(AutoCloseable... cs) {
    for (AutoCloseable c : cs) {
      if (c != null) {
        try {
          c.close();
        } catch (Exception e) {
          logger.warn("close throw: {}", e.getMessage());
        }
      }
    }
  }

}
