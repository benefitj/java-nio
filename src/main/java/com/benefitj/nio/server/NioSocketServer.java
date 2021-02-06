package com.benefitj.nio.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;

public class NioSocketServer {
  public static void main(String[] args) throws IOException {
    final Logger logger = LoggerFactory.getLogger(NioSocketServer.class);
    final SelectorProvider provider = SelectorProvider.provider();
    try (final ServerSocketChannel ssc = provider.openServerSocketChannel();) {
      ssc.configureBlocking(false);
      ServerSocket socket = ssc.socket();
      socket.setReuseAddress(false);
      socket.setReceiveBufferSize(1024 << 4);
      socket.bind(new InetSocketAddress(8088));

      logger.info("server started......");

      final NioEventLoop eventLoop = new NioEventLoop();
      eventLoop.addLast(new TypeConvertHandlerAdapter());
      eventLoop.addLast(new LoggedHandlerAdapter());

      // 订阅selector
      Selector bossSelector = Selector.open();
      ssc.register(bossSelector, SelectionKey.OP_ACCEPT);

      for (;;) {
        if (!eventLoop.isRunning()) {
          break;
        }

        if (bossSelector.select() > 0) {
          final Set<SelectionKey> keys = bossSelector.selectedKeys();
          final Iterator<SelectionKey> itr = keys.iterator();
          while (itr.hasNext()) {
            final SelectionKey key = itr.next();
            itr.remove();
            if (key.isAcceptable()) {
              // 注册
              SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
              eventLoop.doRegister(channel);
            }
          }
        }
      }

      logger.info("server stopped......");
    }
  }

}
