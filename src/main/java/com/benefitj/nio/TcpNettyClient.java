package com.benefitj.nio;

import com.benefitj.core.HexUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TcpNettyClient {
  public static void main(String[] args) throws Exception {

    new Thread(() -> startClient(40)).start();

//    TimeUnit.SECONDS.sleep(4);
//
//    // 启动第二个客户端
//    new Thread(() -> startClient(20)).start();
//
//
//    System.err.println("启动第三个客户端......");
//    new Thread(() -> startClient(30)).start();

  }

  private static final Logger logger = LoggerFactory.getLogger(TcpNettyClient.class);

  public static void startClient(int maxCount) {
    final NioEventLoopGroup group = new NioEventLoopGroup();
    final Bootstrap bootstrap = new Bootstrap()
        .group(group)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline()
//                .addLast(new LoggingHandler(LogLevel.DEBUG))
                .addLast(new MessageToByteEncoder<String>() {
                  @Override
                  protected void encode(ChannelHandlerContext ctx, String msg, ByteBuf out) throws Exception {
                    byte[] msgBytes = msg.getBytes();
                    logger.info("send msg: \"" + msg + "\"");
                    out.writeBytes(msgBytes);
                  }
                })
                .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                  @Override
                  public void channelActive(ChannelHandlerContext ctx) throws Exception {
                    super.channelActive(ctx);
                    logger.info("channelActive   remoteAddress: " + ctx.channel().remoteAddress());
                  }

                  @Override
                  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                    super.channelInactive(ctx);
                    logger.info("channelInactive   remoteAddress: " + ctx.channel().remoteAddress());
                  }

                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                    byte[] dest = new byte[msg.readableBytes()];
                    msg.readBytes(dest);
                    logger.info("receive msg: \"" + new String(dest) + "\", dest: " + HexUtils.bytesToHex(dest));
                  }
                });
          }
        })
        .localAddress(0)
        .remoteAddress(new InetSocketAddress("localhost", 8088));

    final CountDownLatch latch = new CountDownLatch(1);

    final Channel channel = bootstrap.connect().addListener(future -> latch.countDown()).channel();
    new Thread(() -> {
      try {
        latch.await();
      } catch (InterruptedException ignore) {/* ignore */}

      int count = maxCount;
      while ((channel.isActive()) && (count-- > 0)) {
        channel.writeAndFlush("write: " + count);
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignore) {/*ignore*/}
      }

      logger.info("5秒后停止客户端");

      channel.writeAndFlush("after 5s to terminal !");

      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {}

      // 关闭客户端
      channel.close();

    }).start();

    try {
      channel.closeFuture()
          .syncUninterruptibly()
          .addListener(future -> logger.info("stop client : " + future.isSuccess()));
    } finally {
      group.shutdownGracefully();
    }
  }

}
