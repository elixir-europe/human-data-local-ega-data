/*
 * Copyright 2014 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.embl.ebi.ega.dataservice;

//import static uk.ac.embl.ebi.ega.autnservice.EgaSecureAuTNService.executorGroup;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.HashMap;
import uk.ac.embl.ebi.ega.dataservice.endpoints.Service;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;

/**
 *
 * @author asenf
 */
public class EgaSecureDataServiceInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private final HashMap<String, Service> endpointMappings;
    private final DatabaseExecutor dbe;
    
    private final DefaultEventExecutorGroup l, s, r, loc; // long and short lasting requests handled separately
    private final GlobalTrafficShapingHandler globalTrafficShapingHandler;
    
    private final EgaSecureDataService ref;
    
    public EgaSecureDataServiceInitializer(SslContext sslCtx, HashMap mappings, DatabaseExecutor dbe,
            DefaultEventExecutorGroup l, DefaultEventExecutorGroup s, DefaultEventExecutorGroup r,
            GlobalTrafficShapingHandler globalTrafficShapingHandler,
            EgaSecureDataService ref) {
        this.sslCtx = sslCtx;
        this.endpointMappings = mappings;
        this.dbe = dbe;
        this.l = l;
        this.s = s;
        this.r = r;
        this.loc = new DefaultEventExecutorGroup(5);
        this.globalTrafficShapingHandler = globalTrafficShapingHandler;
        
        this.ref = ref;
    }
    
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        // Add SSL handler first to encrypt and decrypt everything.
        // In this example, we use a bogus certificate in the server side
        // and accept any invalid certificates in the client side.
        // You will need something more complicated to identify both
        // and server in the real world.
        if (sslCtx != null) {
            ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
        }

        // On top of the SSL handler, add the text line codec.
        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HttpObjectAggregator(65536));
        ch.pipeline().addLast(new ChunkedWriteHandler());

        ch.pipeline().addLast(this.loc, new EgaChannelSelector(this.sslCtx!=null,
                                              this.endpointMappings,
                                              this.dbe, 
                                              this.s, this.l, this.r, 
                                              this.ref));
        
        // and then business logic. This is CPU heavy - place in separate thread
        //ch.pipeline().addLast(executorGroup, 
        //                      new EgaSecureDataServiceHandler(this.sslCtx!=null,
        //                                                      this.endpointMappings));
//        ch.pipeline().addLast(new EgaSecureDataServiceHandler(this.sslCtx!=null,
//                                                              this.endpointMappings,
//                                                              this.dbe));

    }
}