const net=require('net'),crypto=require('crypto'),os=require('os'),fs=require('fs'),http=require('http');
const WS_PORT=process.env.PORT||8765,HTTP_PORT=process.env.HTTP_PORT||8080,clients=new Set();
const DIR=__dirname;

function getIP(){const n=os.networkInterfaces();for(const k of Object.keys(n))for(const i of n[k])if(i.family==='IPv4'&&!i.internal)return i.address;return'localhost';}

http.createServer((req,res)=>{
  if(req.url==='/'||req.url==='/controller.html'){
    try{const f=fs.readFileSync(DIR+'/controller.html','utf8');res.writeHead(200,{'Content-Type':'text/html'});res.end(f);}
    catch(e){res.writeHead(404);res.end('controller.html not found');}
  } else {res.writeHead(404);res.end('Not found');}
}).listen(HTTP_PORT,'0.0.0.0',()=>console.log('HTTP :'+HTTP_PORT));

function handshake(sock,str){const m=str.match(/Sec-WebSocket-Key:\s*([^\r\n]+)/i);if(!m){sock.destroy();return false;}const a=crypto.createHash('sha1').update(m[1].trim()+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nAccess-Control-Allow-Origin: *\r\nSec-WebSocket-Accept: '+a+'\r\n\r\n');return true;}
function encode(t){const p=Buffer.from(t,'utf8'),l=p.length;let h;if(l<126){h=Buffer.alloc(2);h[0]=0x81;h[1]=l;}else{h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(l,2);}return Buffer.concat([h,p]);}
function broadcast(src,t){const f=encode(t);for(const s of clients)if(s!==src&&!s.sock.destroyed)s.sock.write(f);}

net.createServer(sock=>{
  const st={sock,up:false,buf:Buffer.alloc(0)};
  sock.on('data',chunk=>{
    st.buf=Buffer.concat([st.buf,chunk]);
    if(!st.up){const s=st.buf.toString('latin1'),i=s.indexOf('\r\n\r\n');if(i===-1)return;st.up=handshake(sock,s.slice(0,i));st.buf=st.buf.slice(i+4);if(!st.up)return;clients.add(st);console.log('[+] '+clients.size);}
    while(st.buf.length>=2){const b0=st.buf[0],b1=st.buf[1],op=b0&0xf,masked=(b1&0x80)!==0;let pl=b1&0x7f,hl=2;if(pl===126){if(st.buf.length<4)return;pl=st.buf.readUInt16BE(2);hl=4;}else if(pl===127){if(st.buf.length<10)return;pl=Number(st.buf.readBigUInt64BE(2));hl=10;}if(masked)hl+=4;if(st.buf.length<hl+pl)return;if(op===8){sock.destroy();return;}if(op===9){const pg=Buffer.alloc(2);pg[0]=0x8a;pg[1]=0;sock.write(pg);}if(op===1||op===2){let pay;if(masked){const ms=hl-4,mk=st.buf.slice(ms,ms+4),r=st.buf.slice(hl,hl+pl);pay=Buffer.alloc(pl);for(let i=0;i<pl;i++)pay[i]=r[i]^mk[i%4];}else pay=st.buf.slice(hl,hl+pl);const txt=pay.toString('utf8');if(txt){console.log('[→] '+txt);broadcast(st,txt);}}st.buf=st.buf.slice(hl+pl);}
  });
  sock.on('close',()=>{clients.delete(st);console.log('[-] '+clients.size);});
  sock.on('error',()=>clients.delete(st));
}).listen(WS_PORT,'0.0.0.0',()=>console.log('WS :'+WS_PORT));
