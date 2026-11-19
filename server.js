const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,FILE=path.join(__dirname,'maps.json');
let maps={};try{maps=JSON.parse(fs.readFileSync(FILE))}catch(e){}
const save=()=>{try{fs.writeFileSync(FILE,JSON.stringify(maps))}catch(e){}};
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type','Access-Control-Allow-Methods':'GET,POST,OPTIONS'};
const J=(res,c,o)=>{res.writeHead(c,{'Content-Type':'application/json',...cors});res.end(JSON.stringify(o))};
const srv=http.createServer((req,res)=>{
  const u=new URL(req.url,'http://x');
  if(req.method==='OPTIONS'){res.writeHead(204,cors);return res.end()}
  if(u.pathname==='/api/map'&&req.method==='POST'){
    let b='';req.on('data',d=>{b+=d;if(b.length>2e5)req.destroy()});
    req.on('end',()=>{try{const o=JSON.parse(b);if(!o.name||!o.code||!o.ph)throw 0;const k=String(o.name).toLowerCase();
      if(maps[k]&&maps[k].ph!==o.ph)return J(res,403,{});
      maps[k]={name:o.name,code:o.code,ph:o.ph,tiles:o.tiles||[]};save();J(res,200,{ok:1})}catch(e){J(res,400,{})}});
    return;
  }
  if(u.pathname==='/api/map'){
    const q=(u.searchParams.get('q')||'').toLowerCase(),ph=u.searchParams.get('ph');
    const m=maps[q]||Object.values(maps).find(x=>x.code.toLowerCase()===q);
    if(!m)return J(res,404,{});if(m.ph!==ph)return J(res,403,{});return J(res,200,m);
  }
  fs.readFile(path.join(__dirname,'index.html'),(e,d)=>{
    if(e){res.writeHead(404);return res.end('no index.html')}
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});res.end(d);
  });
});
const wss=new WebSocketServer({server:srv}),rooms=new Map();
function leave(ws){const r=rooms.get(ws.room);if(r){r.delete(ws);if(!r.size)rooms.delete(ws.room)}ws.room=null}
wss.on('connection',ws=>{
  ws.isAlive=true;ws.on('pong',()=>{ws.isAlive=true});
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.t==='join'){
      leave(ws);const name=String(m.room||'').slice(0,40);if(!name)return;
      if(!rooms.has(name))rooms.set(name,new Set());
      const r=rooms.get(name);if(r.size>=8)return ws.close();
      ws.room=name;ws.state=null;r.add(ws);
    }else if(!ws.room)return;
    else if(m.t==='pres'){ws.state=m.s}
    else if(m.t==='emit'){
      const r=rooms.get(ws.room),msg=JSON.stringify({t:'emit',topic:m.topic,data:m.data});
      r&&r.forEach(c=>{if(c!==ws&&c.readyState===1)c.send(msg)});
    }
  });
  ws.on('close',()=>leave(ws));
});
setInterval(()=>rooms.forEach(r=>r.forEach(c=>{
  if(c.readyState!==1)return;const list=[];
  r.forEach(o=>{if(o!==c&&o.state)list.push(o.state)});
  c.send(JSON.stringify({t:'peers',list}));
})),100);
setInterval(()=>wss.clients.forEach(ws=>{if(!ws.isAlive)return ws.terminate();ws.isAlive=false;ws.ping()}),30000);
srv.listen(PORT,()=>console.log('RAFT server on '+PORT));
