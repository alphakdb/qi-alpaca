if[first not enlist(.qi.tostr .qi.getconf[`ENDPOINT;"/v1beta3/crypto/us"])in enlist each("/v2/iex";"/v2/test";"/v1beta3/crypto/us";"/v1beta1/news";"/v1beta1/indicative");
    .qi.fatal"Make Sure Your ENDPOINT Is Entered Correctly! Check The Spelling"]

.qi.import`ipc;
.qi.frompkg[`alpaca;`norm]
.qi.frompkg[`proc;`feed]
.qi.frompkg[`alpaca;`backfill]

.feed.requirekey @/:`ALPACA_KEY`ALPACA_SECRET

url:.qi.tosym .qi.getconf[`url;`:wss://stream.data.alpaca.markets:443]
header:"GET ",(feed:.qi.tostr .qi.getconf[`ENDPOINT;"/v1beta3/crypto/us"])," HTTP/1.1\r\n","Host: stream.data.alpaca.markets\r\n","\r\n";
tickers:`$$[sum","=(t:.qi.tostr .qi.getconf[`TICKERS;"ETH/USD"]);","vs t;enlist t]
tname:$[1=count l:`$"Alpaca",/:-1_'@[;0;upper]each","vs feed;first l;l]
ISCRYPT:header like "*crypto*"

TD:"tqb"!$[ISCRYPT;`AlpacaCryptoT`AlpacaCryptoQ`AlpacaCryptoB;
                    `AlpacaEquityT`AlpacaEquityQ`AlpacaEquityB]

ND:"tqb"!$[ISCRYPT;(norm.Ctrades;norm.Cquotes;norm.Cbars);
                    (norm.Etrades;norm.Equotes;norm.Ebars)]

msg.data:{[x] .feed.upd[TD first x`T;ND[first x`T] x]}

msg.status:{[x]
    if[first 402=x`code;.qi.fatal"Ensure ALPACA_KEY & ALPACA_SECRET Are Entered Correctly In .conf"];
    if[first 400=x`code;.qi.fatal"Ensure FEED is spelled correctly in .conf! (e.g trades rather than trade?)"];
    if[`connected=msg:first`$x`msg;:neg[.z.w] .j.j`action`key`secret!("auth";.conf.ALPACA_KEY;.conf.ALPACA_SECRET)];
    if[`authenticated~first msg;
        :neg[.z.w].j.j(`action,a)!`subscribe,count[a:`$","vs .qi.getconf[`FEED;"trades,quotes"]]#enlist string tickers]
    }

.z.ws:{{$[(first x`T)in"tqb";msg.data x;msg.status x]}each .j.k x}

start:{.feed.start[header;url]}