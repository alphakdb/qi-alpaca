.alpaca.BASEURL:"https://data.alpaca.markets/v2/stocks/bars"

.alpaca.normiv:{x^(`1m`5m`15m`30m`1h`2h`4h`6h`12h`1d`1w`1mo!`1Min`5Min`15Min`30Min`1Hour`2Hour`4Hour`6Hour`12Hour`1Day`1Week`1Month)x}

{missing:`ALPACA_KEY`ALPACA_SECRET except key .conf;
 if[count missing;.qi.fatal"Missing Alpaca credentials: ",(", "sv string missing)," -- run: qbt auth set alpaca --key YOUR_API_KEY --secret YOUR_SECRET"]}`


.alpaca.hdb_dir:{
  $[.qi.isproc;
    .qi.path(.conf.DATA;.proc.self.stackname;`hdb;.proc.self.options`hdb);
    .qi.path .conf.ALPACA_HDB] /TODO
  }

/ Parse list of bar dicts from REST response into typed table for one symbol
.alpaca.parse:{[sym;bars]
  n:count bars;
  b:flip bars;
  times:"P"$-1_'b`t;
  flip`time`sym`open`high`low`close`vwap`volume`feedtime`tptime!(times;n#sym;9h$b`o;9h$b`h;9h$b`l;9h$b`c;9h$b`vw;7h$b`v;n#.z.p;n#0Np)
  }

/ Download and parse one month for one symbol, handles pagination
.alpaca.fetchmonth:{[sym;interval;ym]
  start:ssr[string`date$ym;".";"-"],"T00:00:00Z";
  end:ssr[string`date$ym+1;".";"-"],"T00:00:00Z";
  url:.alpaca.BASEURL,"?symbols=",string[sym],"&timeframe=",string[.alpaca.normiv interval],"&start=",start,"&end=",end,"&limit=10000";
  hdrs:"-H \"APCA-API-KEY-ID: ",.conf.ALPACA_KEY,"\" -H \"APCA-API-SECRET-KEY: ",.conf.ALPACA_SECRET,"\"";
  .qi.info"Fetching ",string[sym]," ",string ym;
  acc:first{not ""~x 1}
    {[s;hdrs;sym;baseurl]
      r:.[{.j.k raze system"curl -sf --max-time 120 ",y," \"",x,"\""};
          (s 1;hdrs);
          {.qi.error"Fetch failed: ",x;`bars`next_page_token!((`$())!();::)}];
      bars:$[sym in key r`bars;r[`bars;sym];()];
      newacc:$[0<count bars;(s 0),enlist .alpaca.parse[sym;bars];s 0];
      t:$[`next_page_token in key r;r`next_page_token;::];
      nxt:"";
      if[not(t~(::))|(t~"");nxt:baseurl,"&page_token=",raze{$[x="+";"%2B";x="/";"%2F";enlist x]}each t];
      (newacc;nxt)
      }[;hdrs;sym;url]/(();url);
  $[0=count acc;();raze acc]
  }

/ Persistent index of completed (sym;date) — O(1) skip check
.alpaca.IDXFILE:`alpaca_backfilled;

.alpaca.rebuildidx:{[hdbpath]
  hdbpath:hsym .qi.tosym hdbpath;
  .qi.info"Building alpaca backfill index from HDB...";
  empty:([]sym:`$();interval:`$();date:`date$());
  idxFile:.qi.path(hdbpath;.alpaca.IDXFILE);
  
  s:.qi.path(hdbpath;`sym);
  if[not .qi.exists s;idxFile set empty;:empty];
  
  symenum:get s;
  dparts:key[hdbpath] where (key hdbpath) like "[0-9]*";
  if[not count dparts;idxFile set empty;:empty];

  rows:raze{[hdbpath;symenum;dt]
    targetDir:.qi.path(hdbpath;dt);
    / Fixed k1 assignment order
    k1:key targetDir;
    tnames:k1 where k1 like "AlpacaEquityB*";
    
    raze{[hdbpath;symenum;dt;tname]
      p:.qi.path(hdbpath;dt;tname;`sym);
      if[not .qi.exists p;:()];
      syms:distinct symenum get p;
      / Extract interval (13 chars in AlpacaEquityB)
      intv:`$13_string tname; 
      ([]sym:syms;interval:count[syms]#intv;date:count[syms]#`date$string dt)
    }[hdbpath;symenum;dt;] each tnames
  }[hdbpath;symenum;] each dparts;
  
  idx:$[count rows;rows;empty];
  idxFile set idx;
  idx
 }

.alpaca.loadidx:{[hdbpath]
  p:.qi.path(hdbpath;.alpaca.IDXFILE);
  $[.qi.exists p;get p;.alpaca.rebuildidx hdbpath]
  }

/ Write one day's rows to HDB partition
.alpaca.writepart:{[hdbpath;interval;date;tbl]
  tname:`$ "AlpacaEquityB", string interval;
  .qi.os.ensuredir .qi.path(hdbpath;`$string date);
  partpath:.qi.path(hdbpath;`$string date;tname);
  .[.qi.path(partpath;`);();,;.Q.en[hdbpath;tbl]];
  .qi.info string[date]," ",string[count tbl]," rows written to ",string tname;
 }

/ Backfill month by month for one symbol
.alpaca.backfillsym:{[s;start;end;int;hdbpath]
  .qi.info"Backfilling ",string[s]," ",string[int]," ",string[start]," to ",string end;
  
  .alpaca.IDX:.alpaca.loadidx hdbpath;
  donedts:exec date from .alpaca.IDX where sym=s,interval=int;

  allmos:distinct `month$start+til 1+"i"$end-start;
  missing_mos:allmos except distinct `month$donedts;
  if[not count missing_mos;
    .qi.info"Already fully backfilled ",string[s]," ",string[int];
    :donedts where donedts within(start;end)
  ];
  .qi.info"Missing data found. Fetching ",string[count missing_mos]," months...";

  raze {[s;int;hdbpath;start;end;donedts;ym]
    tbl:.alpaca.fetchmonth[s;int;ym];
    if[not count tbl; :`date$()];
    
    dts:distinct`date$tbl`time;
    dts:dts where not (dts mod 7) in 0 1;  / drop weekends (0=Sat,1=Sun)
    dts:dts where not dts in donedts;
    
    if[count dts;
      {[hdbpath;int;tbl;dt]
        .alpaca.writepart[hdbpath;int;dt;select from tbl where(`date$time)=dt]
      }[hdbpath;int;tbl;] each dts;
      
      .alpaca.IDX,:([]sym:count[dts]#s;interval:count[dts]#int;date:dts);
      (.qi.path(hdbpath;.alpaca.IDXFILE)) set .alpaca.IDX
    ];
    
    dts where dts within(start;end)
  }[s;int;hdbpath;start;end;donedts;] each missing_mos
 }

/ Backfill all symbols, apply sort and p# at end
.alpaca.backfill:{[syms;start;end;interval;hdbpath]
  interval:.alpaca.normiv interval;
  p:.qi.path hdbpath;
  tname:`$ "AlpacaEquityB", string interval;
  / Run backfill for each symbol
  .alpaca.backfillsym[;start;end;interval;p] each syms;
  / Corrected sort/attr logic using specific tname
  dparts:key[p] where (key p) like "[0-9]*";
  {[p;tname;d]
    t:.qi.path(p;d;tname);
    if[.qi.exists t; `sym xasc t; @[t;`sym;`p#]];
  }[p;tname;] each dparts;
  if[.qi.isproc;
    $[null h:.ipc.conn hdb:.qi.tosym .proc.self.options`hdb;
      .qi.info"Could not connect to ",string[hdb];
      [.qi.info"Reloading ",string hdb; h"reload[]"]]];
  .qi.info"Backfill complete for ",string tname;
  .Q.chk p;
  tname
 }