.alpaca.BASEURL:"https://data.alpaca.markets/v2/stocks/bars"

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
  url:.alpaca.BASEURL,"?symbols=",string[sym],"&timeframe=",string[interval],"&start=",start,"&end=",end,"&limit=10000";
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
  .qi.info"Building alpaca backfill index from HDB (one-time)...";
  empty:([]sym:`$();date:`date$());
  s:.qi.path(hdbpath;`sym);
  if[not .qi.exists s;.qi.path(hdbpath;.alpaca.IDXFILE)set empty;:empty];
  symenum:get s;
  dparts:`date$string each k where(k:key hdbpath)like"[0-9]*";
  rows:raze{[hdbpath;symenum;dt]
    p:.qi.path(hdbpath;dt;`AlpacaEquityB;`sym);
    if[not .qi.exists p;:()];
    syms:distinct symenum get p;
    if[not count syms;:()];
    ([]sym:syms;date:count[syms]#dt)
    }[hdbpath;symenum;]each dparts;
  idx:$[count rows;rows;empty];
  .qi.path(hdbpath;.alpaca.IDXFILE)set idx;
  .qi.info"Index built: ",string[count idx]," entries";
  idx
  }

.alpaca.loadidx:{[hdbpath]
  p:.qi.path(hdbpath;.alpaca.IDXFILE);
  $[.qi.exists p;get p;.alpaca.rebuildidx hdbpath]
  }

/ Write one day's rows to HDB partition
.alpaca.writepart:{[hdbpath;date;tbl]
  .qi.os.ensuredir .qi.path(hdbpath;`$string date);
  partpath:.qi.path(hdbpath;`$string date;`AlpacaEquityB);
  .[.qi.path(partpath;`);();,;.Q.en[hdbpath;tbl]];
  .qi.info string[date]," ",string[count tbl]," rows";
  }

/ Backfill month by month for one symbol
.alpaca.backfillsym:{[sym;start;end;interval;hdbpath]
  .qi.info"Backfilling ",string[sym]," interval ",string[start]," to ",string end;
  .alpaca.IDX:.alpaca.loadidx hdbpath;
  {[sym;interval;hdbpath;start;end;ym]
    alldts:til[("d"$ym+1)-"d"$ym]+"d"$ym;
    donedts:exec date from .alpaca.IDX where sym=sym;
    / trading days in this month = dates any sym has been written for
    tradingdts:exec distinct date from .alpaca.IDX where date in alldts;
    if[count[tradingdts]&all tradingdts in donedts;
      .qi.info"Skipping ",string[sym]," ",string[ym],": already backfilled";
      :tradingdts where tradingdts within(start;end)];
    tbl:.alpaca.fetchmonth[sym;interval;ym];
    if[not count tbl;:`date$()];
    dts:distinct`date$tbl`time;
    dts:dts where not dts in donedts;
    {[hdbpath;tbl;dt].alpaca.writepart[hdbpath;dt;select from tbl where(`date$time)=dt]
      }[hdbpath;tbl;] each dts;
    if[count dts;
      .alpaca.IDX,::(([]sym:count[dts]#sym;date:dts));
      .qi.path(hdbpath;.alpaca.IDXFILE)set .alpaca.IDX];
    dts where dts within(start;end)
    }[sym;interval;hdbpath;start;end;] each distinct`month$start+til 1+end-start;
  }

/ Backfill all symbols, apply sort and p# at end
.alpaca.backfill:{[syms;start;end;interval;hdbpath]
  p:.qi.path hdbpath;
  .alpaca.backfillsym[;start;end;interval;p]each syms;
  {[p;d]t:.qi.path(p;d;`AlpacaEquityB);if[.qi.exists t;`sym xasc t;@[t;`sym;`p#]]}[p;]each key[p] where key[p] like"[0-9]*";
  if[.qi.isproc;
    $[null h:.ipc.conn hdb:.qi.tosym .proc.self.options`hdb;
      .qi.info"Could not connect to ",string[hdb]," to initiate reload";
      [.qi.info"Initiating reload on ",string hdb;h"reload[]"]]];
  .qi.info"Backfill complete";
  .Q.chk p;
  `AlpacaEquityB
  }
