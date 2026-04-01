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
      (newacc;$[t~(::);"";baseurl,"&page_token=",t])
      }[;hdrs;sym;url]/(();url);
  $[0=count acc;();raze acc]
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
  {[sym;interval;hdbpath;ym]
    tbl:.alpaca.fetchmonth[sym;interval;ym];
    if[not count tbl;:()];
    {[hdbpath;tbl;dt].alpaca.writepart[hdbpath;dt;select from tbl where(`date$time)=dt]
      }[hdbpath;tbl;] each distinct`date$tbl`time
    }[sym;interval;hdbpath;] each distinct`month$start+til 1+end-start;
  }

/ Backfill all symbols, apply sort and p# at end
.alpaca.backfill:{[syms;start;end;interval]
  p:.alpaca.hdb_dir[];
  .alpaca.backfillsym[;start;end;interval;p]each syms;
  {[p;d]t:.qi.path(p;d;`AlpacaEquityB);if[.qi.exists t;`sym xasc t;@[t;`sym;`p#]]}[p;]each key[p] where key[p] like"[0-9]*";
  .Q.chk p;
  if[.qi.isproc;
    $[null h:.ipc.conn hdb:.qi.tosym .proc.self.options`hdb;
      .qi.info"Could not connect to ",string[hdb]," to initiate reload";
      [.qi.info"Initiating reload on ",string hdb;h"reload[]"]]];
  .qi.info"Backfill complete";
  }