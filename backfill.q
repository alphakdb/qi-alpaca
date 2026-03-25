
.alpaca.BASEURL:"https://data.alpaca.markets/v2/stocks/bars"
.conf.ALPACA_KEY:"PKPHWIVR6N4YU2ON2D5TJDYWB6"
.conf.ALPACA_SECRET:"5HMBNJhRruFq7gBMLrDQuQ3k8xtVbwnxYk44BUBAiMTD"

/ Parse list of bar dicts from REST response into typed table for one symbol
.alpaca.parse:{[sym;bars]
  n:count bars;
  b:flip bars;
  times:"P"$-1_'b`t;
  flip`time`sym`high`low`open`close`volume`vwap`feedtime`tptime!(times;n#sym;9h$b`h;9h$b`l;9h$b`o;9h$b`c;7h$b`v;9h$b`vw;n#.z.p;n#0Np)
  }

/ Download and parse one month for one symbol, handles pagination
.alpaca.fetchmonth:{[sym;interval;ym]
  start:ssr[string`date$ym;".";"-"],"T00:00:00Z";
  end:ssr[string`date$ym+1;".";"-"],"T00:00:00Z";
  url:.alpaca.BASEURL,"?symbols=",string[sym],"&timeframe=",interval,"&start=",start,"&end=",end,"&limit=10000";
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
.alpaca.backfill:{[syms;start;end;interval;hdbpath]
  p:.qi.path hdbpath;
  .alpaca.backfillsym[;start;end;interval;p] each syms;
  {t:.qi.path(x;y;`AlpacaEquityB);`sym xasc t;@[t;`sym;`p#]}[p;]each key[p] where key[p] like"[0-9]*";
  .qi.info"Backfill complete";
  }