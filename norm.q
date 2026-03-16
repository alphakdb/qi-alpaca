\d .alpaca
norm.Etrades:{
  ("P"$-1_x`t; / time stamp 
   `$x`S; / Sym
   "f"$x`p;  / price
   "j"$x`s;  / size
   `$x`x; / exchange
   .z.p;  / feedtime
   0Np)  / tptime
 }

norm.Equotes:{
    ("P"$-1_x`t; /timestamp
    `$x`S; /sym
    `$x`bx; /b exc
    "f"$x`bp; /bprice
    "j"$x`bs; /bsize
    `$x`ax; /aex
    "f"$x`ap; /ap
    "j"$x`as;
    .z.p;
    0Np) /as
 }

norm.Ebars:{
    ("P"$-1_x`t; / Start Time 
    `$x`S; / Sym
    9h$x`o;  / Open
    9h$x`h;  / High
    9h$x`l; / Low
    9h$x`c; / Close
    9h$x`vw; / VWAP
    7h$x`v;
    .z.p;
    0Np)  / Volume
 }

norm.Ctrades:{
  ("P"$-1_x`t; / time stamp 
   `$x`S; / Sym
   "f"$x`p;  / price
   "f"$x`s;  / size
   `$raze x`tks;
   .z.p;
   0Np)
 }

norm.Cquotes:{
  ("P"$-1_x`t; /timestamp
    `$x`S; /sym
    "f"$x`bp; /bprice
    "f"$x`bs; /bsize
    "f"$x`ap; /ap
    "f"$x`as; / as
    .z.p;
    0Np) 
 }

norm.Cbars:{
  ("P"$-1_x`t; / Start Time 
    `$x`S; / Sym
    9h$x`o;  / Open
    9h$x`h;  / High
    9h$x`l; / Low
    9h$x`c; / Close
    9h$x`vw; / VWAP
    7h$x`v; / volume
    "j"$x`n; / Count
    .z.p;
    0Np) 
 }