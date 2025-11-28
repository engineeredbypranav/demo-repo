/ rte.q - Real Time Engine with Diagnostic Capture

/ 0. Map .u.upd
.u.upd:upd;

/ 1. Define Strict Schema (Target)
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$()
 );

/ 1b. Define Debug Schema (Accepts Anything)
debugRaw:([] 
    c0:(); 
    c1:(); 
    c2:(); 
    c3:()
 );

/ Live Calculation Table
liveAvgTable:([sym:`symbol$()] 
    time:`timespan$();
    avgBid:`float$(); 
    avgAsk:`float$()
 );

/ 2. Analytics Function
getBidAskAvg:{[st;et;granularity;s]
    s:(),s;
    if[0=count s; :([] sym:`symbol$(); avgBid:`float$(); avgAsk:`float$())];
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; :([] sym:`symbol$(); avgBid:`float$(); avgAsk:`float$())];

    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    
    raw:select sym, time, Bid, Ask from chunkStoreKalmanPfillDRA 
        where sym in s, time within (st;et);
    
    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function (Robust Casting Mode)
upd:{[t;x]
    / Capture Raw Message
    lastRawMsg::x;

    / A. SLICE & CAST (The Fix for 'mismatch')
    / We explicitly cast the incoming columns to the types our table expects.
    / This handles timestamp vs timespan diffs automatically.
    d: 4#x;
    
    / Cast logic: 
    / Col 0 (Time) -> "n" (timespan)
    / Col 1 (Sym)  -> "s" (symbol)
    / Col 2 (Bid)  -> "f" (float)
    / Col 3 (Ask)  -> "f" (float)
    fixedData: ("n";"s";"f";"f") $ d;

    / B. DIAGNOSTIC INSERT
    / We still log to debugRaw just in case, but use the raw 'd'
    `debugRaw insert d;

    / C. MAIN INSERT
    toInsert: $ [t=`chunkStoreKalmanPfillDRA; fixedData; x];

    @[{
        x insert y;
        / Success? Run Calc
        runCalc[];
    };(t;toInsert);{[err; data] 
        -1 "!!! INSERT ERROR: ",err;
        -1 "   >> Data types failed even after casting.";
    }[;toInsert]];
 };

/ 4. Calculation Trigger
runCalc:{
    @[{
        now: exec max time from chunkStoreKalmanPfillDRA;
        if[null now; now:.z.n];
        
        / Define 60s window
        st: now - 00:01:00.000;    
        syms: distinct chunkStoreKalmanPfillDRA`sym;
        
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            -1 ">> liveAvgTable updated. Rows: ",string count result;
        ];
    };(::);{ -1 "!!! CALC ERROR: ",x }];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready (Casting Mode).";
