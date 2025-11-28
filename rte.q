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
/ This table helps us see what columns are ACTUALLY coming in
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

/ 3. Upd Function (Diagnostic Mode)
upd:{[t;x]
    / Capture Raw Message for manual inspection
    lastRawMsg::x;

    / SLICE: Take first 4 columns regardless of what they are
    slice: 4#x;

    / DIAGNOSTIC INSERT: Put into debugRaw (no type checks)
    / This allows you to type `debugRaw` and SEE the data even if main insert fails
    `debugRaw insert (slice 0; slice 1; slice 2; slice 3);

    / MAIN INSERT (Strict)
    / We attempt to insert into the typed table
    toInsert: $ [t=`chunkStoreKalmanPfillDRA; slice; x];

    @[{
        x insert y;
        / Success? Run Calc
        runCalc[];
    };(t;toInsert);{[err; data] 
        -1 "!!! INSERT ERROR: ",err;
        -1 "   >> Incoming Types (c0, c1, c2, c3): ",(-3!type each data);
        -1 "   >> Expected Types: 16 (timespan), 11 (symbol), 9 (float), 9 (float)";
        -1 "   >> CHECK 'debugRaw' TABLE TO SEE COLUMN MISMATCH";
    }[;toInsert]];
 };

/ 4. Calculation Trigger
runCalc:{
    @[{
        now: exec max time from chunkStoreKalmanPfillDRA;
        if[null now; now:.z.n];
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

-1 "RTE Ready. If no data appears, check 'debugRaw' table.";
