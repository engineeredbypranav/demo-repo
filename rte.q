/ rte.q - Real Time Engine (Throttled & Verbose)

/ 0. Map .u.upd to upd
.u.upd:upd;

/ Global Counter to throttle logs
updCtr:0;

/ 1. Define Schemas
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$()
 );

liveAvgTable:([sym:`symbol$()] 
    time:`timespan$();
    avgBid:`float$(); 
    avgAsk:`float$()
 );

/ 2. Analytics Function (With Verbose Debugging)
getBidAskAvg:{[st;et;granularity;s]
    -1 "   [CALC STEP] 1. Function called. Range: ",(-3!st)," to ",(-3!et);
    
    s:(),s;
    if[0=count s; -1 "   [CALC WARNING] No symbols provided."; :()];
    
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; -1 "   [CALC WARNING] Time window invalid."; :()];

    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    -1 "   [CALC STEP] 2. Grid generated. Rows: ",string count grid;
    
    raw:select sym, time, Bid, Ask from chunkStoreKalmanPfillDRA 
        where sym in s, time within (st;et);
    -1 "   [CALC STEP] 3. Raw Data Selected. Rows: ",string count raw;
    
    if[0=count raw; -1 "   [CALC WARNING] No raw data found in this window! (Check timestamps)"];

    joined:aj[`sym`time; grid; `sym`time xasc raw];
    -1 "   [CALC STEP] 4. Join Complete. Rows: ",string count joined;

    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function (Throttled)
upd:{[t;x]
    / Increment counter
    updCtr+:1;
    
    / Attempt Insert
    @[{
        / Slice first 4 columns (Time, Sym, Bid, Ask)
        toInsert: 4#y;
        `chunkStoreKalmanPfillDRA insert toInsert;
        
        / ONLY Run Calc every 50 updates to prevent console flooding
        if[0 = (updCtr mod 50);
            -1 ">> upd #",string[updCtr]," received. Running Calc...";
            runCalc[];
        ];

    };(t;x);{[err] -1 "   [INSERT FAIL] ",err}];
 };

/ 4. Calculation Trigger
runCalc:{
    @[{
        / Find the latest time in our data
        now: exec max time from chunkStoreKalmanPfillDRA;
        
        / Safety check: If table is empty, now is null
        if[null now; 
            -1 "   [CALC FAIL] Table chunkStoreKalmanPfillDRA is empty!"; 
            :()
        ];
        
        / Define 60s window relative to DATA TIME
        st: now - 00:01:00.000;    
        syms: distinct chunkStoreKalmanPfillDRA`sym;
        
        -1 "   [CALC STEP] 0. Symbols found in table: ",string count syms;
        
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            
            -1 ">> SUCCESS. Live Table Updated:";
            show liveAvgTable;
            -1 "------------------------------------------------";
        ];
    };(::);{ -1 "   [CALC CRASH] ",x }];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Throttled Output (Every 50 ticks).";
