/ rte.q - Real Time Engine (Atomic Trace Mode)

/ 0. Map .u.upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000

/ 1. MANUAL DISPATCHER
.z.ps:{[x]
    func: first x;
    if[func in `upd`.u.upd;
        -1 ">> [SNIFFER] Dispatching 'upd'...";
        upd[x 1; x 2];
        :();
    ];
    -1 "[SNIFFER] Non-upd msg: ",(-3!x);
    value x; 
 };

/ 2. Define Schema
delete rteData from `.;
rteData:([]
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

/ 3. Analytics Function
getBidAskAvg:{[st;et;granularity;s]
    s:(),s;
    if[0=count s; :()];
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; :()];
    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    raw:select sym, time, Bid, Ask from rteData 
        where sym in s, time within (st;et);
    if[0=count raw; :()];
    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 4. Upd Function (Atomic Trace)
upd:{[t;x]
    -1 ">> upd CALLED. Rows: ",string count x;

    @[{
        -1 "   [TRACE] 1. Extracting columns by INDEX (0,1,2,3)...";
        / Get column names dynamically to avoid typo/whitespace issues
        c: cols x;
        -1 "   [TRACE]    Col 0 Name: ",string[c 0];
        -1 "   [TRACE]    Col 1 Name: ",string[c 1];
        
        / Extract raw columns using Table Indexing (safe for Tables)
        rawTime: x[c 0];
        rawSym:  x[c 1];
        rawBid:  x[c 2];
        rawAsk:  x[c 3];
        
        -1 "   [TRACE] 2. Checking Raw Types...";
        -1 "   [TRACE]    Time Type: ",string type rawTime;
        -1 "   [TRACE]    Sym Type:  ",string type rawSym;
        -1 "   [TRACE]    Bid Type:  ",string type rawBid;
        -1 "   [TRACE]    Ask Type:  ",string type rawAsk;

        -1 "   [TRACE] 3. Casting...";
        / Cast individually - if this fails, we know WHICH col is bad
        safeTime: "n"$rawTime;
        -1 "   [TRACE]    Time Cast OK";
        
        safeSym:  "s"$rawSym;
        -1 "   [TRACE]    Sym Cast OK";
        
        safeBid:  "f"$rawBid;
        -1 "   [TRACE]    Bid Cast OK";
        
        safeAsk:  "f"$rawAsk;
        -1 "   [TRACE]    Ask Cast OK";
        
        -1 "   [TRACE] 4. Rebuilding Table...";
        toInsert: flip `time`sym`Bid`Ask!(safeTime; safeSym; safeBid; safeAsk);
        
        -1 "   [TRACE] 5. Inserting...";
        `rteData insert toInsert;
        
        -1 "   [TRACE] 6. Done. Running Calc...";
        runCalc[];

    };(t;x);{[err] 
        -1 "!!! [UPD CRASHED] ",err;
    }];
 };

/ 5. Calculation Trigger
runCalc:{
    @[{
        now: exec max time from rteData;
        if[null now; :()];
        st: now - 00:01:00.000;    
        syms: distinct rteData`sym;
        
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            -1 ">> SUCCESS. Live Table Updated (Rows: ",string[count result],")";
            show liveAvgTable;
            -1 "------------------------------------------------";
        ];
    };(::);{[err] -1 "!!! [CALC CRASH] ",err}];
 };

/ 6. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Atomic Trace Mode.";
