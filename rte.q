/ rte.q - Real Time Engine (Verbose Tracing)

/ 0. Map .u.upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000
/ -------------------

/ 1. Define Schema
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

/ 2. Analytics Function (With TRACE Prints)
getBidAskAvg:{[st;et;granularity;s]
    s:(),s;
    if[0=count s; 
        -1 "   [TRACE] Calc stopping: No symbols provided."; 
        :()
    ];
    
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; 
        -1 "   [TRACE] Calc stopping: Grid count < 1 (Start: ",string[st]," End: ",string[et],")"; 
        :()
    ];

    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    
    raw:select sym, time, Bid, Ask from rteData 
        where sym in s, time within (st;et);

    if[0=count raw; 
        -1 "   [TRACE] Calc stopping: No raw data found in window ",string[st]," - ",string[et]; 
        -1 "   [TRACE] rteData total rows: ",string[count rteData]," (Last time: ",string[last rteData`time],")";
        :()
    ];

    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function
upd:{[t;x]
    / Print entry to prove we are inside
    -1 ">> upd CALLED. Rows: ",string count x;

    / --- STEP 1: PREPARE DATA ---
    d: select time, sym, Bid, Ask from x;
    d: update "n"$time, "s"$sym, "f"$Bid, "f"$Ask from d;

    / --- STEP 2: INSERT ---
    inserted: 0b;
    @[{
        `rteData insert y;
        inserted::1b;
        -1 "   [TRACE] Inserted ",string[count y]," rows into rteData.";
    };(t;d);{[err] 
        -1 "!!! [INSERT FAIL] ",err;
        show meta d;
    }];

    / --- STEP 3: CALC ---
    if[inserted;
        @[{
            runCalc[];
        };(::);{[err] -1 "!!! [CALC FAIL] ",err}];
    ];
 };

/ 4. Calculation Trigger
runCalc:{
    now: exec max time from rteData;
    if[null now; -1 "   [TRACE] runCalc stopping: 'now' is null"; :()];
    
    st: now - 00:01:00.000;    
    syms: distinct rteData`sym;
    
    -1 "   [TRACE] Running getBidAskAvg... (Syms: ",string[count syms],")";
    result: getBidAskAvg[st; now; 00:00:01.000; syms];
    
    if[count result;
        result: update time:now from result;
        `liveAvgTable upsert result;
        -1 ">> SUCCESS. Live Table Updated (Rows: ",string[count result],")";
        show liveAvgTable;
        -1 "------------------------------------------------";
    ];
    
    if[0=count result; -1 "   [TRACE] Result was empty. No update."];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Tracing Active.";
