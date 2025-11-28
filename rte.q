/ rte.q - Real Time Engine (Final Diagnostic)

/ 0. Map .u.upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000
/ -------------------

/ 1. Nuke & Define Schema
/ We delete the table first to ensure a fresh definition
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

/ 2. Analytics Function
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

/ 3. Upd Function (Step-by-Step)
upd:{[t;x]
    
    / --- STEP 1: PREPARE DATA ---
    / We do this outside the trap to see if SELECT fails
    d: select time, sym, Bid, Ask from x;
    
    / Force clean types based on your META screenshot
    d: update "n"$time, "s"$sym, "f"$Bid, "f"$Ask from d;

    / --- STEP 2: INSERT (Trap A) ---
    inserted: 0b;
    @[{
        `rteData insert y;
        inserted::1b;
    };(t;d);{[err] 
        -1 "!!! [INSERT FAIL] ",err;
        -1 "   >> Meta of Data trying to insert:";
        show meta d;
        -1 "   >> Meta of Target Table (rteData):";
        show meta rteData;
    }];

    / --- STEP 3: CALC (Trap B) ---
    / Only run if insert succeeded
    if[inserted;
        @[{
            runCalc[];
        };(::);{[err] -1 "!!! [CALC FAIL] ",err}];
    ];
 };

/ 4. Calculation Trigger
runCalc:{
    now: exec max time from rteData;
    if[null now; :()];
    
    st: now - 00:01:00.000;    
    syms: distinct rteData`sym;
    
    result: getBidAskAvg[st; now; 00:00:01.000; syms];
    
    if[count result;
        / CRITICAL: Ensure 'time' col is added before upsert
        result: update time:now from result;
        
        / UPSERT
        `liveAvgTable upsert result;
        
        -1 ">> SUCCESS. Live Table Updated (Rows: ",string[count result],")";
        show liveAvgTable;
        -1 "------------------------------------------------";
    ];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Split-Trap Mode.";
