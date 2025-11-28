/ rte.q - Real Time Engine (Paranoid Debug Mode)

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

/ 2. Analytics Function
getBidAskAvg:{[st;et;granularity;s]
    s:(),s;
    if[0=count s; -1 "   [TRACE] No symbols."; :()];
    
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; -1 "   [TRACE] Bad Window."; :()];

    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    
    raw:select sym, time, Bid, Ask from rteData 
        where sym in s, time within (st;et);

    if[0=count raw; 
        -1 "   [TRACE] No raw data in window."; 
        :()
    ];

    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function (The Fix)
upd:{[t;x]
    / WRAP EVERYTHING in a trap to catch the "Silent Death"
    @[{
        -1 ">> upd CALLED. Rows: ",string count y;
        
        / 1. DEBUG COLUMNS
        / Print what columns we actually have to spot mismatches (e.g. bid vs Bid)
        c: cols y;
        -1 "   [COLS FOUND] ",(" " sv string c);
        
        / 2. CHECK REQUIRED COLS
        req: `time`sym`Bid`Ask;
        missing: req where not req in c;
        if[count missing;
            -1 "!!! [CRASH AVERTED] Missing columns: ",(-3!missing);
            :(); / Abort safely
        ];

        / 3. SELECT & CAST
        / We know cols exist now, so this shouldn't crash
        d: select time, sym, Bid, Ask from y;
        d: update "n"$time, "s"$sym, "f"$Bid, "f"$Ask from d;

        / 4. INSERT
        `rteData insert d;
        -1 "   [TRACE] Inserted ",string[count d]," rows.";

        / 5. CALC
        runCalc[];

    };(t;x);{[err] 
        -1 "!!! [UPD CRASHED] ",err;
    }];
 };

/ 4. Calculation Trigger
runCalc:{
    @[{
        now: exec max time from rteData;
        if[null now; :()];
        st: now - 00:01:00.000;    
        syms: distinct rteData`sym;
        
        -1 "   [TRACE] Calc running for ",string[count syms]," syms.";
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            -1 ">> SUCCESS. Live Table Updated.";
            show liveAvgTable;
            -1 "------------------------------------------------";
        ];
    };(::);{[err] -1 "!!! [CALC CRASH] ",err}];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Paranoid Mode.";
