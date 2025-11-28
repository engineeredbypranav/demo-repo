/ rte.q - Real Time Engine (Structure Scanner Mode)

/ 0. Map .u.upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000

/ 1. MANUAL DISPATCHER
.z.ps:{[x]
    func: first x;
    if[func in `upd`.u.upd;
        upd[x 1; x 2];
        :();
    ];
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

/ 4. Upd Function (The Fix: Scanner & Mapper)
upd:{[t;x]
    @[{
        / --- A. SCAN STRUCTURE ---
        / If it's a list (row), let's look at what we have.
        if[0=type x;
             types: type each x;
             / -1 "   [SCAN] Types of first 5 items: ",(-3!5#types);
             
             / --- B. DYNAMIC MAPPING ---
             / Find Sym (Type -11)
             idxSym: first where types = -11h;
             
             / Find Time (Types -19, -16, -12)
             idxTime: first where types within -19 -12h; 
             
             / Find Floats (Type -9) for Bid/Ask
             / We assume the first two floats found are Bid and Ask
             floatIndices: where types = -9h;
             idxBid: floatIndices 0;
             idxAsk: floatIndices 1;
             
             / --- C. EXTRACTION ---
             / If we found a Symbol, grab it. Else error.
             if[null idxSym; -1 "!!! [ERROR] No Symbol column found."; :()];
             valSym: x idxSym;
             
             / If we found a Time, grab it. Else use Local Time.
             if[not null idxTime; valTime: x idxTime];
             if[null idxTime; 
                 / -1 "   [WARN] No Time column found. Using Local Time."; 
                 valTime: .z.p
             ];
             
             / If we found Floats, grab them.
             if[any null (idxBid; idxAsk); -1 "!!! [ERROR] Could not find 2 Float columns for Bid/Ask."; :()];
             valBid: x idxBid;
             valAsk: x idxAsk;
             
             / --- D. CAST & INSERT ---
             safeTime: "n"$valTime;
             safeSym:  "s"$valSym;
             safeBid:  "f"$valBid;
             safeAsk:  "f"$valAsk;
             
             toInsert: flip `time`sym`Bid`Ask!(safeTime; safeSym; safeBid; safeAsk);
             `rteData insert toInsert;
             runCalc[];
             :();
        ];
        
        / Fallback for Tables (Type 98)
        if[98=type x;
            d: flip x;
            toInsert: flip `time`sym`Bid`Ask!("n"$d`time; "s"$d`sym; "f"$d`Bid; "f"$d`Ask);
            `rteData insert toInsert;
            runCalc[];
        ];

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

-1 "RTE Ready. Structure Scanner Mode.";
