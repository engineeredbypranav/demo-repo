/ rte.q - Real Time Engine (Auto-Swap Mode)

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

/ 4. Upd Function (The Fix: Auto-Swap)
upd:{[t;x]
    @[{
        typ: type x;
        
        / --- UNPACK LOGIC ---
        if[typ=0h; 
            / Check type of FIRST item to determine order
            / -11h = Symbol, -16h/-12h = Time
            isSymFirst: -11h = type x 0;

            if[isSymFirst;
                / -1 "   [TRACE] Detected (Sym; Time...) order. Swapping.";
                rawSym:  x 0;
                rawTime: x 1;
            ];
            
            if[not isSymFirst;
                / -1 "   [TRACE] Detected (Time; Sym...) order.";
                rawTime: x 0;
                rawSym:  x 1;
            ];

            rawBid:  x 2;
            rawAsk:  x 3;
        ];
        
        if[typ=98h;
            d: flip x;
            rawTime: d`time;
            rawSym:  d`sym;
            rawBid:  d`Bid;
            rawAsk:  d`Ask;
        ];

        / --- CASTING ---
        / Now we are sure which variable holds what, so casting is safe
        safeTime: "n"$rawTime;
        safeSym:  "s"$rawSym;
        safeBid:  "f"$rawBid;
        safeAsk:  "f"$rawAsk;
        
        / --- INSERT ---
        toInsert: flip `time`sym`Bid`Ask!(safeTime; safeSym; safeBid; safeAsk);
        `rteData insert toInsert;
        
        / --- CALC ---
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

-1 "RTE Ready. Auto-Swap Mode.";
