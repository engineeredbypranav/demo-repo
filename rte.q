/ rte.q - Real Time Engine (Force Cast Fix)

/ 0. Map .u.upd to upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive... Waiting for data."; };
\t 5000
/ -------------------

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

/ 2. Analytics Function
getBidAskAvg:{[st;et;granularity;s]
    s:(),s;
    if[0=count s; :()];
    
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; :()];

    times:st+granularity*til cnt;
    grid: ([] sym:s) cross ([] time:times);
    
    raw:select sym, time, Bid, Ask from chunkStoreKalmanPfillDRA 
        where sym in s, time within (st;et);

    if[0=count raw; :()];

    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function (Force Cast Fix)
upd:{[t;x]
    @[{
        / Handle Table (Type 98) vs List
        toInsert: $[98=type x;
            / FORCE CAST ALL COLUMNS to match schema exactly
            / This solves the 'type' error if Bid/Ask are ints or Sym is string
            select time:"n"$time, sym:"s"$sym, Bid:"f"$Bid, Ask:"f"$Ask from x;
            
            / Else (List Input fallback)
            flip `time`sym`Bid`Ask!("n";"s";"f";"f")$\:(4#x)
        ];

        `chunkStoreKalmanPfillDRA insert toInsert;
        
        runCalc[];

    };(t;x);{[err] -1 "   [INSERT FAIL] ",err}];
 };

/ 4. Calculation Trigger
runCalc:{
    @[{
        now: exec max time from chunkStoreKalmanPfillDRA;
        if[null now; :()];
        
        st: now - 00:01:00.000;    
        syms: distinct chunkStoreKalmanPfillDRA`sym;
        
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            
            -1 ">> SUCCESS. Live Table Updated (Rows: ",string[count result],")";
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

-1 "RTE Ready. Force-Cast Mode Applied.";
