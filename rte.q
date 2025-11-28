/ rte.q - Real Time Engine (Positional Force Cast)

/ 0. Map .u.upd to upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive... Waiting for data."; };
\t 10000
/ -------------------

/ 1. Clean Slate & Define Schema
/ Ensure we delete any old definitions that might have mismatched types
delete chunkStoreKalmanPfillDRA from `.;

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

/ 3. Upd Function (The Heavy Hammer Fix)
upd:{[t;x]
    @[{
        / A. DECONSTRUCT: Get raw list of columns (Values)
        / This ignores column names entirely, preventing name mismatch errors.
        / If x is a table, value flip x gives (col0; col1; col2...)
        / If x is a list, it is already (col0; col1; col2...)
        v: $ [98=type x; value flip x; x];

        / B. FORCE CAST BY POSITION
        / We assume: Col 0 = Time, Col 1 = Sym, Col 2 = Bid, Col 3 = Ask
        / We cast them explicitly to match our local schema types.
        
        cTime: "n"$ v 0;  / Cast to Timespan (fixes Timestamp issues)
        cSym:  "s"$ v 1;  / Cast to Symbol   (fixes String issues)
        cBid:  "f"$ v 2;  / Cast to Float    (fixes Int/Long issues)
        cAsk:  "f"$ v 3;  / Cast to Float
        
        / C. REBUILD
        / Create a clean table with guaranteed names and types
        toInsert: flip `time`sym`Bid`Ask!(cTime; cSym; cBid; cAsk);

        / D. INSERT
        `chunkStoreKalmanPfillDRA insert toInsert;
        
        / E. CALC
        runCalc[];

    };(t;x);{[err] 
        -1 "   [INSERT FAIL] ",err; 
        -1 "   >> TIP: Check if your feed is sending Time/Sym/Bid/Ask in that order.";
    }];
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

-1 "RTE Ready. Positional Casting Active.";
