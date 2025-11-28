/ rte.q - Real Time Engine with Auto-Discovery

/ 0. Map .u.upd
.u.upd:upd;

/ 1. Define Schemas
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$()
 );

debugRaw:([] c0:(); c1:(); c2:(); c3:());

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

/ 3. Upd Function (Safe Mode + Auto-Discovery)
upd:{[t;x]
    lastRawMsg::x;

    @[{
        / Check if x is a table or list
        d: $[98=type y; value flip y; y];
        
        / Get types of all incoming columns
        types: type each d;
        
        / AUTO-DISCOVERY LOGIC
        / Find column indices based on expected types
        / Time: Expect type 16 (timespan) or 12 (timestamp) or 19 (time)
        timeIdx: first where types within (12h; 19h); 
        if[null timeIdx; timeIdx: first where types = 16h]; / Check timespan specifically

        / Sym: Expect type 11 (symbol)
        symIdx: first where types = 11h;

        / Floats: Expect type 9 (float). We need two of them (Bid, Ask).
        floatIdxs: where types = 9h;
        bidIdx: floatIdxs 0;
        askIdx: floatIdxs 1;

        / If we can't find the columns, abort safely
        if[any null (timeIdx; symIdx; bidIdx; askIdx);
            -1 "!!! SCHEMA MISMATCH DETECTED";
            -1 "   Incoming Types: ",(-3!types);
            -1 "   We need: Time(12/16/19), Sym(11), Float(9), Float(9)";
            :(); / Return early
        ];

        / Extract correctly mapped columns
        colTime: d timeIdx;
        colSym:  d symIdx;
        colBid:  d bidIdx;
        colAsk:  d askIdx;

        / Cast Time if necessary (Timestamp -> Timespan)
        / If incoming is timestamp (12) or time (19), cast to timespan (16)
        if[not 16h = type first colTime; colTime: "n"$colTime];

        / Construct Clean Table
        toInsert: flip `time`sym`Bid`Ask!(colTime; colSym; colBid; colAsk);

        / Insert
        `chunkStoreKalmanPfillDRA insert toInsert;

        / Run Calc
        runCalc[];

    };(t;x);{[err] 
        -1 "!!! UPD CRASHED: ",err; 
        -1 "   >> Please type 'lastRawMsg' to inspect data.";
    }];
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

-1 "RTE Ready (Auto-Discovery Mode).";
