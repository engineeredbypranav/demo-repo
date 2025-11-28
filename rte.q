/ rte.q - Real Time Engine (Inspector Mode)

/ 0. Map .u.upd to upd (Just in case)
.u.upd:upd;

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

/ 3. Upd Function (INSPECTOR MODE)
/ This will NOT crash. It will just tell us what the data looks like.
upd:{[t;x]
    lastRawMsg::x;

    -1 ">> upd RECEIVED on table: ",string t;

    / Check if x is a table or a list of columns
    isTable: 98=type x;
    
    / Normalize data to a list of columns 'd'
    d: $ [isTable; value flip x; x];

    / INSPECTION LOGIC
    / Print the Type of each column
    -1 "   [TYPES]   ",(-3!type each d);
    
    / Print the First Item of each column (Sample Data)
    / This lets us see "Ah, column 0 is a Symbol, column 1 is Time"
    safeSample: {[c] $[count c; first c; "empty"]};
    -1 "   [SAMPLE]  ",(-3!safeSample each d);

    / SAFETY INSERT
    / We attempt to slice first 4 cols, but we won't cast blindly.
    / If schema matches, it inserts. If not, it prints error but keeps running.
    @[{
        toInsert: 4#y;
        `chunkStoreKalmanPfillDRA insert toInsert;
        
        / If insert worked, run calc
        runCalc[];
        
    };(t;d);{[err] -1 "   [INSERT FAIL] ",err}];
    
    -1 "------------------------------------------------";
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
            -1 "   >> SUCCESS: liveAvgTable updated. Rows: ",string count result;
        ];
    };(::);{ -1 "   [CALC FAIL] ",x }];
 };

/ 5. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Inspecting Data Structure...";
