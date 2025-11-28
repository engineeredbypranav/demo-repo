/ rte.q - Real Time Engine (Sniffer & Debug Mode)

/ 0. Map .u.upd to upd
.u.upd:upd;

/ --- DEBUG TOOLS ---

/ 1. Heartbeat
/ Prints every 5 seconds to prove the process is running
.z.ts:{ -1 "[RTE] Alive... Waiting for data."; };
\t 5000

/ 2. Message Sniffer
/ Captures EVERYTHING sent to this process.
/ If this prints nothing, the TP is NOT sending data.
.z.ps:{[x]
    -1 "[SNIFFER] Received msg: ",(-3!x);
    value x; / Execute it normally
 };

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

/ 3. Upd Function
upd:{[t;x]
    -1 ">> upd CALLED on table: ",string t;

    @[{
        / Slice first 4 columns (Time, Sym, Bid, Ask)
        toInsert: 4#y;
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

-1 "RTE Ready. Sniffer Active.";
