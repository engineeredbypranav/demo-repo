/ rte.q - Real Time Engine (Manual Dispatch Mode)

/ 0. Map .u.upd to upd (Just in case)
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000

/ 1. MANUAL DISPATCHER (.z.ps)
/ This is the specific fix for your issue.
/ We intercept the message and call upd() manually.
.z.ps:{[x]
    / Check if the message is a list starting with `upd or `.u.upd
    func: first x;
    
    / Dispatch Logic
    if[func in `upd`.u.upd;
        -1 ">> [SNIFFER] Manually dispatching 'upd' message...";
        / x[1] is table name, x[2] is data
        upd[x 1; x 2];
        :();
    ];

    / Fallback for other messages
    -1 "[SNIFFER] Received non-upd msg: ",(-3!x);
    value x; 
 };
/ -------------------

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

/ 4. Upd Function
upd:{[t;x]
    / Print entry immediately
    -1 ">> upd CALLED (Manual Dispatch). Rows: ",string count x;

    @[{
        / 1. SELECT & CAST
        / Extract only the 4 cols we need from the big table
        d: select time, sym, Bid, Ask from x;
        d: update "n"$time, "s"$sym, "f"$Bid, "f"$Ask from d;

        / 2. INSERT
        `rteData insert d;
        -1 "   [TRACE] Inserted ",string[count d]," rows.";

        / 3. CALC
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

-1 "RTE Ready. Manual Dispatch Mode.";
