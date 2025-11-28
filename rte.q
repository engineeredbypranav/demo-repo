/ rte.q - Real Time Engine (Deconstruction Mode)

/ 0. Map .u.upd to upd
.u.upd:upd;

/ --- DEBUG TOOLS ---
.z.ts:{ -1 "[RTE] Alive..."; };
\t 10000

/ 1. MANUAL DISPATCHER
.z.ps:{[x]
    func: first x;
    if[func in `upd`.u.upd;
        -1 ">> [SNIFFER] Dispatching 'upd'...";
        upd[x 1; x 2];
        :();
    ];
    -1 "[SNIFFER] Non-upd msg: ",(-3!x);
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

/ 4. Upd Function (The Fix: Deconstruction)
upd:{[t;x]
    -1 ">> upd CALLED. Input Type: ",string type x;

    @[{
        / A. DECONSTRUCT
        / Convert Table (98) or Flip (99) into a pure Dictionary
        / If x is a table, 'flip x' gives a dictionary of cols.
        / If x is already a dictionary, we just use x.
        d: $ [98=type x; flip x; x];

        / B. EXTRACT & CAST (Low-Level)
        / We pull columns directly by name. No Q-SQL.
        / This avoids 'type' errors associated with 'update' syntax.
        cTime: "n"$ d`time;
        cSym:  "s"$ d`sym;
        cBid:  "f"$ d`Bid;
        cAsk:  "f"$ d`Ask;

        / C. REBUILD
        / Construct a guaranteed clean table
        toInsert: flip `time`sym`Bid`Ask!(cTime; cSym; cBid; cAsk);

        / D. INSERT
        `rteData insert toInsert;
        -1 "   [TRACE] Inserted ",string[count toInsert]," rows.";

        / E. CALC
        runCalc[];

    };(t;x);{[err] 
        -1 "!!! [UPD CRASHED] ",err;
        -1 "   >> Input x structure keys: ",(-3!keys flip x);
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

-1 "RTE Ready. Deconstruction Mode.";
