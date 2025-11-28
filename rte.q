/ rte.q - Real Time Engine (5-Minute Interval Mode)

/ 0. Map .u.upd
.u.upd:upd;

/ --- CONFIG ---
/ Run Calculation every 300,000ms (5 minutes)
/ This completely stops the "infinite execution" console spam.
.z.ts:{ 
    -1 "[RTE] 5-minute timer fired. Running Calc...";
    runCalc[]; 
 };

/ Set timer to 5 minutes
\t 300000

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

/ 4. Upd Function (Insert Only - High Performance)
/ NO CALCULATIONS HERE. Pure data ingestion.
upd:{[t;x]
    @[{
        typ: type x;
        sourceTable: ();
        
        / CASE 1: Interleaved List (-11 98...)
        isInterleaved: (0=typ) and (98=type x 1);
        
        if[isInterleaved;
            cnt: count x;
            idxSyms: 2*til cnt div 2;
            syms: x idxSyms;
            idxTabs: 1 + 2*til cnt div 2;
            tabs: x idxTabs;
            merged: raze tabs;
            if[not `sym in cols merged;
                merged: update sym:syms from merged
            ];
            sourceTable: merged;
        ];

        / CASE 2: Standard Table
        if[98=typ; sourceTable: x];
        
        / CASE 3: Standard List
        if[(0=typ) and not isInterleaved;
             sourceTable: flip `time`sym`Bid`Ask!(x 0; x 1; x 2; x 3);
        ];

        if[0=count sourceTable; :()];

        / --- PROCESS ---
        d: select time, sym, Bid, Ask from sourceTable;
        d: update "n"$time, "s"$sym, "f"$Bid, "f"$Ask from d;
        
        `rteData insert d;
        
    };(t;x);{[err] -1 "!!! [UPD ERROR] ",err}];
 };

/ 5. Calculation Trigger (Called by Timer every 5 mins)
runCalc:{
    @[{
        now: exec max time from rteData;
        if[null now; :()];
        
        / Window: Last 60 seconds of data
        st: now - 00:01:00.000;    
        syms: distinct rteData`sym;
        
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        if[count result;
            result: update time:now from result;
            `liveAvgTable upsert result;
            
            -1 ">> SUCCESS. Live Table Updated @ ",string[.z.t];
            show liveAvgTable;
            -1 "------------------------------------------------";
        ];
    };(::);{[err] -1 "!!! [CALC ERROR] ",err}];
 };

/ 6. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];
h:@[hopen;tpPort;{0N}];
if[not null h; 
    -1 "Connected to TP ",string tpPort;
    h(".u.sub";`chunkStoreKalmanPfillDRA; `);
];

-1 "RTE Ready. Updates every 5 minutes.";
