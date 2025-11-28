/ rte.q - Real Time Engine with Forward Fill Average Logic

/ 0. CRITICAL FIXES for Function Mapping
/ Map .u.upd to upd because feed.q likely calls .u.upd
.u.upd:upd;
/ Map .u.end to prevent errors at end of day
.u.end:{[x;y] -1 "Received End of Day"; };

/ 1. Define Schema
/ Keeping time as timespan (n) based on previous check.
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

/ DEBUG LOGGING
debugLog:([] time:`timestamp$(); level:`symbol$(); msg:(); payload:());

/ Robust logger that handles any data type in payload
logEvent:{[lvl; m; p]
    @[{
        `debugLog insert (.z.p; x; y; z);
        if[1000<count debugLog; delete from `debugLog where i<10]; 
        -1 string[.z.p]," [",string[x],"] ",y;
    };(lvl;m;p);{ -1 "LOGGING ERROR: ",x }];
 };

logEvent[`INFO; "RTE Started. Listening for 'upd' and '.u.upd'"; ()];

/ 1.5 MESSAGE SNIFFER (.z.ps)
/ This captures ALL async messages sent to this process.
/ It allows us to debug exactly what function the TP is calling.
.z.ps:{[x]
    / Log the raw message structure
    logEvent[`DEBUG; "RAW MESSAGE RECEIVED"; x];
    
    / Execute the message normally
    value x;
 };

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
    
    if[0=count raw; logEvent[`WARN; "Calc: No raw data found in window"; (st;et)]];
        
    joined:aj[`sym`time; grid; `sym`time xasc raw];
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    :res
 };

/ 3. Upd Function
upd:{[t;x]
    logEvent[`INFO; "upd CALLED for table: ",string[t]; ()];

    / A. Slicing Logic
    toInsert: $ [t=`chunkStoreKalmanPfillDRA; 
        / Slicing first 4 columns assuming (time; sym; Bid; Ask)
        $[98=type x; select time, sym, Bid, Ask from x; 4#x];
        x
    ];

    / B. Insert
    @[{
        x insert y;
        logEvent[`INFO; "Insert Success. Rows: ",string count x; ()];
    };(t;toInsert);{[err; data] 
        logEvent[`ERROR; "INSERT FAILED: ",err; data];
    }[;toInsert]];
    
    / C. Calculation
    if[t=`chunkStoreKalmanPfillDRA;
        @[{
            now: exec max time from chunkStoreKalmanPfillDRA;
            if[null now; now:.z.n];
            
            st: now - 00:01:00.000;    
            syms: distinct chunkStoreKalmanPfillDRA`sym;
            
            result: getBidAskAvg[st; now; 00:00:01.000; syms];
            
            if[count result;
                result: update time:now from result;
                `liveAvgTable upsert result;
                logEvent[`INFO; "LiveTable Updated"; result];
            ];
        };(::);{[err] logEvent[`ERROR; "CALC FAILED: ",err; ()]}];
    ];
 };

/ 4. Connection
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

h:@[hopen;tpPort;{0N}];
if[null h; logEvent[`FATAL; "Connect Failed"; tpPort]; -1 "Connect Failed"; exit 1];

logEvent[`INFO; "Connected to TP. Subscribing..."; tpPort];
h(".u.sub";`chunkStoreKalmanPfillDRA; `);
