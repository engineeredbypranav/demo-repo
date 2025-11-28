/ rte.q - Real Time Engine with Forward Fill Average Logic

/ 1. Define Schema
/ NOTE: We assume the first 4 columns coming from the TP are time, sym, Bid, Ask.
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$()
 );

/ Define a keyed table to hold the live state of averages
liveAvgTable:([sym:`symbol$()] 
    time:`timespan$();
    avgBid:`float$(); 
    avgAsk:`float$()
 );

/ DEBUG GLOBAL: Stores the last raw message received from TP
lastRawMsg:();

/ 2. Analytics Function: getBidAskAvg
/ Uses aj (as-of join) for robust forward filling and sampling
getBidAskAvg:{[st;et;granularity;s]
    / 0N!"[Calc] Start. Syms: ",(-3!s);

    / Ensure s is a list for consistent handling
    s:(),s;
    if[0=count s; :([] sym:`symbol$(); avgBid:`float$(); avgAsk:`float$())];
    
    / Calculate grid steps
    cnt:1+"j"$(et-st)%granularity;
    if[cnt<1; :([] sym:`symbol$(); avgBid:`float$(); avgAsk:`float$())];

    times:st+granularity*til cnt;
    
    / Construct Grid Table using CROSS (Cartesian Product) - Most Robust Method
    grid: ([] sym:s) cross ([] time:times);
    
    / Select raw data within range
    raw:select sym, time, Bid, Ask from chunkStoreKalmanPfillDRA 
        where sym in s, time within (st;et);
    
    / DEBUG: Check if we actually found data
    if[0=count raw; 
        0N!"[Calc] WARNING: No raw data found in window. Returning grid with nulls.";
    ];
        
    / Perform As-Of Join
    / 'raw' must be sorted by the join keys (`sym`time)
    joined:aj[`sym`time; grid; `sym`time xasc raw];
    
    / Calculate Average on the resampled (forward-filled) data
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    
    :res
 };

/ 3. Upd function
upd:{[t;x]
    / Capture raw msg
    lastRawMsg::x;

    / A. Prepare Data for Insertion
    / Handle potential differences in TP output (List of cols vs Table)
    toInsert: $ [t=`chunkStoreKalmanPfillDRA; 
        / If x is a table (type 98), select cols. 
        / If list (type 0), we slicing first 4. 
        / CRITICAL: This assumes the feed order is (time; sym; Bid; Ask; ...)
        $[98=type x; 
            select time, sym, Bid, Ask from x; 
            4#x 
        ];
        x
    ];

    / B. Insert
    / Using protected evaluation to print RED error if schema mismatches
    @[{
        x insert y;
    };(t;toInsert);{[err] -1 "!!! INSERT FAILED (Check Types/Order): ",err}];
    
    / C. Trigger Calculation
    if[t=`chunkStoreKalmanPfillDRA;
        @[{
            / 1. Determine Window
            now: exec max time from chunkStoreKalmanPfillDRA;
            if[null now; now:.z.n]; 
            
            / 2. Define Start Time (60s ago)
            st: now - 00:01:00.000;    
            
            / 3. Get Symbols
            syms: distinct chunkStoreKalmanPfillDRA`sym;
            
            / 4. Run Calc
            result: getBidAskAvg[st; now; 00:00:01.000; syms];
            
            / 5. Timestamp and Upsert
            result: update time:now from result;
            `liveAvgTable upsert result;

            / 6. Visual Confirmation
            -1 ">> Updated liveAvgTable with ",string[count result]," rows. (Time: ",string[now],")";
            show liveAvgTable;
            
        };(::);{[err] -1 "!!! CALC FAILED: ",err}];
    ];
 };

/ 4. Connect to Tickerplant and Subscribe
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

h:@[hopen;tpPort;{0N}];
if[null h; -1 "Failed to connect to TP on port ",string tpPort; exit 1];
-1 "Connected to TP on port ",string tpPort;

h(".u.sub";`chunkStoreKalmanPfillDRA; `);

-1 "RTE Initialized. Waiting for ticks...";
