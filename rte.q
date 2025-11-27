/ rte.q - Real Time Engine with Forward Fill Average Logic

/ 1. Define Schema (SLIMMED DOWN)
/ Only keeping time, sym, Bid, Ask.
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$()
 );

/ Define a keyed table to hold the live state of averages
liveAvgTable:([sym:`symbol$()] 
    avgBid:`float$(); 
    avgAsk:`float$()
 );

/ 2. Analytics Function: getBidAskAvg
/ Uses aj (as-of join) for robust forward filling and sampling
getBidAskAvg:{[st;et;granularity;s]
    / Ensure s is a list for consistent handling
    s:(),s;
    
    / Calculate grid steps
    cnt:1+"j"$(et-st)%granularity;
    times:st+granularity*til cnt;
    
    / Construct Grid Table (Cartesian Product of Syms x Times)
    / We repeat the times vector for each symbol
    gTime:raze (count s)#enlist times;
    / We repeat each symbol for the length of the time vector
    gSym:raze (count times)#'s;
    
    grid:([] sym:gSym; time:gTime);
    
    / Select raw data within range
    raw:select sym, time, Bid, Ask from chunkStoreKalmanPfillDRA 
        where sym in s, time within (st;et);
    
    / Sort raw data by sym and time (Required for aj)
    raw:`sym`time xasc raw;
    
    / Perform As-Of Join
    / This effectively "forward fills" the data onto the exact grid points
    joined:aj[`sym`time; grid; raw];
    
    / Calculate Average on the resampled (forward-filled) data
    res:select avgBid:avg Bid, avgAsk:avg Ask by sym from joined;
    
    :res
 };

/ 3. Upd function (Modified for Schema Mismatch)
/ This function runs every time the Tickerplant sends a new record
upd:{[t;x]
    / A. Prepare Data for Insertion
    / If the table is chunkStoreKalmanPfillDRA, we only want the first 4 columns 
    / (time, sym, Bid, Ask) from the incoming feed 'x'
    toInsert: $[t=`chunkStoreKalmanPfillDRA; 4#x; x];

    / B. Insert the sliced data into the table
    t insert toInsert;
    
    / C. Trigger Calculation immediately
    if[t=`chunkStoreKalmanPfillDRA;
        
        / Define Window: Calculate Avg for the last 60 seconds relative to NOW
        now: .z.n;                 
        st: now - 00:01:00.000;    
        
        / Get list of symbols currently in the table
        syms: distinct chunkStoreKalmanPfillDRA`sym;
        
        / Run the calculation
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        / Update the persistent live table
        liveAvgTable upsert result;

        / Print to Console
        -1 "\n--- Tick Update @ ",string[now]," ---";
        show liveAvgTable;
    ];
 };

/ 4. Connect to Tickerplant and Subscribe
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

h:@[hopen;tpPort;{0N}];
if[null h; -1 "Failed to connect to TP on port ",string tpPort; exit 1];
-1 "Connected to TP on port ",string tpPort;

h(".u.sub";`chunkStoreKalmanPfillDRA; `);

-1 "RTE Initialized. Schema reduced to 4 cols (time, sym, Bid, Ask).";
