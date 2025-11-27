/ rte.q - Real Time Engine with Forward Fill Average Logic

/ 1. Define Schema
chunkStoreKalmanPfillDRA:([]
    time:`timespan$();
    sym:`symbol$();
    Bid:`float$();
    Ask:`float$();
    Alpha_11:`float$();
    Alpha_11_coef_ASK:`float$();
    Alpha_11_coef_BID:`float$();
    Alpha_5:`float$();
    Alpha_5_coef_ASK:`float$();
    Alpha_5_coef_BID:`float$();
    Alpha_6:`float$();
    Alpha_6_coef_ASK:`float$();
    Alpha_6_coef_BID:`float$();
    Alpha_8:`float$();
    Alpha_8_coef_ASK:`float$();
    Alpha_8_coef_BID:`float$();
    AskRaw:`float$();
    AskRawHiddenState:`float$();
    AskVarRaw:`float$();
    BidRaw:`float$();
    BidRawHiddenState:`float$();
    BidVarRaw:`float$();
    CPP_ASK_Edge_MHW:`float$();
    CPP_ASK_Edge_MHW_coef_ASK:`float$();
    CPP_BID_Edge_MHW:`float$();
    CPP_BID_Edge_MHW_coef_BID:`float$();
    ExpiryTimeMicros:`float$();
    GPS_MID:`float$();
    IEAC_BASIS_PX:`float$();
    IEAC_BASIS_PX_coef_ASK:`float$();
    IEAC_BASIS_PX_coef_BID:`float$()
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

/ 3. Upd function (Modified for Event-Driven Calculation)
/ This function runs every time the Tickerplant sends a new record
upd:{[t;x]
    / A. Insert the new data into the table
    t insert x;
    
    / B. Trigger Calculation immediately
    if[t=`chunkStoreKalmanPfillDRA;
        
        / Define Window: Calculate Avg for the last 60 seconds relative to NOW
        now: .z.n;                 
        st: now - 00:01:00.000;    
        
        / Get list of symbols currently in the table
        syms: distinct chunkStoreKalmanPfillDRA`sym;
        
        / Run the calculation
        result: getBidAskAvg[st; now; 00:00:01.000; syms];
        
        / Print to Console
        -1 "\n--- Tick Update @ ",string[now]," ---";
        show result;
    ];
 };

/ 4. Connect to Tickerplant and Subscribe
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

h:@[hopen;tpPort;{0N}];
if[null h; -1 "Failed to connect to TP on port ",string tpPort; exit 1];
-1 "Connected to TP on port ",string tpPort;

h(".u.sub";`chunkStoreKalmanPfillDRA; `);

-1 "RTE Initialized. Logic: Recalculate Average on Every Tick.";
