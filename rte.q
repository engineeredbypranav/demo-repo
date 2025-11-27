/ rte.q - Real Time Engine with Forward Fill Average Logic

/ 1. Define Schema matches schema.q (Relevant columns only for brevity, add others if needed)
chunkStoreKalmanPfillDRA:([] 
    time:`timespan$(); 
    sym:`symbol$(); 
    Bid:`float$(); 
    Ask:`float$();
    / ... (other columns can be allowed implicitly or defined if strictly needed)
    Alpha_11:`float$(); Alpha_11_coef_ASK:`float$(); Alpha_11_coef_BID:`float$();
    Alpha_5:`float$(); Alpha_5_coef_ASK:`float$(); Alpha_5_coef_BID:`float$();
    Alpha_6:`float$(); Alpha_6_coef_ASK:`float$(); Alpha_6_coef_BID:`float$();
    Alpha_8:`float$(); Alpha_8_coef_ASK:`float$(); Alpha_8_coef_BID:`float$();
    AskRaw:`float$(); AskRawHiddenState:`float$(); AskVarRaw:`float$();
    BidRaw:`float$(); BidRawHiddenState:`float$(); BidVarRaw:`float$();
    CPP_ASK_Edge_MHW:`float$(); CPP_ASK_Edge_MHW_coef_ASK:`float$();
    CPP_BID_Edge_MHW:`float$(); CPP_BID_Edge_MHW_coef_BID:`float$();
    ExpiryTimeMicros:`float$(); GPS_MID:`float$();
    IEAC_BASIS_PX:`float$(); IEAC_BASIS_PX_coef_ASK:`float$(); IEAC_BASIS_PX_coef_BID:`float$()
 );

/ 2. Upd function to insert data from TP
upd:insert;

/ 3. Connect to Tickerplant and Subscribe
/ Check if TP port is passed as arg, else default to 5010
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

/ Open connection
h:@[hopen;tpPort;{0N}];

if[null h; 
    -1 "Failed to connect to TP on port ",string tpPort;
    exit 1
 ];

-1 "Connected to TP on port ",string tpPort;

/ Subscribe to the table and all symbols (`)
h(".u.sub";`chunkStoreKalmanPfillDRA; `);


/ 4. Analytics Function: getBidAskAvg
/ This mimics the logic of getFilledTab -> getAvg
/ It creates a time grid, forward fills missing data, and calculates the average.

/ @param st (timespan) Start Time
/ @param et (timespan) End Time
/ @param bin (timespan) Granularity (e.g. 00:00:01.000)
/ @param s (symbol list) List of symbols to query
getBidAskAvg:{[st;et;bin;s]
    
    / A. Generate Time Grid
    / Calculate number of bins
    cnt: 1+floor (et-st)%bin;
    / Create vector of times
    times: st + bin * til cnt;
    
    / B. Create Grid Table (Cartesian product of Time x Syms)
    / We need a row for every time for every symbol to ensure forward fill works across gaps
    grid: ([] time: raze (count s)#enlist times; sym: raze (count times)#'s);
    
    / C. Select relevant raw data from memory
    raw: select time, sym, Bid, Ask from chunkStoreKalmanPfillDRA 
         where time within (st;et), sym in s;
    
    / D. Join Raw Data to Grid (Union Join)
    / We uj (union join) so we have the grid points + actual data points
    joined: `time xasc grid uj raw;
    
    / E. Forward Fill
    / 'fills' works on vectors, so we group by sym first
    filled: update fills Bid, fills Ask by sym from joined;
    
    / F. Calculate Average
    / Now that gaps are filled, we calculate the avg of Bid and Ask
    / We filter for 'time in times' if you only want the grid points, 
    / or keep all points. Usually, resampling implies keeping just grid points.
    / Here we take the average over the whole filled window per sym.
    res: select avgBid: avg Bid, avgAsk: avg Ask by sym from filled;
    
    :res
 };

-1 "RTE Initialized. Function getBidAskAvg[st;et;bin;s] is ready.";
