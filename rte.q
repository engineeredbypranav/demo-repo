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
/ Added 'time' column to track when the update occurred
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

/ 3. Upd function (Deep Debugging Enabled)
/ This function runs every time the Tickerplant sends a new record
upd:{[t;x]
    / Capture the raw message to the global variable for manual inspection
    lastRawMsg::x;

    / detailed print to understand the feed structure
    -1 ">> UPD TRIGGERED. Table: ",string[t];
    -1 "   Type of x: ",string[type x]," (0=List, 98=Table)";
    -1 "   Count of x: ",string count x;

    / A. Prepare Data for Insertion
    / Handle potential differences in TP output (List of cols vs Table)
    toInsert: $ [t=`chunkStoreKalmanPfillDRA; 
        / If x is a table (type 98), select cols. If list (type 0), slice first 4.
        $[98=type x; 
            select time, sym, Bid, Ask from x; 
            4#x 
        ];
        x
    ];

    / B. Insert the sliced data into the table
    / We use protected evaluation here too, to catch schema errors
    @[{
        x insert y;
        -1 "   >> Insert Successful. Table Count: ",string count x;
    };(t;toInsert);{[err] -1 "   !! INSERT FAILED: ",err}];
    
    / C. Trigger Calculation immediately
    if[t=`chunkStoreKalmanPfillDRA;
        @[{
            now: exec max time from chunkStoreKalmanPfillDRA;
            if[null now; now:.z.n];
            
            st: now - 00:01:00.000;    
            syms: distinct chunkStoreKalmanPfillDRA`sym;
            
            result: getBidAskAvg[st; now; 00:00:01.000; syms];
            
            / Add the 'time' column to the result using the feed time 'now'
            result: update time:now from result;

            `liveAvgTable upsert result;

            -1 "   >> Calculation Updated. Live Table Rows: ",string count liveAvgTable;
            / Uncomment the line below to spam the console with the table
            / show liveAvgTable;
        };(::);{[err] -1 "   !! CALC FAILED: ",err}];
    ];
    -1 "------------------------------------------------";
 };

/ 4. Connect to Tickerplant and Subscribe
tpPort:5010;
if[not null "J"$first .z.x; tpPort:"J"$first .z.x];

h:@[hopen;tpPort;{0N}];
if[null h; -1 "Failed to connect to TP on port ",string tpPort; exit 1];
-1 "Connected to TP on port ",string tpPort;

h(".u.sub";`chunkStoreKalmanPfillDRA; `);

-1 "RTE Initialized. Schema updated: 'time' column added to liveAvgTable.";
