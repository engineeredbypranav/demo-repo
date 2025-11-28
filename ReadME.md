The logic can be summarized as:

Subscribe to the Tickerplant (TP):

The RTE connects to the TP on port 5010.

It listens for updates on the chunkStoreKalmanPfillDRA table.

Create a Moving Average Table:

It maintains a local table (rteData) that stores the incoming data.

Every 1 second, it triggers a calculation (runCalc).

This calculation looks at the rteData and filters for the last 60 seconds relative to the latest timestamp.

It calculates the average Bid and Ask for every unique symbol found in that 60-second window.

The result is upserted into liveAvgTable, which holds the current moving average for each symbol.

Manage Memory (Pruning):

To keep things fast, it automatically deletes data older than 5 minutes from its local memory so it doesn't run out of space or slow down over time.
