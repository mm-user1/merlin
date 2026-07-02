### Changelog
All significant changes and version history are recorded in this file.
#### [1.3.2] - 2026-07-02
- Added Multi-Equity mode for the Analytics page
- Added Fast Numba Grid Search for S_03 v10
- Added the new S_06 R-Trend v02 strategy with Fast Numba Grid mode
#### [1.3.1] - 2026-04-01
- Added Initial Search Coverage - uniform initial coverage of the parameter space
- The Consistency metric was replaced with R2 (the name remained the same) - describing the equity curve with a single number
- Significantly improved performance - the backend was moved from the file system to RAM, loops in strategies now use numpy arrays
- Fixed the issue with duplicate combinations during optimization
- Significantly improved and refined the Analytics page
	- Improved UI
	- Improved database performance - metrics and equity curves for sets are now cached
	- Added filters, sorting, set colors, bulk actions with sets
	- Added the Consist metric for sets
	- Added a mini-chart of return distribution inside the set
- Improved work with the Queue
	- Automatic set creation
	- Queue items can be reordered
	- Completed queue items can be deleted
- Added configurable WFA Adaptive Cooldown mode
- Improved Forward Test mode - it does not allow combinations that did not pass the threshold to trade, configured on the main page
#### [1.3.0] - 2026-02-28
- Added database management - database selection on startup, switching between different databases
- Added WFA Adaptive mode
- Added preview of IS, OOS, FW periods, as well as how many WFA windows there will be in total with the current settings
- Added a research queue - planned research jobs can be started and stopped
- Fixed paths to CSV files - they are now absolute everywhere
- Added the Analytics page - a separate large feature with a huge number of new capabilities
	- Displaying a table of research studies with metrics + multi-select and aggregated metrics
	- Sorting, filters, annualized profit metric, focus mode on one research study
	- Ability to combine research studies into sets, then work with them (rename, delete, move, update)
	- Aggregated equity curve is displayed when several research studies are selected
#### [1.2.5] - 2026-02-07
- Added the new strategy S03 Reversal v10
- Added the ability to download trades for a single backtest from the main page - Trades button
#### [1.2.1] - 2026-02-04
- Fixed - the database now takes 40% less disk space for a WFA research study, Stitched Equity Curve is stored and displayed in simplified form, other Equity Curves are not stored in the database and are generated on request
#### [1.2.0] - 2026-02-01
- Added Post Process module for selecting candidates after optimization has already completed, it includes Deflated Sharpe Ratio (DSR), Forward Test (FT) and Stress Test (ST) - they can be enabled all together and separately
- Added Winrate % (target and constraint) and Consecutive Losses (constraint only) - they are now displayed in the results table
- Added Manual Test function - testing on a separately specified period for the selected parameter set (launched after optimization is complete)
- Added OOS Test function - automated analogue of Manual Test - now it is possible to decide in advance whether testing on a separate period will be performed after optimization, when OOS Test is enabled - WFA is unavailable, and vice versa
- Refactored WFA results - now the same logic as Optuna results (equity charts, tabs, tables with parameter sets, etc.)
#### [1.1.0] - 2026-01-11
- Refactored WFA module - now it works as it should (IS window - optimization, OOS window - trading simulation of the top-1 parameter set, after completion a stitched equity curve across all OOS windows is displayed)
- Added a new Results page - it completely replaces CSV output + additionally displays the equity curve + from it you can do Download Trades for the selected parameter set
- Integrated SQLite database - now all results of Optuna and WFA research studies are stored there
- Made a large update of the Optuna module - added the ability to select several optimization objectives, added NSGA II and III samplers, added optimization constraints
- Added disableable sanitize capability (works only for Sharpe, Sortino, SQN, PF if they are the optimization objective), if PF = inf, this is not sanitized, such a trial will be failed - this is needed for the Optuna sampler to understand where the "bad" results live (by default performed only when trades = 0, that is, so that Optuna does not throw parameters from this problematic area)
- Refactored Composite Score - now it is deterministic, it does not need other trials for evaluation (the previous version remains, can be switched if needed), this is needed for correct multithreading
#### [1.0.9] - 2025-12-20
- Added multithreading in Optuna
- Fixed - some parameters were not displayed (N/A) in WFA CSV (Trail RR, ATR Period, etc.)
- Removed dynamic warmup, now the number of warmup bars is set only directly in UI
- Fixed the first-trade mismatch with TradingView - a helper was added to the WFA module that directly specifies starting trading from 00:00 of the required day
- Fixed - wf_config NameError bug in export_wfa_trades_history
- Fixed - presets directory casing in server.py
- Fixed - silent strategy resolution failure during CSV import
- Fixed - invalid numeric CSV values silently become 0 / 0.0
- Fixed - create param_id() swallows all exceptions
#### [1.0.0] - 2025-12-16
- Completed full migration to the new architecture - core + modules + adding strategies + splitting the monolithic html. This was a very difficult task, almost a month was spent on everything
- In parallel, many fixes were made - overall everything works, the project currently has two strategies, both are optimized, all functions work on them
- Now the project is called Merlin - because he is a cool wizard
#### [0.3.8] - 2025-11-27
- Completed partial migration to the new architecture - one core + different strategies, the task turned out to be more difficult than it seemed, so there will be another large migration stage next. All prompts + audits and migration docs are located in ./info/
- Made several fixes after migration completion - overall the project more or less works, all functions have been preserved
#### [0.2.11] - 2025-11-20
- Added Walk-Forward Analysis Stage 1 MVP
- Updated defaults for the Optimizer section (almost all checkboxes enabled + MA step 25)
- Updated UI
- WFA engine fix - Date Range selected in the left part of UI was not used for window calculation, instead all data was used
- Removed saving csv to static, now only loading via Download after completion
- Removed the Optuna 100 initial random trials limit (now max = 50000)
- Added Warmup period for the whole project - Warmup Period is now added everywhere (it equals the longest MA period * 1.5), this happens in all places where the dataset is trimmed
- Output WFA CSV data fix
	- Fixed OOS and FW profit calculation (initial capital was set to $10000 instead of $100)
	- Added Net Profit% in IS windows
	- In === WINDOW DETAILS === the date is written instead of the bar number
	- At the beginning of CSV, IS-OOS dates and FW Reserve are written
	- Appearance now writes in which exact window the combination was found
- Added export of script trades (only for WFA mode) - they are downloaded in a separate ZIP after completion, they can be loaded into TV using Trading Report Generator
- All output file names are standardized (operating mode is added at the end - Grid, Optuna, Optuna+WFA)
- Added the ability to process several tickers when WFA is enabled
#### [0.1.6] - 2025-11-10
- Fixed incorrect trailing exit price (if it is higher for long and lower for short) - there was a global problem in close price calculation, fixed in files backtest_engine.py and optimizer_engine.py
#### [0.1.5] - 2025-11-06
- Added optimization via Optuna
- Fixed issue with Score 15.00 during Optuna optimization (calculated incorrectly)
- Fixed issue with Stop X = 1.7000000000000002 and for 2.4 as well
- Shortened column names in the output CSV
- In the output CSV renamed Optimization Metadata => Optuna Metadata
#### [0.0.17] - 2025-11-02
- Added Score System from 6 parameters (including Sharpe Ratio (corrected), everything is written to the output CSV) - this was Claude's suggestion
- Added Lock for Trail MA types
#### [0.0.15] - 2025-10-29
- Removed T MA Type and TrailMA Type columns from the output CSV (if they were not iterated), moved to the parameter block - same logic for the whole block and all parameters
- Added presets + import of presets from the output CSV (everything specified in the locked parameters block), if a parameter participated in iteration, it is not imported as a preset. Default settings and added presets can be overwritten (the right side does not participate, its presets are not affected)
#### [0.0.13] - 2025-10-28
- More accurate ETA calculation - now the number of candles in the input CSV and testing dates are taken into account
- Added Net Profit Filter - removing from the output CSV rows with Net Profit% below the specified value
- Fixed long number in Trail RR Long/Short in the output CSV
#### [0.0.10] - 2025-10-27
- Renamed columns in the output CSV:
Max Drawdown% => Max DD%
Total Trades => Trades
- At the beginning of the output CSV, a block of fixed parameters that were NOT iterated is written, they are also excluded from the table (row replaced with block, this is more convenient)
- ETA calculation made based on 6 workers = 16 combinations/sec (ETA changes when the number of workers changes)
- Added selection of several CSV files as input (with ctrl or mouse in folder)
#### [0.0.6] - 2025-10-26
- By default all checkboxes in the right part are disabled
- Fixed Trail MA Length (complained about value 5)
- Added selection of number of workers (default 6)
- CSV output name fixed (example "OKX_COREUSDT.P, 30 2024.01.01-2025.10.01_ALL.csv")
- At the beginning of the output CSV, a row of fixed parameters that were NOT iterated is written, they are also excluded from the table
- Added progress bar in terminal