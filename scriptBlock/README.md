# AirTableSync: ScriptBlock solution

1-way synchronization for an AirTable base, using script blocks, hosted on AirTable

## Setup

- Prequisites:

  - Have a source & destination base (can be on separate accounts)
    - take note of the destination base's id, which can be found [here](https://airtable.com/api)
  - Know your destination base account's API key, which can be found [here](https://airtable.com/account)

1. On the source base, create a `SyncInfo` table with 3 text columns, and populate the values _(table should have one row only)_:

- `DestinationApiKey` - the API key for the account that the destination base lives in _(see above)_
- `DestinationBaseId` - the id of the destination base _(see above)_
- `DestinationSchema` - a JSON string which conveys the tables & columns in the destination that should be sync'd
  - values in columns which are not specified here will be preserved during sync's
  - see example [here](./schemaExample.json)
  - suggestion: make this column type `Long Text` to accommodate multiple lines

2. Add 3 [script blocks](https://support.airtable.com/hc/en-us/articles/360043041074-Scripting-block-overview) to the source base, and paste the corresponding code from this project into them

- [populateSourceIds](./populateSourceIds.js)
- [syncEntireBase](./syncEntireBase.js)
- [syncRecord](./syncRecord.js)

3. On the destination base, create a `SourceId` text column on each table that should be sync'd

## Initial Sync

- Run the `populateSourceIds` script block
  - It will populate the record id's from the source base into the destination's `SourceId` columns
  - It does this by finding 1:1 matches using the `Name` column
  - If there are 0, or more-than-1 matches for a particular source record, it will log the issue
    - These should be taken care of manually before proceeding
  - You should delete this script block after running it - it's just a one-time thing
- Run the `syncEntireBase` script block
  - it will:
    - **update** records in the destination, using the `SourceId` column
    - **add** records to the destination which did not exist
    - **delete** records in the destination which don't exist in the source
      - these would have been logged as have 0 matches in the previous `populateSourceIds` script
  - you can optionally delete this script, depending on whether you foresee needing to do an entire base sync again

## Per-record sync

Once you have completed the **Setup** & **Initial Sync** steps above, you're ready to set up per-record synchronizing:

- On the source base, for each table that will be sync'd, create a [button field](https://support.airtable.com/hc/en-us/articles/360048496693-Button-field) with a `Run script` action that points to the `syncRecord` script block (added above, in the **Setup** step)
- Clicking that button will now synchronize that record _and any linked records_
  - by default, the script will recurse through all linked records, synchronizing everything remotely associated with the chosen record. If this is "too thorough", you can adjust the `SYNCHRONIZE_LINKED_RECORD_DEPTH` variable at the top of the script
    - `0` will sync _only the chosen record_. It will not try to synchronize any linked records.
    - `1` will sync the chosen record, and any 1st-order linked records. It will not try to sync the linked records' linked records _(I realize that "reads" oddly - hopefully that makes sense)_.
    - `10` will recurse through, sync'ing linked-records-of-linked-records to a depth of 10
