/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";
const UNIQUE_FIELD = "Name";

const setLastSyncDate = async (timestamp) => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  await syncTable.updateRecordAsync(query.records[0], {
    LastSyncTimestamp: timestamp,
  });
};

const getSyncInfo = async () => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  const record = query.records[0];
  const syncInfo = {
    lastSyncTimestamp: record.getCellValue("LastSyncTimestamp"),
    destinationApiKey: record.getCellValue("DestinationApiKey"),
    destinationBaseId: record.getCellValue("DestinationBaseId"),
    tablesToSync: record
      .getCellValue("TablesToSync")
      .split(",")
      .filter((r) => r)
      .map((r) => r.trim()),
  };
  return syncInfo;
};

const getRecords = async (apiKey, baseId, tableName, fields, filterFormula) => {
  const allRecords = [];
  let offset;
  do {
    const params = {
      pageSize: 100,
      view: "Grid view",
    };
    if (filterFormula) {
      params.filterByFormula = filterFormula;
    }
    if (offset) {
      params.offset = offset;
    }
    let queryUrl = `${API_BASE_URL}${baseId}/${tableName}?`;
    for (const k of Object.keys(params)) {
      queryUrl = `${queryUrl}${k}=${encodeURIComponent(params[k])}&`;
    }
    if (fields) {
      //NOTE: fields has an unusual param format
      for (const f of fields) {
        queryUrl = `${queryUrl}&${encodeURIComponent(
          "fields[]"
        )}=${encodeURIComponent(f)}&`;
      }
    }
    queryUrl = queryUrl.slice(0, queryUrl.length - 1);

    const response = await fetch(queryUrl, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
    allRecords.push(...json.records);
    offset = json.offset;
  } while (offset);

  return allRecords;
};

const createRecords = async (apiKey, baseId, tableName, records) => {
  // NOTE: airtable REST API for "create" method allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, MAX_RECORDS_PER_REQUEST);
    remainingRecords = remainingRecords.slice(MAX_RECORDS_PER_REQUEST);
    const response = await fetch(`${API_BASE_URL}${baseId}/${tableName}?`, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      method: "POST",
      body: JSON.stringify({ records: currentChunk }),
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
  }
};

const updateRecords = async (apiKey, baseId, tableName, records) => {
  // NOTE: airtable REST API for "update" method allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, MAX_RECORDS_PER_REQUEST);
    remainingRecords = remainingRecords.slice(MAX_RECORDS_PER_REQUEST);
    const response = await fetch(`${API_BASE_URL}${baseId}/${tableName}?`, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      method: "PATCH",
      body: JSON.stringify({ records: currentChunk }),
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
  }
};

const deleteRecords = async (apiKey, baseId, tableName, ids) => {
  // NOTE: airtable REST API for "delete" method allows a max of 10 records per request
  let remainingIds = [...ids];

  while (remainingIds.length > 0) {
    const currentChunkIds = remainingIds.slice(0, MAX_RECORDS_PER_REQUEST);
    remainingIds = remainingIds.slice(MAX_RECORDS_PER_REQUEST);
    let queryUrl = `${API_BASE_URL}${baseId}/${tableName}?`;
    for (const id of currentChunkIds) {
      queryUrl = `${queryUrl}&${encodeURIComponent("records[]")}=${id}&`;
    }
    queryUrl = queryUrl.slice(0, queryUrl.length - 1);
    const response = await fetch(queryUrl, {
      headers: { Authorization: `Bearer ${apiKey}` },
      method: "DELETE",
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
  }
};

const syncInfo = await getSyncInfo();
const jobTimestamp = new Date().getTime();

output.markdown(
  `## Tables to synchronize: ${syncInfo.tablesToSync
    .map((t) => "`" + t + "`")
    .join(",")}`
);
for (const tableToSync of syncInfo.tablesToSync) {
  output.markdown(`#### Synchronizing table "${tableToSync}"`);

  const sourceTable = base.getTable(tableToSync);
  const sourceFieldNames = sourceTable.fields
    .map((f) => f.name)
    .filter((n) => n !== "id");
  const sourceRecords = (await sourceTable.selectRecordsAsync()).records;
  const sourceKeys = sourceRecords.map((r) => r[UNIQUE_FIELD]);

  /*
     sourceDestinationIdMapping is an object in the format:

     { destinationRecordId: sourceRecordId, ... }
    */
  const sourceDestinationIdMapping = (
    await getRecords(
      syncInfo.destinationApiKey,
      syncInfo.destinationBaseId,
      tableToSync,
      ["source_id"]
    )
  ).reduce((acc, r) => {
    acc[r.fields.source_id] = r.id;
    return acc;
  }, {});

  console.log(sourceDestinationIdMapping);

  const destinationKeysAndIds = (
    await getRecords(
      syncInfo.destinationApiKey,
      syncInfo.destinationBaseId,
      tableToSync,
      [UNIQUE_FIELD]
    )
  ).reduce((acc, r) => {
    acc[r.fields[UNIQUE_FIELD]] = r.id;
    return acc;
  }, {});
  const destinationKeys = Object.keys(destinationKeysAndIds);

  const keysToAdd = sourceKeys.filter((k) => !destinationKeys.includes(k));
  const keysToDelete = destinationKeys.filter((k) => !sourceKeys.includes(k));

  const recordsToAdd = [];
  const recordsToUpdate = [];

  for (const r of sourceRecords) {
    const recordKey = r[UNIQUE_FIELD];
    const destinationRecord = { ...r, source_id: r.id };
    delete destinationRecord.id;

    if (keysToAdd.includes(recordKey)) {
      recordsToAdd.push({ fields: destinationRecord });
    } else {
      recordsToUpdate.push({
        id: destinationKeysAndIds[recordKey],
        fields: destinationRecord,
      });
    }
  }
  output.markdown(`  - creating ${recordsToAdd.length} records...`);
  await createRecords(
    syncInfo.destinationApiKey,
    syncInfo.destinationBaseId,
    tableToSync,
    recordsToAdd
  );

  output.markdown(`  - updating ${recordsToUpdate.length} records...`);
  await updateRecords(
    syncInfo.destinationApiKey,
    syncInfo.destinationBaseId,
    tableToSync,
    recordsToUpdate
  );

  output.markdown(`  - deleting ${keysToDelete.length} records...`);
  const idsToDelete = keysToDelete.map((k) => destinationKeysAndIds[k]);
  await deleteRecords(
    syncInfo.destinationApiKey,
    syncInfo.destinationBaseId,
    tableToSync,
    idsToDelete
  );
}
await setLastSyncDate(jobTimestamp);
