/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

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
    apiKey: record.getCellValue("ApiKey"),
    destinationBaseId: record.getCellValue("DestinationBaseId"),
    tablesToSync: record
      .getCellValue("TablesToSync")
      .split(",")
      .filter((r) => r)
      .map((r) => r.trim()),
  };
  return syncInfo;
};

const getRecords = async (baseId, tableName, fields, filterFormula) => {
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
      headers: { Authorization: `Bearer ${syncInfo.apiKey}` },
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

const createRecords = async (baseId, tableName, records) => {
  // NOTE: airtable REST API for "create" method allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, 10);
    remainingRecords = remainingRecords.slice(10);
    const response = await fetch(`${API_BASE_URL}${baseId}/${tableName}?`, {
      headers: {
        Authorization: `Bearer ${syncInfo.apiKey}`,
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

const updateRecords = async (baseId, tableName, records) => {
  // NOTE: airtable REST API for "update" method allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, 10);
    remainingRecords = remainingRecords.slice(10);
    const response = await fetch(`${API_BASE_URL}${baseId}/${tableName}?`, {
      headers: {
        Authorization: `Bearer ${syncInfo.apiKey}`,
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

const deleteRecords = async (baseId, tableName, ids) => {
  // NOTE: airtable REST API for "delete" method allows a max of 10 records per request
  let remainingIds = [...ids];

  while (remainingIds.length > 0) {
    const currentChunkIds = remainingIds.slice(0, 10);
    remainingIds = remainingIds.slice(10);
    let queryUrl = `${API_BASE_URL}${baseId}/${tableName}?`;
    for (const id of currentChunkIds) {
      queryUrl = `${queryUrl}&${encodeURIComponent("records[]")}=${id}&`;
    }
    queryUrl = queryUrl.slice(0, queryUrl.length - 1);
    const response = await fetch(queryUrl, {
      headers: { Authorization: `Bearer ${syncInfo.apiKey}` },
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
  const sourceKeys = (
    await getRecords(base.id, tableToSync, [UNIQUE_FIELD])
  ).map((r) => r.fields[UNIQUE_FIELD]);
  const destinationKeysAndIds = (
    await getRecords(syncInfo.destinationBaseId, tableToSync, [UNIQUE_FIELD])
  ).reduce((acc, r) => {
    acc[r.fields[UNIQUE_FIELD]] = r.id;
    return acc;
  }, {});
  const destinationKeys = Object.keys(destinationKeysAndIds);

  const keysToAdd = sourceKeys.filter((k) => !destinationKeys.includes(k));
  const keysToDelete = destinationKeys.filter((k) => !sourceKeys.includes(k));

  const filterFormula = syncInfo.lastSyncTimestamp
    ? "DATETIME_DIFF(LAST_MODIFIED_TIME(), " +
      `DATETIME_PARSE(${syncInfo.lastSyncTimestamp}, 'x'),` +
      "'seconds') > 0"
    : null;
  const changedSourceRecords = await getRecords(
    base.id,
    tableToSync,
    null,
    filterFormula
  );
  const recordsToAdd = [];
  const recordsToUpdate = [];

  for (const r of changedSourceRecords) {
    const recordKey = r.fields[UNIQUE_FIELD];
    if (keysToAdd.includes(recordKey)) {
      recordsToAdd.push({ fields: r.fields });
    } else {
      recordsToUpdate.push({
        id: destinationKeysAndIds[recordKey],
        fields: r.fields,
      });
    }
  }
  output.markdown(`  - creating ${recordsToAdd.length} records...`);
  await createRecords(syncInfo.destinationBaseId, tableToSync, recordsToAdd);

  output.markdown(`  - updating ${recordsToUpdate.length} records...`);
  await updateRecords(syncInfo.destinationBaseId, tableToSync, recordsToUpdate);

  output.markdown(`  - deleting ${keysToDelete.length} records...`);
  const idsToDelete = keysToDelete.map((k) => destinationKeysAndIds[k]);
  await deleteRecords(syncInfo.destinationBaseId, tableToSync, idsToDelete);
}
await setLastSyncDate(jobTimestamp);
