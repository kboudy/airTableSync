/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";

const getSyncInfo = async () => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  const record = query.records[0];
  const syncInfo = {
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

const getRecord = async (apiKey, baseId, tableName, recordId) => {
  let queryUrl = `${API_BASE_URL}${baseId}/${tableName}/${recordId}`;

  const response = await fetch(queryUrl, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await response.json();
  if (json.error) {
    throw new Error(JSON.stringify(json.error));
  }
  return json;
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
    return json;
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
  const sourceFieldNames = sourceTable.fields.map((f) => f.name);
  const sourceRecords = (await sourceTable.selectRecordsAsync()).records;
  const sourceRecordIds = sourceRecords.map((sr) => sr.id);

  /*
       sourceDestinationIdMapping is an object in the format:
  
       { sourceRecordId: destinationRecordId, ... }
      */
  const destSourceIds = await getRecords(
    syncInfo.destinationApiKey,
    syncInfo.destinationBaseId,
    tableToSync,
    ["source_id"]
  );
  const sourceDestinationIdMapping = destSourceIds
    .filter((r) => sourceRecordIds.includes(r.fields.source_id))
    .reduce((acc, r) => {
      acc[r.fields.source_id] = r.id;
      return acc;
    }, {});

  const counts = { create: 0, update: 0, delete: 0 };

  // for each source record, if there's a corresponding destination record, update it.  Otherwise, add it;
  for (const sr of sourceRecords) {
    if (sourceDestinationIdMapping[sr.id]) {
      // it exists - update it;
      counts.update++;
      const destinationRecord = {};
      for (const sfn of sourceFieldNames) {
        destinationRecord[sfn] = await sr.getCellValue(sfn);
      }
      await updateRecords(
        syncInfo.destinationApiKey,
        syncInfo.destinationBaseId,
        tableToSync,
        [{ id: sourceDestinationIdMapping[sr.id], fields: destinationRecord }]
      );
    } else {
      // it doesn't exist - add it;
      counts.create++;
      const destinationRecord = { source_id: sr.id };
      for (const sfn of sourceFieldNames) {
        destinationRecord[sfn] = await sr.getCellValue(sfn);
      }
      await createRecords(
        syncInfo.destinationApiKey,
        syncInfo.destinationBaseId,
        tableToSync,
        [{ fields: destinationRecord }]
      );
    }
  }

  const destinationIdsToDelete = destSourceIds
    .filter((r) => !sourceRecordIds.includes(r.fields.source_id))
    .map((r) => r.id);
  for (const id of destinationIdsToDelete) {
    await deleteRecords(
      syncInfo.destinationApiKey,
      syncInfo.destinationBaseId,
      tableToSync,
      [id]
    );
    counts.delete++;
  }

  output.markdown(`* created: ${counts.create}`);
  output.markdown(`* updated: ${counts.update}`);
  output.markdown(`* deleted: ${counts.delete}`);
}
