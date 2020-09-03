// this will live on airtable

const API_BASE_URL = "https://api.airtable.com/v0/";

const setLastSyncDate = async () => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  await syncTable.updateRecordAsync(query.records[0], {
    LastSyncDate: new Date(),
  });
};

const getSyncInfo = async () => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  const syncInfo = {
    lastSyncDate: query.records[0].getCellValue("LastSyncDate"),
    apiKey: query.records[0].getCellValue("ApiKey"),
    destinationBaseId: query.records[0].getCellValue("DestinationBaseId"),
  };
  return syncInfo;
};

const syncInfo = await getSyncInfo();

const getRecords = async (tableName, fields, filterFormula) => {
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
    let queryUrl = `${API_BASE_URL}${syncInfo.destinationBaseId}/${tableName}?`;
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
    allRecords.push(...json.records);
    offset = json.offset;
  } while (offset);

  return allRecords;
};

const createRecords = async (tableName, records) => {
  // NOTE: airtable REST API for "create" method allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, 10);
    remainingRecords = remainingRecords.slice(10);
    const response = await fetch(
      `${API_BASE_URL}${syncInfo.destinationBaseId}/${tableName}?`,
      {
        headers: {
          Authorization: `Bearer ${syncInfo.apiKey}`,
          "Content-Type": "application/json",
        },
        method: "POST",
        body: JSON.stringify({ records: currentChunk }),
      }
    );
  }
};

//const results = await getRecords("Fruits",["Name","Color"]);

const records = [];
for (let i = 1; i <= 25; i++) {
  records.push({
    fields: {
      Name: `n${i}`,
      Color: `c${i}`,
    },
  });
}
await createRecords("Fruits", records);
