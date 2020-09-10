/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";
const SOURCE_ID = "SourceId";

/* #region Methods */
const getSyncConfig = async () => {
  const configTable = base.getTable("Config");
  const configQuery = await configTable.selectRecordsAsync();
  const tlConfigRecord = configQuery.records.filter(
    (r) => r.getCellValue("Name") === "TechLeadershipWebsiteConfig"
  );
  if (tlConfigRecord.length !== 1) {
    throw new Error(
      `Expected 1 row in the config table with a name of "TechLeadershipWebsiteConfig", but there was ${tlConfigRecord.length}`
    );
  }
  const tlConfigRecordJson = JSON.parse(
    tlConfigRecord[0].getCellValue("Value")
  );

  const destinationSchema = tlConfigRecordJson.DestinationSchema;

  const syncConfig = {
    destinationApiKey: tlConfigRecordJson.DestinationApiKey,
    destinationBaseId: tlConfigRecordJson.DestinationBaseId,
    destinationSchema: destinationSchema,
    tablesToSync: Object.keys(destinationSchema),
  };
  return syncConfig;
};

const getRecords = async (
  apiKey,
  baseId,
  tableName,
  fields,
  filterFormula,
  maxRecords
) => {
  const allRecords = [];
  let offset;
  do {
    const params = {
      pageSize: 100,
    };
    if (filterFormula) {
      params.filterByFormula = filterFormula;
    }
    if (maxRecords) {
      params.maxRecords = maxRecords;
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

const makeApiRequest = async (
  apiKey,
  baseId,
  tableName,
  records,
  httpMethod
) => {
  // NOTE: airtable REST API for create & update methods allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    const currentChunk = remainingRecords.slice(0, MAX_RECORDS_PER_REQUEST);
    remainingRecords = remainingRecords.slice(MAX_RECORDS_PER_REQUEST);
    const response = await fetch(`${API_BASE_URL}${baseId}/${tableName}?`, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      method: httpMethod,
      body: JSON.stringify({ records: currentChunk, typecast: true }),
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
  }
};

const updateRecords = async (apiKey, baseId, tableName, records) => {
  await makeApiRequest(apiKey, baseId, tableName, records, "PATCH");
};
/* #endregion Methods */

/* #region Main-Execution */
const syncConfig = await getSyncConfig();

for (const tableToSync of syncConfig.tablesToSync) {
  const sourceRecords = (await base.getTable(tableToSync).selectRecordsAsync())
    .records;
  const destinationRecords = await getRecords(
    syncConfig.destinationApiKey,
    syncConfig.destinationBaseId,
    tableToSync
  );
  const recordsToUpdate = [];
  for (const dr of destinationRecords) {
    const matches = sourceRecords.filter((sr) => sr.name === dr.fields.Name);
    if (matches.length === 1) {
      recordsToUpdate.push({ id: dr.id, fields: { SourceId: matches[0].id } });
    } else {
      console.log(
        `${tableToSync}['${dr.fields.Name}'] had ${matches.length} matches`
      );
    }
  }
  await updateRecords(
    syncConfig.destinationApiKey,
    syncConfig.destinationBaseId,
    tableToSync,
    recordsToUpdate
  );
}
/* #endregion Main-Execution */
