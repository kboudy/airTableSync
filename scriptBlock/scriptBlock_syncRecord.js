/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";
const SOURCE_ID = "SourceId";

const getSyncInfo = async () => {
  const syncTable = base.getTable("SyncInfo");
  const query = await syncTable.selectRecordsAsync();
  const record = query.records[0];
  const destinationSchema = JSON.parse(
    record.getCellValue("DestinationSchema")
  );
  const syncInfo = {
    destinationApiKey: record.getCellValue("DestinationApiKey"),
    destinationBaseId: record.getCellValue("DestinationBaseId"),
    destinationSchema: destinationSchema,
    tablesToSync: Object.keys(destinationSchema),
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
      view: "Grid view",
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
      body: JSON.stringify({ records: currentChunk }),
    });
    const json = await response.json();
    if (json.error) {
      throw new Error(JSON.stringify(json.error));
    }
  }
};

const createRecords = async (apiKey, baseId, tableName, records) => {
  await makeApiRequest(apiKey, baseId, tableName, records, "POST");
};

const updateRecords = async (apiKey, baseId, tableName, records) => {
  await makeApiRequest(apiKey, baseId, tableName, records, "PATCH");
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

const getIdMapping = async (tableName) => {
  const sourceRecordIds = (
    await base.getTable(tableName).selectRecordsAsync()
  ).records.map((sr) => sr.id);
  const allSourceIdsInDestination = await getRecords(
    syncInfo.destinationApiKey,
    syncInfo.destinationBaseId,
    tableName,
    [SOURCE_ID]
  );
  const idMapping = allSourceIdsInDestination
    .filter((r) => sourceRecordIds.includes(r.fields[SOURCE_ID]))
    .reduce((acc, r) => {
      acc[r.fields[SOURCE_ID]] = r.id;
      return acc;
    }, {});
  return { allSourceIdsInDestination, idMapping };
};

const getDestinationRecord = async (
  sourceRecord,
  table,
  syncInfo,
  linkedFields,
  attachmentFieldNames,
  idMappingByTable,
  commonFieldNames
) => {
  const destinationRecord = {};
  for (const sfn of commonFieldNames) {
    destinationRecord[sfn] = await sourceRecord.getCellValue(sfn);
    const isAttachment =
      destinationRecord[sfn].length > 0 && attachmentFieldNames.includes(sfn);
    if (isAttachment) {
      //NOTE: Jake only wants to bring across the first attachment
      destinationRecord[sfn] = [
        {
          url: destinationRecord[sfn][0].url,
          fileName: destinationRecord[sfn][0].fileName,
        },
      ];
    }

    const link = linkedFields[sfn];

    if (link) {
      // it's a linked field
      const linkIdMapping = idMappingByTable[link.table].idMapping;
      if (destinationRecord[sfn]) {
        destinationRecord[sfn] = destinationRecord[sfn]
          .filter((r) => linkIdMapping[r.id])
          .map((r) => linkIdMapping[r.id]);
      }
    }
  }
  destinationRecord[SOURCE_ID] = sourceRecord.id;
  return destinationRecord;
};

const recurseLinkedFields = async (table, record, linkedFieldTrail = {}) => {
  const linkedFields = table.fields
    .filter((f) => f.type === "multipleRecordLinks")
    .reduce((acc, f) => {
      const linkedTable = base.getTable(f.options.linkedTableId);
      const linkedField = linkedTable.fields.filter(
        (ltf) => ltf.id === f.options.inverseLinkFieldId
      )[0];

      acc[f.name] = {
        table: linkedTable.name,
        field: linkedField.name,
      };
      return acc;
    }, {});
  for (const linkedTableName in linkedFields) {
    if (!linkedFieldTrail[linkedTableName]) {
      linkedFieldTrail[linkedTableName] = [];
    }
    for (const linkedFieldRecord of record.getCellValue(linkedTableName)) {
      if (
        linkedFieldTrail[linkedTableName].filter(
          (rId) => rId === linkedFieldRecord.id
        ).length === 0
      ) {
        linkedFieldTrail[linkedTableName].push(linkedFieldRecord.id);
        const linkedTable = base.getTable(linkedTableName);
        const linkedRecord = (await linkedTable.selectRecordsAsync()).getRecord(
          linkedFieldRecord.id
        );
        linkedFieldTrail = await recurseLinkedFields(
          linkedTable,
          linkedRecord,
          linkedFieldTrail
        );
      }
    }
  }
  return linkedFieldTrail;
};

//-------------------------------------------------------------------------------------------

let activeTable = base.getTable(cursor.activeTableId);
// Note - this prompt is automatically populated when script is triggered by a button field
let activeRecord = await input.recordAsync("Record to sync:", activeTable);

const syncInfo = await getSyncInfo();
const jobTimestamp = new Date().getTime();

// make sure we start with the active table
let tablesToSync = syncInfo.tablesToSync.filter((t) => t !== activeTable.name);
tablesToSync = [activeTable.name, ...tablesToSync];

const idMappingByTable = {};
for (const table of tablesToSync) {
  idMappingByTable[table] = await getIdMapping(table);
}

// to prepare, we'll gather all the records that need to be synced, by following a linked-field trail
const linkedFieldTrail = await recurseLinkedFields(activeTable, activeRecord);

// we'll need to circle back to the original table to catch any newly-created linked fields
tablesToSync.push(activeTable.name);
for (const tableToSync of tablesToSync) {
  if (!linkedFieldTrail[tableToSync]) {
    continue;
  }
  const sourceTable = base.getTable(tableToSync);
  const linkedFields = sourceTable.fields
    .filter((f) => f.type === "multipleRecordLinks")
    .reduce((acc, f) => {
      const linkedTable = base.getTable(f.options.linkedTableId);
      const linkedField = linkedTable.fields.filter(
        (ltf) => ltf.id === f.options.inverseLinkFieldId
      )[0];

      acc[f.name] = {
        table: linkedTable.name,
        field: linkedField.name,
      };
      return acc;
    }, {});
  const attachmentFieldNames = sourceTable.fields
    .filter((f) => f.type === "multipleAttachments")
    .map((f) => f.name);
  const sourceFieldNames = sourceTable.fields.map((f) => f.name);
  const destinationFieldNames = syncInfo.destinationSchema[tableToSync].map(
    (f) => f.name
  );
  const commonFieldNames = sourceFieldNames.filter((sfn) =>
    destinationFieldNames.includes(sfn)
  );
  const { idMapping } = idMappingByTable[tableToSync];
  const counts = { create: 0, update: 0, delete: 0 };
  for (const sourceRecordId of linkedFieldTrail[tableToSync]) {
    const sr = (await sourceTable.selectRecordsAsync()).getRecord(
      sourceRecordId
    );
    if (idMapping[sr.id]) {
      // it exists - update it;
      counts.update++;
      const destinationRecord = await getDestinationRecord(
        sr,
        tableToSync,
        syncInfo,
        linkedFields,
        attachmentFieldNames,
        idMappingByTable,
        commonFieldNames
      );
      await updateRecords(
        syncInfo.destinationApiKey,
        syncInfo.destinationBaseId,
        tableToSync,
        [{ id: idMapping[sr.id], fields: destinationRecord }]
      );
    } else {
      // it doesn't exist - add it;
      counts.create++;
      const destinationRecord = await getDestinationRecord(
        sr,
        tableToSync,
        syncInfo,
        linkedFields,
        attachmentFieldNames,
        idMappingByTable,
        commonFieldNames
      );
      await createRecords(
        syncInfo.destinationApiKey,
        syncInfo.destinationBaseId,
        tableToSync,
        [{ fields: destinationRecord }]
      );
    }
  }
  // update the id mapping for this table (so any linked fields in subsequent tables get the new ids)
  idMappingByTable[tableToSync] = await getIdMapping(tableToSync);
}
