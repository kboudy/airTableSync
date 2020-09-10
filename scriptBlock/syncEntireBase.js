/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";
const SOURCE_ID = "SourceId";
const TECH_LEADERSHIP_GROUP_MEMBER = "Tech Leadership Group Member";

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
    syncConfig.destinationApiKey,
    syncConfig.destinationBaseId,
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

const convertToDestinationRecord = async (
  sourceRecord,
  linkedFields,
  attachmentFieldNames,
  selectFieldNames,
  idMappingByTable,
  commonFieldNames
) => {
  const destinationRecord = {};
  for (const sfn of commonFieldNames) {
    destinationRecord[sfn] = await sourceRecord.getCellValue(sfn);
    const isAttachment =
      destinationRecord[sfn] &&
      destinationRecord[sfn].length > 0 &&
      attachmentFieldNames.includes(sfn);
    if (isAttachment) {
      //NOTE: Jake only wants to bring across the first attachment
      destinationRecord[sfn] = [
        {
          url: destinationRecord[sfn][0].url,
          fileName: destinationRecord[sfn][0].fileName,
        },
      ];
    }

    const isSelect = destinationRecord[sfn] && selectFieldNames.includes(sfn);
    if (isSelect) {
      if (Array.isArray(destinationRecord[sfn])) {
        destinationRecord[sfn] = destinationRecord[sfn].map((r) => r.name);
      } else {
        destinationRecord[sfn] = destinationRecord[sfn].name;
      }
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
/* #endregion Methods */

/* #region Main-Execution */
const syncConfig = await getSyncConfig();

output.markdown(
  `## Tables to synchronize: ${syncConfig.tablesToSync
    .map((t) => "`" + t + "`")
    .join(",")}`
);

const idMappingByTable = {};
for (const table of syncConfig.tablesToSync) {
  idMappingByTable[table] = await getIdMapping(table);
}

for (const tableToSync of syncConfig.tablesToSync) {
  output.markdown(`#### Synchronizing table "${tableToSync}"`);

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
  const selectFieldNames = sourceTable.fields
    .filter((f) => f.type === "singleSelect" || f.type === "multipleSelects")
    .map((f) => f.name);
  const sourceFieldNames = sourceTable.fields.map((f) => f.name);
  const destinationFieldNames = syncConfig.destinationSchema[tableToSync];
  const commonFieldNames = sourceFieldNames.filter((sfn) =>
    destinationFieldNames.includes(sfn)
  );
  const sourceRecords = (await sourceTable.selectRecordsAsync()).records.filter(
    (sr) =>
      tableToSync !== "Contacts" ||
      sr.getCellValue(TECH_LEADERSHIP_GROUP_MEMBER)
  );
  const sourceRecordIds = sourceRecords.map((sr) => sr.id);

  const { allSourceIdsInDestination, idMapping } = idMappingByTable[
    tableToSync
  ];

  const counts = { create: 0, update: 0, delete: 0 };

  const recordsToCreate = [];
  const recordsToUpdate = [];
  const recordsToDelete = [];

  // for each source record, if there's a corresponding destination record, update it.  Otherwise, add it;
  for (const sr of sourceRecords) {
    if (idMapping[sr.id]) {
      // it exists - update it;
      counts.update++;
      const destinationRecord = await convertToDestinationRecord(
        sr,
        linkedFields,
        attachmentFieldNames,
        selectFieldNames,
        idMappingByTable,
        commonFieldNames
      );
      recordsToUpdate.push({ id: idMapping[sr.id], fields: destinationRecord });
    } else {
      // it doesn't exist - add it;
      counts.create++;
      const destinationRecord = await convertToDestinationRecord(
        sr,
        linkedFields,
        attachmentFieldNames,
        selectFieldNames,
        idMappingByTable,
        commonFieldNames
      );
      recordsToCreate.push({ id: idMapping[sr.id], fields: destinationRecord });
    }
  }

  const destinationIdsToDelete = allSourceIdsInDestination
    .filter((r) => !sourceRecordIds.includes(r.fields[SOURCE_ID]))
    .map((r) => r.id);
  for (const id of destinationIdsToDelete) {
    recordsToDelete.push(id);
    counts.delete++;
  }

  if (recordsToUpdate.length > 0) {
    await updateRecords(
      syncConfig.destinationApiKey,
      syncConfig.destinationBaseId,
      tableToSync,
      recordsToUpdate
    );
  }

  if (recordsToCreate.length > 0) {
    await createRecords(
      syncConfig.destinationApiKey,
      syncConfig.destinationBaseId,
      tableToSync,
      recordsToCreate
    );
  }

  if (recordsToDelete.length > 0) {
    await deleteRecords(
      syncConfig.destinationApiKey,
      syncConfig.destinationBaseId,
      tableToSync,
      recordsToDelete
    );
  }

  // update the id mapping for this table (so any linked fields in subsequent tables get the new ids)
  idMappingByTable[tableToSync] = await getIdMapping(tableToSync);

  output.markdown(`* created: ${counts.create}`);
  output.markdown(`* updated: ${counts.update}`);
  output.markdown(`* deleted: ${counts.delete}`);
}
/* #endregion Main-Execution */
