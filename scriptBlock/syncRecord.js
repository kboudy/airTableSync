/*************************************************************

NOTE: This is designed to live in a script block, on airTable

*************************************************************/

// how many consecutive linked field "hops" to take (0 = *just* the clicked record)
const SYNCHRONIZE_LINKED_RECORD_DEPTH = 10;
const VERBOSE_LOGGING = true;

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";
const SOURCE_ID = "SourceId";
const ALLOW_SYNC_TO_TLG_WEBSITE = "Allow Sync to TLG Website";

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

// for logging - get a friendly name for the company or contact
const getObjName = (obj) => {
  if (obj["First Name"]) {
    const lastName = obj["Last Name"] ? obj["Last Name"].trim() : "";
    return `${obj["First Name"].trim()} ${lastName}`;
  }
  return obj.Name;
};

const recurseLinkedFields = async (
  syncConfig,
  table,
  record,
  linkedFieldTrail = [],
  currentDepth = 1
) => {
  const allLinkedFields = table.fields
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
  const relevantLinkedFields = {};
  for (const key in allLinkedFields) {
    const o = allLinkedFields[key];
    if (
      syncConfig.tablesToSync.includes(o.table) &&
      syncConfig.destinationSchema[o.table].includes(o.field)
    ) {
      relevantLinkedFields[key] = o;
    }
  }
  for (const fieldName in relevantLinkedFields) {
    const linkedFieldRecords = await record.getCellValue(fieldName);
    if (linkedFieldRecords) {
      for (const linkedFieldRecord of linkedFieldRecords) {
        if (
          linkedFieldTrail.filter(
            (lf) => lf.linkedRecordId === linkedFieldRecord.id
          ).length === 0
        ) {
          const linkedTable = base.getTable(
            relevantLinkedFields[fieldName].table
          );

          const linkedRecord = (
            await linkedTable.selectRecordsAsync()
          ).getRecord(linkedFieldRecord.id);

          const isContactWithoutTechLeadership =
            linkedTable.name === "Contacts" &&
            !linkedRecord.getCellValue(ALLOW_SYNC_TO_TLG_WEBSITE);

          if (!isContactWithoutTechLeadership) {
            linkedFieldTrail.push({
              originTable: table.name,
              originRecordName: record.name,
              originFieldName: fieldName,
              linkedTable: linkedTable.name,
              linkedRecordName: linkedFieldRecord.name,
              linkedRecordId: linkedFieldRecord.id,
            });
            const nextDepth = currentDepth + 1;
            if (nextDepth <= SYNCHRONIZE_LINKED_RECORD_DEPTH) {
              linkedFieldTrail = await recurseLinkedFields(
                syncConfig,
                linkedTable,
                linkedRecord,
                linkedFieldTrail,
                nextDepth
              );
            }
          }
        }
      }
    }
  }
  return linkedFieldTrail;
};

const log = (msg) => {
  if (VERBOSE_LOGGING) {
    output.markdown(msg);
  }
};
/* #endregion Methods */

/* #region Main-Execution */

let activeTable = base.getTable(cursor.activeTableId);
// Note - this prompt is automatically populated when script is triggered by a button field
let activeRecord = await input.recordAsync("Record to sync:", activeTable);

if (
  activeTable.name === "Contacts" &&
  !activeRecord.getCellValue(ALLOW_SYNC_TO_TLG_WEBSITE)
) {
  throw new Error(
    `Can only sync contacts that have "${ALLOW_SYNC_TO_TLG_WEBSITE}" checked`
  );
}

const syncConfig = await getSyncConfig();

// make sure we start with the active table
let tablesToSync = syncConfig.tablesToSync.filter(
  (t) => t !== activeTable.name
);
tablesToSync = [activeTable.name, ...tablesToSync];

const idMappingByTable = {};
for (const table of tablesToSync) {
  idMappingByTable[table] = await getIdMapping(table);
}

// to prepare, we'll gather all the records that need to be synced, by following a linked-field trail
let linkedFieldTrail = [];
if (SYNCHRONIZE_LINKED_RECORD_DEPTH > 0) {
  linkedFieldTrail = await recurseLinkedFields(
    syncConfig,
    activeTable,
    activeRecord
  );
}

// insert the original record as the first to update
// (first eliminate it in the trail if it already exists)
linkedFieldTrail = linkedFieldTrail.filter(
  (lf) => lf.linkedRecordId !== activeRecord.id
);
linkedFieldTrail = [
  { linkedTable: activeTable.name, linkedRecordId: activeRecord.id },
  ...linkedFieldTrail,
];

const tic = "`";
for (const lf of linkedFieldTrail) {
  if (lf.originTable) {
    log(
      `* ${lf.originTable}[["${lf.originRecordName}"]].${lf.originFieldName} → ${tic}${lf.linkedTable}["${lf.linkedRecordName}"]${tic}`
    );
  } else {
    log(`## ${activeTable.name}[["${activeRecord.name}"]]`);
  }
  const tableToSync = lf.linkedTable;
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
  const sourceFieldNames = sourceTable.fields
    .filter((f) => f.type !== "button")
    .map((f) => f.name);
  const destinationFieldNames = syncConfig.destinationSchema[tableToSync];
  const commonFieldNames = sourceFieldNames.filter((sfn) =>
    destinationFieldNames.includes(sfn)
  );
  const { idMapping } = idMappingByTable[tableToSync];

  const sr = (await sourceTable.selectRecordsAsync()).getRecord(
    lf.linkedRecordId
  );
  if (idMapping[sr.id]) {
    // it exists - update it;
    const destinationRecord = await convertToDestinationRecord(
      sr,
      linkedFields,
      attachmentFieldNames,
      selectFieldNames,
      idMappingByTable,
      commonFieldNames
    );
    await updateRecords(
      syncConfig.destinationApiKey,
      syncConfig.destinationBaseId,
      tableToSync,
      [{ id: idMapping[sr.id], fields: destinationRecord }]
    );
  } else {
    // it doesn't exist - add it;
    const destinationRecord = await convertToDestinationRecord(
      sr,
      linkedFields,
      attachmentFieldNames,
      selectFieldNames,
      idMappingByTable,
      commonFieldNames
    );
    await createRecords(
      syncConfig.destinationApiKey,
      syncConfig.destinationBaseId,
      tableToSync,
      [{ fields: destinationRecord }]
    );
  }

  // update the id mapping for this table (so any linked fields in subsequent tables get the new ids)
  idMappingByTable[tableToSync] = await getIdMapping(tableToSync);
}
/* #endregion Main-Execution */
