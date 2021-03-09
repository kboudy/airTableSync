// This is a one-off script for copying the Level Team Members to an Employee-sync'd version (2021-03-08)

require("dotenv").config();

const axios = require("axios");

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";

const SOURCE_TABLE_NAME = "Level Team Members";
const DEST_TABLE_NAME = "Level Team Members - Synced";

const columns_to_copy = [
  { column: "Company Deal Team", table: "Companies" },
  { column: "Company Board Member", table: "Companies" },
  { column: "Company Board Observer", table: "Companies" },
  { column: "Company Coverage Team", table: "Companies" },
  { column: "Bids", table: "LOIs" },
  { column: "LOIs", table: "LOIs" },
];

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

    const response = await axios.get(queryUrl, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    allRecords.push(...response.data.records);
    offset = response.data.offset;
  } while (offset);

  return allRecords;
};

const updateRecords = async (apiKey, baseId, tableName, records) => {
  // NOTE: airtable REST API for create & update methods allows a max of 10 records per request
  let remainingRecords = [...records];

  while (remainingRecords.length > 0) {
    let currentChunk = remainingRecords.slice(0, MAX_RECORDS_PER_REQUEST);
    remainingRecords = remainingRecords.slice(MAX_RECORDS_PER_REQUEST);
    await axios.patch(
      `${API_BASE_URL}${baseId}/${tableName}?`,
      { records: currentChunk, typecast: true },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );
  }
};

const getMappedIds = (mapping, table, column, record) => {
  const sourceIds = record.fields[column];
  const destinationIds = [];
  for (const sId of sourceIds) {
    const dId = mapping[table][sId];
    if (dId) {
      destinationIds.push(dId);
    }
  }
  return destinationIds;
};

/* #endregion Methods */

/* #region Main-Execution */
(async () => {
  const tables = Array.from(new Set(columns_to_copy.map((c2c) => c2c.table)));

  const mapping = {};
  for (const t of tables) {
    mapping[t] = {};
    const sourceRecords = await getRecords(
      process.env.ACCOUNT_API_KEY,
      process.env.BASE_ID,
      t
    );
    const destinationRecords = await getRecords(
      process.env.ACCOUNT_API_KEY,
      process.env.BASE_ID,
      `${t} sync`
    );
    if (sourceRecords.length !== destinationRecords.length) {
      throw "Expected tables to have the same number of records";
    }
    for (const sr of sourceRecords) {
      if (typeof sr.fields.Name === "object") {
        // they have an "{error:"#error!"}" named record for LOIs.Bloomfire.  Ignore it
        continue;
      }
      const matchingDestinationRecords = destinationRecords.filter(
        (dr) => sr.fields.Name === dr.fields.Name
      );
      if (matchingDestinationRecords.length !== 1) {
        throw "Expected each source record to have 1 match on the 'Name' column";
      }
      mapping[t][sr.id] = matchingDestinationRecords[0].id;
    }
  }

  // now we have a mapping table for companies & LOIs record id's.  We'll convert them in the Level Team Members' columns
  const levelTeamMembersRecords = await getRecords(
    process.env.ACCOUNT_API_KEY,
    process.env.BASE_ID,
    "Level Team Members"
  );
  const recordsToUpdate = [];
  for (const r of levelTeamMembersRecords) {
    const fieldsToUpdate = {};
    for (const c2c of columns_to_copy) {
      const { column, table } = c2c;
      if (r.fields[column]) {
        fieldsToUpdate[`${column} Sync`] = getMappedIds(
          mapping,
          table,
          column,
          r
        );
      }
    }

    if (Object.keys(fieldsToUpdate).length > 0) {
      recordsToUpdate.push({ id: r.id, fields: fieldsToUpdate });
    }
  }

  await updateRecords(
    process.env.ACCOUNT_API_KEY,
    process.env.BASE_ID,
    "Level Team Members",
    recordsToUpdate
  );
})();
