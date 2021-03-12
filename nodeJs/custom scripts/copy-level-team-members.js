// This is a one-off script for copying the Level Team Members to an Employee-sync'd version (2021-03-08)

require("dotenv").config();

const axios = require("axios");

const MAX_RECORDS_PER_REQUEST = 10;
const API_BASE_URL = "https://api.airtable.com/v0/";

const SOURCE_TABLE_NAME = "Level Team Members";
const DEST_TABLE_NAME = "Level Team Members : Employee Sync";

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
/* #endregion Methods */

/* #region Main-Execution */
(async () => {
  const sourceRecords = await getRecords(
    process.env.ACCOUNT_API_KEY,
    process.env.BASE_ID,
    SOURCE_TABLE_NAME
  );
  const destinationRecords = await getRecords(
    process.env.ACCOUNT_API_KEY,
    process.env.BASE_ID,
    DEST_TABLE_NAME
  );
  const recordsToUpdate = [];
  for (const sr of sourceRecords) {
    const matchingDestRecords = destinationRecords.filter(
      (dr) => dr.fields.Name.toLowerCase() === sr.fields.Name.toLowerCase()
    );
    if (matchingDestRecords.length > 0) {
      const mr = matchingDestRecords[0];
      const fieldsToUpdate = {};
      for (const fieldName of Object.keys(sr.fields)) {
        if (!["Name", "Email", "Phone"].includes(fieldName)) {
          fieldsToUpdate[fieldName] = sr.fields[fieldName];
        }
      }
      recordsToUpdate.push({ id: mr.id, fields: fieldsToUpdate });
    }
  }
  await updateRecords(
    process.env.ACCOUNT_API_KEY,
    process.env.BASE_ID,
    DEST_TABLE_NAME,
    recordsToUpdate
  );
})();
