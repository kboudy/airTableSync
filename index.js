require("dotenv").config();

const moment = require("moment"),
  fs = require("fs"),
  path = require("path");

const base1 = require("airtable").base(process.env.BASE1_ID),
  base2 = require("airtable").base(process.env.BASE2_ID);

const AIRTABLE_AND_MOMENT_DATE_FORMAT = "ddd MMM D YYYY h:mm A ZZ";

const lastRunInfoPath = path.join(
  path.dirname(require.main.filename),
  "lastRunInfo.json"
);

const loadLastRunDate = () => {
  if (!fs.existsSync(lastRunInfoPath)) {
    return null;
  }
  return moment(
    JSON.parse(fs.readFileSync(lastRunInfoPath)).lastRunDate,
    AIRTABLE_AND_MOMENT_DATE_FORMAT,
    "utf8"
  );
};

const saveLastRunDate = () => {
  fs.writeFileSync(
    lastRunInfoPath,
    JSON.stringify({
      lastRunDate: moment().format(AIRTABLE_AND_MOMENT_DATE_FORMAT),
    }),
    "utf8"
  );
};

const getFieldOnAllRecords = (base, tableName, fieldName) => {
  return new Promise((resolve, reject) => {
    const fieldsAndIds = {};
    try {
      base(tableName)
        .select({ fields: [fieldName] })
        .eachPage(
          (records, fetchNextPage) => {
            records.forEach((record) => {
              fieldsAndIds[record.get(fieldName)] = record.id;
            });

            fetchNextPage();
          },
          (err) => {
            if (err) {
              reject(err);
            } else {
              resolve(fieldsAndIds);
            }
          }
        );
    } catch (err) {
      console.log(err);
      process.exit(1);
    }
  });
};

const syncTable = async (
  baseOrigin,
  baseDestination,
  tableName,
  lastPulled,
  uniqueField
) => {
  const originFields = Object.keys(
    await getFieldOnAllRecords(baseOrigin, tableName, uniqueField)
  );
  const destinationFieldsAndIds = await getFieldOnAllRecords(
    baseDestination,
    tableName,
    uniqueField
  );
  const toAdd = originFields.filter((f) => !destinationFieldsAndIds[f]);
  const toDelete = Object.keys(destinationFieldsAndIds).filter(
    (f) => !originFields.includes(f)
  );

  if (toDelete.length > 0) {
    const idsToDelete = [];
    for (const d of toDelete) {
      console.log(`Deleting ${d}`);
      idsToDelete.push(destinationFieldsAndIds[d]);
    }
    try {
      await baseDestination(tableName).destroy(idsToDelete);
    } catch (err) {
      console.log(err);
      process.exit(1);
    }
  }

  const filterCriteria = lastPulled
    ? {
        filterByFormula:
          "DATETIME_DIFF(LAST_MODIFIED_TIME(), " +
          `DATETIME_PARSE('${lastPulled.format(
            AIRTABLE_AND_MOMENT_DATE_FORMAT
          )}', '${AIRTABLE_AND_MOMENT_DATE_FORMAT}'),` +
          "'seconds') > 0",
      }
    : {};

  return new Promise((resolve, reject) => {
    baseOrigin(tableName)
      .select(filterCriteria)
      .eachPage(
        async (records, fetchNextPage) => {
          for (const record of records) {
            const recUniqField = record.get(uniqueField);
            if (toAdd.includes(recUniqField)) {
              console.log(`Creating ${recUniqField}`);
              try {
                await baseDestination(tableName).create([
                  { fields: record.fields },
                ]);
              } catch (err) {
                console.log(err);
                process.exit(1);
              }
            } else {
              console.log(`Updating ${recUniqField}`);
              try {
                await baseDestination(tableName).update([
                  {
                    id: destinationFieldsAndIds[recUniqField],
                    fields: record.fields,
                  },
                ]);
              } catch (err) {
                console.log(err);
                process.exit(1);
              }
            }
          }

          fetchNextPage();
        },
        (err) => {
          if (err) {
            reject(err);
            return;
          } else {
            resolve();
          }
        }
      );
  });
};

(async () => {
  await syncTable(base1, base2, "Fruits", loadLastRunDate(), "Name");
  saveLastRunDate();
})();
