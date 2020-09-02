require("dotenv").config();

const moment = require("moment"),
  fs = require("fs"),
  chalk = require("chalk"),
  path = require("path");

const baseOrigin = require("airtable").base(process.env.SOURCE_BASE_ID),
  baseDestination = require("airtable").base(process.env.DESTINATION_BASE_ID);

const TABLE_TO_SYNC = "Fruits";
const UNIQUE_FIELD_NAME = "Name";
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
  bOrigin,
  bDestination,
  tableName,
  lastPulled,
  uniqueField
) => {
  const originFields = Object.keys(
    await getFieldOnAllRecords(bOrigin, tableName, uniqueField)
  );
  const destinationFieldsAndIds = await getFieldOnAllRecords(
    bDestination,
    tableName,
    uniqueField
  );
  const toAdd = originFields.filter((f) => !destinationFieldsAndIds[f]);
  const toDelete = Object.keys(destinationFieldsAndIds).filter(
    (f) => !originFields.includes(f)
  );

  const resultCounts = { updated: 0, deleted: 0, created: 0 };
  if (toDelete.length > 0) {
    const idsToDelete = [];
    for (const d of toDelete) {
      console.log(chalk.gray(` - deleting "${d}"`));
      idsToDelete.push(destinationFieldsAndIds[d]);
    }
    try {
      await bDestination(tableName).destroy(idsToDelete);
      resultCounts.deleted = resultCounts.deleted + idsToDelete.length;
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
    bOrigin(tableName)
      .select(filterCriteria)
      .eachPage(
        async (records, fetchNextPage) => {
          for (const record of records) {
            const recUniqField = record.get(uniqueField);
            if (toAdd.includes(recUniqField)) {
              console.log(chalk.gray(` - creating "${recUniqField}"`));
              try {
                await bDestination(tableName).create([
                  { fields: record.fields },
                ]);
                resultCounts.created++;
              } catch (err) {
                console.log(err);
                process.exit(1);
              }
            } else {
              console.log(chalk.gray(` - Updating "${recUniqField}"`));
              try {
                await bDestination(tableName).update([
                  {
                    id: destinationFieldsAndIds[recUniqField],
                    fields: record.fields,
                  },
                ]);
                resultCounts.updated++;
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
            resolve(resultCounts);
          }
        }
      );
  });
};

(async () => {
  console.log(`Synchronizing the "${TABLE_TO_SYNC}" table...`);
  const resultCounts = await syncTable(
    baseOrigin,
    baseDestination,
    TABLE_TO_SYNC,
    loadLastRunDate(),
    UNIQUE_FIELD_NAME
  );
  console.log();
  if (
    resultCounts.created + resultCounts.updated + resultCounts.deleted ===
    0
  ) {
    console.log(chalk.blue(`(no changes since last run)`));
  } else {
    console.log(chalk.blue(`Results:`));
    console.log(chalk.green(`  - created: ${resultCounts.created}`));
    console.log(chalk.yellow(`  - updated: ${resultCounts.updated}`));
    console.log(chalk.red(`  - deleted: ${resultCounts.deleted}`));
  }
  saveLastRunDate();
})();
