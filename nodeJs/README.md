# AirTableSync

Proof of concept for one-way sync for a table in 2 different bases

## Setup

- create an API key on your [account page](https://airtable.com/account)
- create two bases (one will be the sync source, and the other will be the sync destination)
  - each base should have a `Fruits` table (which, in this proof of concept, is hard-coded as the "table to sync")
  - there should be a "Name" field - this acts as a unique key
    - _NOTE: I'd rather use the id field, but I don't see how to create a record in the destination base with a specific id_
  - add as many other fields as you like _(to both tables - the schemas should be the same)_
- take note of the base id's
  - they can be found by clicking on your source & dest bases [here](https://airtable.com/api)
- create env variables, or a `.env` file (in the project's root dir) with the following format:

```
AIRTABLE_API_KEY=someApiKey
SOURCE_BASE_ID=someBaseId
DESTINATION_BASE_ID=someOtherBaseId
```

- install node packages
  - `npm i`

## Running the demo

- make changes in the source db (add/modify/delete records)
- `node index.js`
- note that the destination matches
