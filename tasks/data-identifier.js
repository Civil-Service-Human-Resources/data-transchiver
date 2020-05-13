const db = require('../db/dbService.js');

const selectUsersFromLearnerRecords = `select user_id, max(updated_at) updated_at 
from learner_record.module_record 
where user_id <> ''
group by user_id
order by updated_at desc`;

let dataIdentifier = {
    queryUsersFromLearnerRecords: async () => {
        return await db.queryFromLearnerRecords(
            selectUsersFromLearnerRecords
        );
    },
    populateCandidateRecords: async (query, con) => {
        return await db.insertIntoCandidateRecords(query, con);
    },
    proccessCandidateRecords: async (records) => {
        try {
            var con = await db.getConnection();
            for (const record of records) {
                await dataIdentifier.populateCandidateRecords([[record.user_id,  record.updated_at]], con);
            }
        } catch (err){
            throw err;
        } finally {
            await db.disconnect(con);
        }
    },
    execute: async () => {
        console.info("DataIdentifier task is running.....");
        let startTime = process.hrtime();

        let records = await dataIdentifier.queryUsersFromLearnerRecords();
        if (records !== null) {
            await dataIdentifier.proccessCandidateRecords(records);
            console.log("Number of records: " + records.length);
        }
        endTime = process.hrtime(startTime);
        endTimeMS = endTime[0] + "." + endTime[1];
        return endTimeMS;
    }
}

module.exports = dataIdentifier;
