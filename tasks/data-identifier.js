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
    populateCandidateRecords: async (query) => {
        return await db.insertIntoCandidateRecords(query);
    },
    execute: async () => {
        console.info("DataIdentifier task is running.....");
        let startTime = process.hrtime();

        let records = await dataIdentifier.queryUsersFromLearnerRecords();

        let queryEndTime = process.hrtime(startTime);
        console.info("Query for user learner records took: " + queryEndTime[0] + "." + queryEndTime[1] + " seconds.");

        if ( null !== records ){
            for( const record of records) {
                await dataIdentifier.populateCandidateRecords([ 
                    [record.user_id,  record.updated_at]
                ]);
            }
        }
        endTime = process.hrtime(startTime);
        endTimeMS = endTime[0] + "." + endTime[1];
        return endTimeMS;
    }
}

module.exports = dataIdentifier;
