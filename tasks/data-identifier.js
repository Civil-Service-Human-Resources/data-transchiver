const db = require('../db/dbService.js');

const selectUsersFromLearnerRecords = `select distinct(user_id), last_updated 
from db_archiver.course_record 
where user_id <> ''
group by user_id
order by user_id asc, last_updated desc`;

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
        console.log("DataIdentifier task is running.....");
        let startTime = process.hrtime();

        let results = await dataIdentifier.queryUsersFromLearnerRecords();
        if ( null !== results ){
            results.forEach(async record => {
                await dataIdentifier.populateCandidateRecords([ 
                    [record.user_id,  record.last_updated]
                ]);
            })
        }
        return process.hrtime(startTime);
    }
}

module.exports = dataIdentifier;
