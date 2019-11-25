const db = require('../db/dbService.js');

const selectCandidateRecords = `select distinct(user_id), last_updated 
from db_archiver.candidate_record 
where user_id <> ''
group by user_id
order by user_id asc, last_updated desc`;

let dataTransfer = {
    getCandidates: async () => {
        let results = await db.queryRecords(
            selectCandidateRecords
        );
        if ( null != results ){
            return results;
        }
        return null;
    },
    copyToTarget: async (users) => {
        users.forEach( async (user) => {
            docs = await db.queryFromCosmos(user.user_id);
            if ( null !== docs && docs !== undefined){
                let result = await db.writeToCosmos(docs);
                // if (result !== undefined){
                //     console.log("insertedCount " + result.insertedCount);
                // }
            }
        });
        return true;
    },
    deleteFromSource: async () => {
        // TODO: delete records from source
        return true;
    },
    execute: async () => {
        console.log("DataTransfer task is running....");
        let startTime = process.hrtime();
        var users = await dataTransfer.getCandidates();
        
        if (null !== users){
            var copied = await dataTransfer.copyToTarget(users);
            if (copied){
                // TODO: update copy_status to COMPLETED
                var deleted = await dataTransfer.deleteFromSource(users);
                if (deleted){
                    // TODO: update delete_status to COMPLETED
                }
            }
        }
        return process.hrtime(startTime);
    }
}

module.exports = dataTransfer;
