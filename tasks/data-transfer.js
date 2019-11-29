const db = require('../db/dbService.js');
const moment = require('moment');
const md5 = require('md5');

const selectCandidateRecords = `select distinct(user_id) user_id, DATE_FORMAT(max(updated_at), '%Y-%m-%dT%TZ') updated_at 
from db_archiver.candidate_record 
where user_id <> ''
group by user_id
order by updated_at desc`;

const _COMPLETED_ = "COMPLETED";
const _ERROR_ = "ERROR";
const _SKIPPED_ = "SKIPPED";

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
    prepareForDelete: (docs) => {
        let newDocs = [];
        for (const doc of docs){
            let _id = doc._id;
            newDocs.push(_id);
        }
        return newDocs;
    },
    prepareForCopy: (docs) => {
        let newDocs = [];
        for (const doc of docs){
            let ts = doc.statement.timestamp;
            let user_id =  doc.statement.actor.account.name;
            let statement_time = ts.slice(0, 19).replace('T', ' ');
            let inserted_time = moment().format("YYYY-MM-DD HH:mm:ss");
            let statement = JSON.stringify(doc);
            let statement_hash = md5(statement);

            var jsonRecord = [statement_hash, user_id, inserted_time, statement_time, statement];
            newDocs.push(jsonRecord);
        }
        return newDocs;
    },
    deleteFromSource: async (docs) => {
        let deleted = 0;
        let docs_to_delete = dataTransfer.prepareForDelete(docs);
        let result = await db.deleteFromCosmos(docs_to_delete);
        if (null != result && result.deletedCount > 0){
            deleted = result.deletedCount;
        }
        return deleted;
    },
    doTransfer: async (users) => {
        var statementsFound = 0;
        var statementsCopied = 0;
        var statementsDeleted = 0;

        var statementsFound_total = 0;
        var statementsCopied_total = 0;
        var statementsDeleted_total = 0;

        for (const user of users) {
            statementsFound = 0;
            statementsCopied = 0;
            statementsDeleted = 0;

            docs = await db.queryFromCosmos(user);

            if ( null !== docs && docs.length > 0){
                statementsFound = docs.length;
                statementsFound_total += statementsFound;
                
                let docs_to_copy = dataTransfer.prepareForCopy(docs);
                let result = await db.copyToTargetMysql(docs_to_copy);

                if (null !== result && result.affectedRows > 0){
                    statementsCopied = result.affectedRows;
                    statementsCopied_total += statementsCopied;
                }
                await db.updateNumOfRecordsCopied(user.user_id, statementsCopied);
            
                if ( statementsCopied >= statementsFound ){
                    await db.updateCopyStatus(user.user_id, _COMPLETED_);

                    statementsDeleted += await dataTransfer.deleteFromSource(docs);
                    statementsDeleted_total += statementsDeleted;
                    await db.updateNumOfRecordsDeleted(user.user_id, statementsDeleted);

                    if ( statementsDeleted === statementsFound ){
                        await db.updateDeleteStatus(user.user_id, _COMPLETED_);
                    }else{
                        await db.updateDeleteStatus(user.user_id, _ERROR_);
                    }
                }else{
                    await db.updateCopyStatus(user.user_id, _ERROR_);
                    await db.updateDeleteStatus(user.user_id, _SKIPPED_);
                }
            }else{
                await db.updateCopyStatus(user.user_id, _SKIPPED_);
                await db.updateDeleteStatus(user.user_id, _SKIPPED_);
            }
            await db.updateNumOfRecordsCopied(user.user_id, statementsCopied);
            await db.updateNumOfRecordsDeleted(user.user_id, statementsDeleted);

            await db.updateTranferTime(user.user_id, moment().format('YYYY-MM-DD HH:mm:ss'));
        }
        return [statementsFound_total, statementsCopied_total, statementsDeleted_total];
    },
    printJobStatus: (
        usersCount,docsFound, docsCopied, docsDeleted, 
        transferStarted, isTransferSuccessful) => {
        console.log("learners FOUND       : " + ((null !== usersCount  && usersCount  > 0)? usersCount: 0));
        console.log("statements FOUND     : " + ((null !== docsFound   && docsFound   > 0)? docsFound:  0));
        console.log("statements COPIED    : " + ((null !== docsCopied  && docsCopied  > 0)? docsCopied: 0));
        console.log("statements DELETED   : " + ((null !== docsDeleted && docsDeleted > 0)? docsDeleted:0));
        console.log("Transfer status      : " + (
            (transferStarted && isTransferSuccessful || !transferStarted? "Success" : "Failed")
        ));
    },
    execute: async () => {
        console.log("DataTransfer task is running....");
        let startTime = process.hrtime();
        var isTransferSuccessful = true;
        var transferStarted = false;

        var users, usersCount = 0;
        var docsFound = 0;
        var docsCopied = 0;
        var docsDeleted = 0;

        users = await dataTransfer.getCandidates();

        if (null !== users && users.length > 0){
            usersCount = users.length;
            var transferStarted = true;
            [docsFound, docsCopied, docsDeleted] = await dataTransfer.doTransfer(users);

            if (docsCopied && docsCopied > 0){
                if ( docsCopied < docsDeleted ){
                    isTransferSuccessful = false;
                }
            }
        }

        endTime = process.hrtime(startTime);
        dataTransfer.printJobStatus(
            usersCount,docsFound, docsCopied, 
            docsDeleted,transferStarted, isTransferSuccessful
        );
        endTimeMS = endTime[0] + "." + endTime[1];
        return endTimeMS;
    }
}

module.exports = dataTransfer;
