const db = require('../db/dbService.js');
const moment = require('moment');
const md5 = require('md5');
const mongodb = require('mongodb');
const dbCredentials = require('../constants/dbCredentials.js');

const selectCandidateRecords = `select user_id, DATE_FORMAT(max(updated_at), '%Y-%m-%dT%TZ') updated_at 
from db_archiver.candidate_record 
where user_id <> ''
group by user_id
order by updated_at desc`;

const _COMPLETED_ = "COMPLETED";
const _ERROR_ = "ERROR";
const _SKIPPED_ = "SKIPPED";
const DELETE_BATCH_SIZE = process.env.STATEMENTS_DELETE_BATCH_SIZE || 100;
console.info("using DELETE_BATCH_SIZE " + DELETE_BATCH_SIZE);

const MongoClient = mongodb.MongoClient;

let dataTransfer = {
    getCandidates: async () => {
        try{
            var client = await db.getConnection();
            await client.connect();
            let results = await db.queryRecords(
                selectCandidateRecords,
                client
            );
            await client.commit();
            if (null != results) {
                return results;
            }
            return null;
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
    },
    prepareForDelete: (docs) => {
        let newDocs = [];
        for (const doc of docs) {
            let _id = doc._id;
            newDocs.push(_id);
        }
        return newDocs;
    },
    prepareForCopy: (docs) => {
        let newDocs = [];
        for (const doc of docs) {
            let ts = doc.statement.timestamp;
            let user_id = doc.statement.actor.account.name;
            let statement_time = ts.slice(0, 19).replace('T', ' ');
            let inserted_time = moment().format("YYYY-MM-DD HH:mm:ss");
            let statement = JSON.stringify(doc);
            let statement_hash = md5(statement);

            var jsonRecord = [statement_hash, user_id, inserted_time, statement_time, statement];
            newDocs.push(jsonRecord);
        }
        return newDocs;
    },
    deleteFromSource: async (docs, client) => {
        const BATCH_SIZE = DELETE_BATCH_SIZE;
        documents_count = docs.length;

        var i, j, deleted = 0;
        for (i = 0, j = documents_count; i < j; i += BATCH_SIZE) {

            let docs_chunck = docs.slice(i, i + BATCH_SIZE);

            let docs_to_delete = dataTransfer.prepareForDelete(docs_chunck);
            let result = await db.deleteFromCosmos(docs_to_delete, client);
            if (null !== result && result.deletedCount > 0) {
                deleted += result.deletedCount;
            }
        }

        return deleted;
    },
    doTransfer: async (users) => {
        var statementsFound, statementsFound_total = 0;
        var statementsCopied, statementsCopied_total = 0;
        var statementsDeleted, statementsDeleted_total = 0;
        var statementsReplaced, statementsReplaced_total = 0;

        let mySqlClient, mongoClient;

        try {
            mongoClient = await MongoClient.connect(dbCredentials.cosmos_src_connection_string, {
                useUnifiedTopology: true,
                useNewUrlParser: true
            });
            mySqlClient = await db.getConnectionToTarget();
            await mySqlClient.connect();

            for (const user of users) {
                statementsFound, statementsCopied = 0;
                statementsReplaced, statementsDeleted = 0;

                docs = await db.queryFromCosmos(user, mongoClient);

                if (null !== docs && docs.length > 0) {
                    statementsFound = docs.length;
                    statementsFound_total += statementsFound;

                    let docs_to_copy = dataTransfer.prepareForCopy(docs);
                    let result = await db.copyToTargetMysql(docs_to_copy, mySqlClient);

                    if (result !== null && result.affectedRows > 0) {
                        let affected_rows = result.affectedRows;

                        if (affected_rows > statementsFound) {
                            statementsReplaced = affected_rows - statementsFound;
                            statementsCopied = statementsFound;
                        } else {
                            statementsCopied = affected_rows;
                        }
                        statementsCopied_total += statementsCopied;
                        statementsReplaced_total += statementsReplaced;
                    }
                    await db.updateNumOfRecordsCopied(user.user_id, statementsCopied, mySqlClient);

                    if (statementsCopied === statementsFound) {
                        await db.updateCopyStatus(user.user_id, _COMPLETED_);

                        statementsDeleted += await dataTransfer.deleteFromSource(docs, mongoClient);
                        statementsDeleted_total += statementsDeleted;
                        await db.updateNumOfRecordsDeleted(user.user_id, statementsDeleted, mySqlClient);

                        if (statementsDeleted === statementsCopied) {
                            await db.updateDeleteStatus(user.user_id, _COMPLETED_, mySqlClient);
                        } else {
                            await db.updateDeleteStatus(user.user_id, _ERROR_, mySqlClient);
                        }
                    } else {
                        await db.updateCopyStatus(user.user_id, _ERROR_, mySqlClient);
                        await db.updateDeleteStatus(user.user_id, _SKIPPED_, mySqlClient);
                    }
                } else {
                    await db.updateCopyStatus(user.user_id, _ERROR_, mySqlClient);
                    await db.updateDeleteStatus(user.user_id, _SKIPPED_, mySqlClient);
                }
                await db.updateNumOfRecordsCopied(user.user_id, statementsCopied, mySqlClient);
                await db.updateNumOfRecordsDeleted(user.user_id, statementsDeleted, mySqlClient);

                await db.updateTranferTime(user.user_id, moment().format('YYYY-MM-DD HH:mm:ss'), mySqlClient);
            }
        } catch (err) {
            throw err
        } finally {
            db.disconnect(mongoClient);
            db.disconnect(mySqlClient);
        }
        return [
            statementsFound_total, statementsCopied_total,
            statementsReplaced_total, statementsDeleted_total
        ];
    },
    printJobStatus: (
        usersCount, docsFound, docsCopied, docsReplaced, docsDeleted,
        transferStarted, isTransferSuccessful) => {
        console.info("learner records FOUND : " + ((null !== usersCount   && usersCount   > 0) ? usersCount   : 0));
        console.info("statements FOUND      : " + ((null !== docsFound    && docsFound    > 0) ? docsFound    : 0));
        console.info("statements COPIED     : " + ((null !== docsCopied   && docsCopied   > 0) ? docsCopied   : 0));
        console.info("statements REPLACED   : " + ((null !== docsReplaced && docsReplaced > 0) ? docsReplaced : 0));
        console.info("statements DELETED    : " + ((null !== docsDeleted  && docsDeleted  > 0) ? docsDeleted  : 0));
        console.info("Transfer status       : " + (
            (transferStarted && isTransferSuccessful || !transferStarted ? "Success" : "Failed")
        ));
    },
    execute: async () => {
        console.info("DataTransfer task is running....");
        let startTime = process.hrtime();
        var isTransferSuccessful = true;
        var transferStarted = false;

        var users, usersCount = 0;
        var docsFound = 0;
        var docsCopied = 0;
        var docsReplaced = 0;
        var docsDeleted = 0;

        users = await dataTransfer.getCandidates();

        if (users !== null && users.length > 0) {
            usersCount = users.length;
            console.log("Number of users: " + usersCount);
            var transferStarted = true;
            [docsFound, docsCopied, docsReplaced, docsDeleted] = await dataTransfer.doTransfer(users);

            if (docsCopied && docsCopied > 0) {
                if (docsCopied !== docsDeleted) {
                    isTransferSuccessful = false;
                }
            }
        }

        endTime = process.hrtime(startTime);
        dataTransfer.printJobStatus(
            usersCount, docsFound, docsCopied,
            docsReplaced, docsDeleted, transferStarted, isTransferSuccessful
        );
        endTimeMS = endTime[0] + "." + endTime[1];
        return endTimeMS;
    }
}

module.exports = dataTransfer;