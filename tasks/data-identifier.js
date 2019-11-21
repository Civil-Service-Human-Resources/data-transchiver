let sleep =  (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

let dataIdentifier = {
    execute: async () => {
        console.log("DataIdentifier task is running.....");
        let startTime = process.hrtime();
        await sleep(10000);
        return process.hrtime(startTime);
    }
}

module.exports = dataIdentifier;
