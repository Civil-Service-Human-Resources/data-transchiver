
let sleep =  (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

let dataTransfer = {
    copyToTarget: async () => {
        console.log("|___ Copying data to target....");
        await sleep(10000);
    },
    execute: async () => {
        console.log("DataTransfer task is running....");
        let startTime = process.hrtime();
        
        await dataTransfer.copyToTarget();
        await dataTransfer.deleteFromSource();

        return process.hrtime(startTime);
    },
    deleteFromSource: async () => {
        console.log("|___ Deleting records from source....");
        await sleep(15000);
    }
}

module.exports = dataTransfer;
