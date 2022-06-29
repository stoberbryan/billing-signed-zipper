const AWS = require('aws-sdk');
const _ = require('lodash');
const fs = require('fs');
const s3 = new AWS.S3();
const Bucket = 'customer-billing-details-destination'
const file_data = []
const JSZip = require('jszip');
const path = require('path')
const json2xls = require('json2xls');
const { ulid } = require('ulid');
const limit = 9

const handler = async (event) => {
    const allKeys = await getAllKeys({ Bucket });
    return allKeys.length;
};
function getRandomInt(max) {
    return Math.floor(Math.random() * max);
  }
async function getAllKeys(params, allKeys = []) {
    const BATCH_ID = getRandomInt(1000)
    console.log('BATCH_ID', BATCH_ID)
    const response = await s3.listObjectsV2(params).promise();
    response.Contents.forEach(obj => allKeys.push(obj.Key));

    if (response.NextContinuationToken) {
        params.ContinuationToken = response.NextContinuationToken;
        await getAllKeys(params, allKeys); // RECURSIVE CALL
    }

    allKeys = allKeys.filter((x) => {
        const ext = path.extname(x)
        if (ext === '.zip') return false
        const file_name = x.split('/')[3]
        const parent_netsuite_id = parseInt(file_name.split("_")[0]);
        return Number.isFinite(parent_netsuite_id)
    })

    const names = allKeys.map((x) => {
        const folder = x.split('/')[0]
        const parent_netsuite_id = folder.split('_')[0]
        const company_name = folder.split('_')[1]
        const billing_period = x.split('/')[2]
        const file_name = x.split('/')[3]
        return { folder, company_name, Key: x, billing_period, file_name, parent_netsuite_id }
    })

    const customers = _.groupBy(names, 'folder')
    //   console.log(customers)
    let i = 0
    let _parent_netsuite_id
    // const zip = new require('node-zip')();

    for (const custy in customers) {
        if (limit < i) break;
        if (!fs.existsSync(`./tmp/${custy}`)) {
            fs.mkdirSync(`./tmp/${custy}`);
        }
        const zip = new JSZip();
        const obj = customers[custy]
        for await (let x of obj) {
            // console.log(x)
            const { Key, file_name, company_name, billing_period, parent_netsuite_id } = x
            if (!file_name) continue;
            const p = { Bucket, Key }
            const file = (await s3.getObject(p).promise()).Body
            await storeFile(`./tmp/${custy}/${file_name}`, file)
            zip.file(`${billing_period}_${file_name}`, file);
            _parent_netsuite_id = parent_netsuite_id
            console.log(`processed`, i)
        }

        // const data = zip.generate({base64: false, compression:'DEFLATE'});
        await zipStore(custy, zip)
        // console.log(data); // ugly data
        // await storeFile(`./tmp/${custy}/${custy}.zip`, data)
        const newKey = `${custy}/${custy}_${BATCH_ID}_${new Date().toDateString()}.zip`
        const params = {
            Bucket,
            Key: newKey, // File name you want to save as in S3
            Body: fs.readFileSync(`./tmp/${custy}/${custy}.zip`)
        };

        await s3.upload(params).promise();
        delete params['Body']
        params.Expires = 604800
        const url = await getSignedUrl(params)
        console.log(url)
        file_data.push({
            parent_netsuite_id: _parent_netsuite_id,
            url,
            custy,
            i,
            BATCH_ID
        })
        i = i + 1
    }

    const xls = json2xls(file_data);
    fs.writeFileSync(`./urls_${BATCH_ID}.csv`, xls, 'binary');

    return customers;
}

async function zipStore(custy, zip) {
    return new Promise((resolve, reject) => {
        zip.generateNodeStream({ type: 'nodebuffer', streamFiles: true })
            .pipe(fs.createWriteStream(`./tmp/${custy}/${custy}.zip`))
            .on('finish', () => {
                console.log("sample.zip written.");
                return resolve("sample.zip written.")
            });
    })
}

async function storeFile(p, data) {
    return new Promise((resolve, reject) => {
        fs.writeFile(
            p,
            data,
            {
                encoding: "utf8",
                flag: "w",
            },
            (err) => {
                if (err) {
                    console.log(err);
                    reject(err);
                } else {
                    resolve(p);
                }
            }
        );
    });
}

async function removeFile(p) {
    return new Promise((resolve, reject) => {
        fs.unlink(p, (err => {
            if (err) reject(err);
            else {
                resolve(p)
            }
        }));
    })
}

async function getSignedUrl(params) {
    return s3.getSignedUrlPromise('getObject', params);
}

async function uploadS3(Bucket, Key, Body) {
    return s3.upload({ Bucket, Key, Body }).promise()
};

function emptyFolder() {
    return new Promise((resolve, reject) => {
        const directory = 'tmp/';
        fs.readdir(directory, (err, files) => {
            if (err) reject( err);
            for (const file of files) {
                fs.unlink(path.join(directory, file), err => {
                    if (err) reject(err);
                });
            }
            resolve('done')
        });
    })
}

handler()