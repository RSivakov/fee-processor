require('dotenv').config();

const axios = require('axios');
const fs = require('fs');
const axiosRateLimit = require('axios-rate-limit');
const { assert } = require('console');
const https = require('https');
const createCSV = require('csv-writer').createObjectCsvWriter;


const GRAPH_BASE_URL = 'https://api.thegraph.com/subgraphs/name/paraswap/paraswap-subgraph';
const GRAPH_PAGE_SIZE = 1000;
const GRAPH_MAX_SKIP = 5000;
const GRAPH_RETRIES = 10;
const GRAPH_RETRIES_SLEEP_MS = 1 * 1000;
const CHAINS_CONFIGURATION = {
    '1': {},
    '10': { urlSuffix: '-optimism' },
    '56': { urlSuffix: '-bsc' },
    '137': { urlSuffix: '-polygon' },
    '250': { urlSuffix: '-fantom' },
    '42161': { urlSuffix: '-arbitrum' },
    '43114': { urlSuffix: '-avalanche' },
}


const httpsAgent = new https.Agent({ keepAlive: true });
const http = axiosRateLimit(axios.create({ httpsAgent }), { maxRPS: 5 });


async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


async function fetchSupbgraphPage(referrer, url, blockNumber, skip) {
    console.info(`Fetching blockNumber: ${blockNumber}, skip: ${skip}} from ${url}`);
    const query =
        `
    {
        swaps(
            first: ${GRAPH_PAGE_SIZE},
            skip: ${skip},
            orderBy: timestamp, 
            orderDirection: asc,
            where: { blockNumber_gte: ${blockNumber}, referrer: "${referrer}", referrerFee_gt: 0 }
        ) {
            id
            blockNumber
            feeToken
            referrer
            referrerFee
        }
    } 
    `.trim()
    const response = await http.post(url, { query });
    const { data: { data: { swaps } } } = response;
    return swaps;
}


async function getAllChainFees(referrer) {
    const dir = `./data/${referrer}`
    if (!fs.existsSync(dir)){
        fs.mkdirSync(dir, { recursive: true });   
    }

    const chainsKeys = Object.keys(CHAINS_CONFIGURATION);
    for (const c of chainsKeys) {
        console.info(`Starting fee indexing for chain ${c}`);

        const subgraphUrl = CHAINS_CONFIGURATION[c].urlSuffix ? GRAPH_BASE_URL + CHAINS_CONFIGURATION[c].urlSuffix : GRAPH_BASE_URL;

        let chainResults = {};
        let blockNumber = 0;
        let skip = 0;
        const lastIndexedReferrerFees = new Set();
        while (true) { // external loop for fetching pages while we have more
            let retry = 0;
            let swaps = [];
            do { // internal do..while loop for retires
                try {
                    swaps = await fetchSupbgraphPage(referrer, subgraphUrl, blockNumber, skip);
                    if (swaps && swaps.length) {
                        // find the last one we indexed
                        let i = 0
                        while (i < swaps.length) {
                            if (lastIndexedReferrerFees.has(swaps[i].id)) {
                                i++;
                            } else {
                                break; // break the search for last index indexed
                            }
                        }
                        console.log(`Staring page indexing from index ${i}`);

                        lastIndexedReferrerFees.clear();

                        // index the new referrer fees
                        for (let s = i; s < swaps.length; s++) {
                            const { id, feeToken, referrer, referrerFee } = swaps[s];
                            lastIndexedReferrerFees.add(id);
                            if (!(referrer in chainResults)) {
                                chainResults[referrer] = {};
                            }
                            const referrerResults = chainResults[referrer];
                            if (!(feeToken in referrerResults)) {
                                referrerResults[feeToken] = 0n;
                            }
                            referrerResults[feeToken] += BigInt(referrerFee);
                        }
                        console.info(`Indexed ${swaps.length} swaps`);
                    } else {
                        console.warn('Found no swaps in last request!');
                    }
                    break; // break the internal do..while retry loop
                } catch (e) {
                    console.error('Failed querying subgraph', e);
                    await sleep(GRAPH_RETRIES_SLEEP_MS);
                }
            } while (retry++ < GRAPH_RETRIES)

            if (swaps.length < GRAPH_PAGE_SIZE) {
                console.info('No more data for chain');
                break; // break the external while(true) loop
            } else {
                // Increment skip if we have more room
                if (skip + GRAPH_PAGE_SIZE <= GRAPH_MAX_SKIP) {
                    skip += GRAPH_PAGE_SIZE;
                    continue;
                }

                // Increment blockNumber to the last block we got
                blockNumber = swaps.slice(-1)[0].blockNumber;
                skip = 0;
            }
        } // loop single chain

        if(!Object.keys(chainResults).length){
            console.info(`No records for ${referrer} in ${c}`);
            continue;
        }
        // write results
        const filename = `./${dir}/referrer_fees_${c}.csv`;
        console.info(`Writing data to csv '${filename}'`);
        const csv = createCSV({
            path: `./${filename}`,
            alwaysQuote: true,
            header: [
                { id: 'r', title: 'referrer' },
                { id: 't', title: 'token' },
                { id: 'f', title: 'fees' },
            ]
        });
        const records = [];
        for (const [referrer, referrerTokens] of Object.entries(chainResults)) {
            for (const [referrerToken, fees] of Object.entries(referrerTokens)) {
                records.push({ r: referrer, t: referrerToken, f: fees })
            }
        }
        csv.writeRecords(records);
        console.info(`Wrote ${records.length} records to csv '${filename}'`);

    } // loop chains
}

if(!process.env.REFERRERS) throw new Error("Missing referrer in .env");

const referrers = process.env.REFERRERS.split(',');

referrers.forEach(getAllChainFees);