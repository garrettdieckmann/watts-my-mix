import { Command, OptionValues } from 'commander';
import { EIAClient, EiaFetchOptions } from "./EIAClient";
import { EiaMongoClient, PersistenceRecord } from './storageClient';

const program = new Command();

program
    .version('1.0.0')
    .description('CLI script to scrape EIA 930 data')
    .requiredOption('-ba, --balancing-authority <value>', 'Balancing Authority to scrape for')
    .requiredOption('-f, --from-time <datetime>', 'Earliest datetime (format: \'2022-12-13T00\') to pull')
    .option('-t, --to-time <datetime>', 'Latest datetime (format: \'2022-12-13T24\') to pull')
    .option('-d, --data-set <value>', 'Which dataset to retrieve. Allowed: [\'generation\', \'interchange\']', 'generation')
    .parse(process.argv);

const options = program.opts();

cli(options);

async function cli(options: OptionValues) {
    const envVars = parseEnvVars();

    const eiaClient = new EIAClient(envVars.apiKey);
    const mongoClient = new EiaMongoClient(envVars.mongoConnectionString);

    const fetchOptions: EiaFetchOptions = {
        ba: options.balancingAuthority,
        start: options.fromTime,
        end: options.toTime
    };

    try {
        const fetchResults = await eiaClient.fetchDataSet(options.dataSet, fetchOptions);
        console.log(`Number of fetch records: ${fetchResults.total}`);
    
        const record = convertEiaResponseToRecord(options.dataSet, fetchResults.data);
        const storeResults = await mongoClient.insertRecord(record);
        console.log(`Stored records with ID: ${storeResults}`);
    } finally {
        await mongoClient.cleanUp();
    }
}

function parseEnvVars() {
    const apiKey = process.env.EIA_API_KEY;
    if (typeof apiKey === 'undefined') {
        throw new Error('Must set "EIA_API_KEY" environment variable');
    }
    const mongoConnectionString = process.env.MONGO_CONNECTION_STRING;
    if (typeof mongoConnectionString === 'undefined') {
        throw new Error('Must set "MONGO_CONNECTION_STRING" environment variable');
    }
    return {
        apiKey,
        mongoConnectionString
    };
}

function convertEiaResponseToRecord(source: string, data: Array<any>): PersistenceRecord {
    const recordTimes = data.map((item) => new Date(`${item.period}:00:00Z`).getTime());

    return {
        metadata: {
            scrapeTime: new Date(),
            maxRecordTime: new Date(Math.max(...recordTimes)),
            minRecordTime: new Date(Math.min(...recordTimes)),
            source
        },
        results: data
    };
}
