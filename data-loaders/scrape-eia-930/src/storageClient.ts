import { MongoClient, Db, Document, ObjectId } from 'mongodb';

export interface PersistenceRecord {
    metadata: {
        scrapeTime: Date
        maxRecordTime: Date
        minRecordTime: Date
        source: string
    },
    results: Array<unknown>
}

export class EiaMongoClient {
    private connectionString: string;
    private mongoClient: MongoClient;
    private database: Db;

    constructor(connectionString: string) {
        this.connectionString = connectionString;
        this.mongoClient = new MongoClient(this.connectionString);
        this.database = this.mongoClient.db('EIA');
    }

    async insertRecord(record: PersistenceRecord): Promise<ObjectId> {
        const collection = this.database.collection<PersistenceRecord>('rawData');
        const results = await collection.insertOne(record);
        return results.insertedId;
    }

    async cleanUp(): Promise<void> {
        this.mongoClient.close();
    }
}