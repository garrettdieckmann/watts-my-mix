import axios from 'axios';

export interface EiaFetchOptions {
    ba: string
    start: string,
    end?: string
}

interface EiaApiCallOptions extends EiaFetchOptions {
    facetField: string
}

interface EiaBaseResponse {
    total: number
    dateFormat: string
    frequency: string
    description: string
}

export interface EiaInterchangeResponse extends EiaBaseResponse {
    data: Array<{
        period: string
        fromba: string
        'fromba-name': string
        toba: string
        'toba-name': string
        value: number
        'value-units': string
    }>
}

export interface EiaGenerationResponse extends EiaBaseResponse {
    data: Array<{
        period: string
        respondent: string
        'respondent-name': string
        fueltype: string
        'type-name': string
        value: number
        'value-units': string
    }>
}

export class EIAClient {
    protected EIA_BASE_URL: string;
    private apiKey: string;

    constructor(apiKey: string) {
        this.EIA_BASE_URL = 'https://api.eia.gov/v2';
        this.apiKey = apiKey;
    }

    private async callEiaAPI(url: string, options: EiaApiCallOptions) {
        const params: { [key: string]: string | undefined } = {
            api_key: this.apiKey,
            frequency: 'hourly',
            'data[0]': 'value',
            start: options.start,
            end: options.end
        };
        params[`facets[${options.facetField}][]`] = options.ba;

        const eiaUrl = axios.getUri({
            url,
            baseURL: this.EIA_BASE_URL,
            params
        });
        const results = await axios.get(eiaUrl);
        return results.data.response;
    }

    public async fetchGeneration(options: EiaFetchOptions): Promise<EiaGenerationResponse> {
        const generationUrl = '/electricity/rto/fuel-type-data/data';
        const callOptions = {
            facetField: 'respondent',
            ba: options.ba,
            start: options.start,
            end: options.end
        };
        return await this.callEiaAPI(generationUrl, callOptions);
    }

    public async fetchInterchange(options: EiaFetchOptions): Promise<EiaInterchangeResponse> {
        const interchangeUrl = '/electricity/rto/interchange-data/data';
        const callOptions = {
            facetField: 'fromba',
            ba: options.ba,
            start: options.start,
            end: options.end
        };
        return await this.callEiaAPI(interchangeUrl, callOptions);
    }

    public async fetchDataSet(dataSet: string, options: EiaFetchOptions): Promise<EiaGenerationResponse | EiaInterchangeResponse> {
        const functionMapping: { [key: string]: (options: EiaFetchOptions) => Promise<EiaGenerationResponse | EiaInterchangeResponse> } = {
            generation: this.fetchGeneration,
            interchange: this.fetchInterchange
        };

        const results = await functionMapping[dataSet].call(this, options);
        return results;
    }
}