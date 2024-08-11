import path from "path";
import { Worker } from 'worker_threads';
import { JobProcessor } from "../interface/JobProcessor"

type ProcessorFunction = (job: any) => Promise<any>;

export class WorkerManager {
    private jobProcessors: Map<string, {
        processorFunction: ProcessorFunction,
        workers: Worker[],
        maxWorkers: number
    }> = new Map();

    registerJobProcessor = (jobType: string, processorFunction: ProcessorFunction, maxWorkers: number) => {
        this.jobProcessors.set(jobType, {
            processorFunction,
            workers: [],
            maxWorkers
        })

        this.spawnWorker(jobType);
    }

    spawnWorker = (jobType: string) => {
        const processorInfo = this.jobProcessors.get(jobType);
        if (!processorInfo || processorInfo.workers.length >= processorInfo.maxWorkers) {
            return;
        }

        const worker = new Worker(path.join(__dirname, '..', 'workers', 'worker.js'), {
            workerData: { jobType }
        })

        worker.postMessage({
            type: 'init',
            processorFunction: processorInfo.processorFunction.toString()
        })

        processorInfo.workers.push(worker)
    }

    processJob = (jobType: string, jobData: any): Promise<any> => {
        const processInfo = this.jobProcessors.get(jobType);
        if (!processInfo) {
            throw new Error(`No processor registered for job type: ${jobType}`);
        }
        const worker = this.getLeastBusyWorker(jobType);

        return new Promise((resolve, reject) => {
            worker.postMessage({
                jobType, jobData
            });
            worker.once('message', result => {
                if (result.error) {
                    reject(new Error(result.error))
                } else {
                    resolve(result.result)
                }
            })
        })
    }

    private getLeastBusyWorker(jobType: string): Worker {
        const processorInfo = this.jobProcessors.get(jobType);
        if (!processorInfo || processorInfo.workers.length === 0) {
            throw new Error(`No workers available for job type: ${jobType}`);
        }

        // TODO: find better algorithm
        return processorInfo.workers[0];
    }
}