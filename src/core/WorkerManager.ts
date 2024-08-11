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
  
    /**
     * Register a processor function and set the maximum number of workers for a specific job type.
     * @param jobType - The type of job to register
     * @param processorFunction - The function to process jobs of this type
     * @param maxWorkers - The maximum number of workers allowed for this job type
     * @returns true if registration was successful, false if job type was already registered
     * @throws Error if maxWorkers is invalid
     */
    registerJobProcessor(jobType: string, processorFunction: ProcessorFunction, maxWorkers: number): boolean {
      if (maxWorkers <= 0) {
        throw new Error(`Invalid maxWorkers value: ${maxWorkers}. Must be greater than 0.`);
      }
  
      if (this.jobProcessors.has(jobType)) {
        console.warn(`Job type '${jobType}' is already registered. Skipping registration.`);
        return false;
      }
  
      this.jobProcessors.set(jobType, {
        processorFunction,
        workers: [],
        maxWorkers
      });
  
      try {
        this.spawnWorker(jobType);
        return true;
      } catch (error) {
        console.error(`Failed to spawn initial worker for job type '${jobType}':`, error);
        this.jobProcessors.delete(jobType);
        return false;
      }
    }
  
    /**
     * Spawn a new worker for the specified job type.
     * @param jobType - The type of job for which to spawn a worker
     * @returns true if a new worker was spawned, false if max workers reached
     * @throws Error if job type is not registered or worker creation fails
     */
    private spawnWorker(jobType: string): boolean {
      const processorInfo = this.jobProcessors.get(jobType);
      if (!processorInfo) {
        throw new Error(`No processor registered for job type: ${jobType}`);
      }
  
      if (processorInfo.workers.length >= processorInfo.maxWorkers) {
        console.warn(`Maximum number of workers reached for job type: ${jobType}`);
        return false;
      }
  
      try {
        const worker = new Worker(path.join(__dirname, '..', 'workers', 'worker.js'), {
          workerData: { jobType }
        });
  
        worker.postMessage({
          type: 'init',
          processorFunction: processorInfo.processorFunction.toString()
        });
  
        processorInfo.workers.push(worker);
        console.log(`Spawned new worker for job type: ${jobType}. Total workers: ${processorInfo.workers.length}`);
        return true;
      } catch (error) {
        throw new Error(`Failed to create worker for job type '${jobType}': ${error}`);
      }
    }
  
    /**
     * Process a job of the specified type.
     * @param jobType - The type of job to process
     * @param jobData - The data for the job
     * @returns A promise that resolves with the job result
     * @throws Error if job type is not registered or no workers are available
     */
    async processJob(jobType: string, jobData: any): Promise<any> {
      const processorInfo = this.jobProcessors.get(jobType);
      if (!processorInfo) {
        throw new Error(`No processor registered for job type: ${jobType}`);
      }
  
      if (processorInfo.workers.length === 0) {
        throw new Error(`No workers available for job type: ${jobType}`);
      }
  
      const worker = this.getLeastBusyWorker(jobType);
      return new Promise((resolve, reject) => {
        worker.postMessage({ jobType, jobData });
        worker.once('message', (result) => {
          if (result.error) {
            reject(new Error(result.error));
          } else {
            resolve(result.result);
          }
        });
      });
    }
  
    /**
     * Get the current worker count for the specified job type.
     * @param jobType - The type of job to check
     * @returns The number of active workers for the job type, or 0 if the job type is not registered
     */
    getWorkerCount(jobType: string): number {
      return this.jobProcessors.get(jobType)?.workers.length ?? 0;
    }
  
    /**
     * Check if a new worker can be spawned for the specified job type.
     * @param jobType - The type of job to check
     * @returns True if a new worker can be spawned, false otherwise or if job type is not registered
     */
    canSpawnWorker(jobType: string): boolean {
      const processorInfo = this.jobProcessors.get(jobType);
      return processorInfo ? processorInfo.workers.length < processorInfo.maxWorkers : false;
    }
  
    private getLeastBusyWorker(jobType: string): Worker {
      const processorInfo = this.jobProcessors.get(jobType);
      if (!processorInfo || processorInfo.workers.length === 0) {
        throw new Error(`No workers available for job type: ${jobType}`);
      }
      // For simplicity, we're just returning the first worker.
      // In a real system, you'd implement logic to track and return the least busy worker.
      return processorInfo.workers[0];
    }
  }