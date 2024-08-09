import Queue from "bull";

export class TaskQueue {
    private queue: Queue.Queue;

    constructor(name: string, redisUrl: string, options?: Queue.QueueOptions) {
        this.queue = new Queue(name, redisUrl, options);
    }

    async addTask(data: any, options?: Queue.JobOptions) : Promise<Queue.Job> {
        return this.queue.add(data, options)
    }

    async processTask(handler: (job: Queue.Job) => Promise<any>) : Promise<void> {
        this.queue.process(handler);
    }

    async getJob(jobId: string | number) : Promise<Queue.Job | null> {
        return this.queue.getJob(jobId);
    }

    async getMetrics(): Promise<{waiting: number; active: number; completed: number; failed: number}> {
        const [waiting, active, completed, failed] = await Promise.all([
            this.queue.getWaitingCount(),
            this.queue.getActiveCount(),
            this.queue.getCompletedCount(),
            this.queue.getFailedCount(),
        ]);

        return { waiting, active, completed, failed}
    }
}