import {
  type BackgroundJob,
  type BackgroundJobHandlers,
  dispatchBackgroundJob,
  type JobPublisher,
} from "@rakazo/adapter-kit";

/**
 * Test-only deterministic transport for exercising the production job handlers.
 * It is deliberately a queue, not an organization simulator: handlers remain
 * responsible for every state transition and newly published next step.
 */
export class DeterministicOrganizationJobQueue implements JobPublisher {
  private readonly queued: BackgroundJob[] = [];
  readonly trace: Array<{ name: string; payload: unknown }> = [];

  async enqueue(job: BackgroundJob): Promise<void> {
    if (job.replaceKey) {
      const index = this.queued.findIndex((queued) => queued.replaceKey === job.replaceKey);
      if (index >= 0) this.queued.splice(index, 1);
    }
    this.queued.push(job);
  }

  async cancel(key: string): Promise<void> {
    for (let index = this.queued.length - 1; index >= 0; index -= 1) {
      if (this.queued[index]?.replaceKey === key) this.queued.splice(index, 1);
    }
  }

  async close(): Promise<void> {}

  get size() {
    return this.queued.length;
  }

  async drain(handlers: BackgroundJobHandlers, options: { maxJobs?: number; now?: Date } = {}) {
    const maxJobs = options.maxJobs ?? 100;
    let processed = 0;
    while (this.queued.length > 0) {
      if (processed >= maxJobs) {
        throw new Error(
          `Organization job drain exceeded ${maxJobs} jobs: ${this.trace.map((item) => item.name).join(" → ")}`,
        );
      }
      const now = options.now ?? new Date();
      const index = this.queued.findIndex((item) => !item.availableAt || item.availableAt <= now);
      if (index < 0) break;
      const job = this.queued.splice(index, 1)[0]!;
      this.trace.push({ name: job.name, payload: job.payload });
      await dispatchBackgroundJob(handlers, job.name, job.payload);
      processed += 1;
    }
    return { processed, pending: this.queued.length, trace: [...this.trace] };
  }
}
