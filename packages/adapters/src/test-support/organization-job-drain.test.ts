import type { BackgroundJobHandlers } from "@rakazo/adapter-kit";
import { describe, expect, it, vi } from "vitest";
import { DeterministicOrganizationJobQueue } from "./organization-job-drain.js";

describe("DeterministicOrganizationJobQueue", () => {
  it("dispatches a due job behind a delayed job", async () => {
    const queue = new DeterministicOrganizationJobQueue();
    const handler = vi.fn(async () => undefined);
    const now = new Date("2026-08-30T10:00:00.000Z");

    await queue.enqueue({
      name: "employee.wakeup",
      payload: { workspaceId: "workspace", botId: "later", reason: "scheduled" },
      availableAt: new Date("2026-08-30T10:01:00.000Z"),
    });
    await queue.enqueue({
      name: "employee.wakeup",
      payload: { workspaceId: "workspace", botId: "due", reason: "assigned" },
    });

    const result = await queue.drain(
      { "employee.wakeup": handler } as unknown as BackgroundJobHandlers,
      { now },
    );

    expect(result.processed).toBe(1);
    expect(result.pending).toBe(1);
    expect(handler).toHaveBeenCalledWith({
      workspaceId: "workspace",
      botId: "due",
      reason: "assigned",
    });
    expect(result.trace.map((item) => item.name)).toEqual(["employee.wakeup"]);
  });
});
