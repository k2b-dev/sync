// ==========================
// v6 Error Classes
// ==========================

export class SyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The Sync instance or a primitive was used before/after its valid lifecycle window. */
export class SyncLifecycleError extends SyncError {}

/** The supplied NATS connection or server does not meet v6 requirements. */
export class UnsupportedServerError extends SyncError {}

/** A user-supplied id, tenant, key, or consumer name violates the documented bounds. */
export class InvalidNameError extends SyncError {}

/** Two declarations for the same {namespace, kind, id} carry different configurations. */
export class ConflictingResourceDeclarationError extends SyncError {}

/** An existing NATS resource carries the same short name but a different full identity hash. */
export class ResourceIdentityCollisionError extends SyncError {}

/** An existing NATS resource differs from the declared configuration. Sync never mutates it. */
export class ResourceDriftError extends SyncError {
  readonly resource: string;
  readonly differences: ResourceDifference[];

  constructor(resource: string, differences: ResourceDifference[]) {
    const detail = differences
      .map((d) => `${d.field}: declared ${JSON.stringify(d.declared)}, actual ${JSON.stringify(d.actual)}`)
      .join("; ");
    super(`resource ${resource} drifted from its declaration: ${detail}`);
    this.resource = resource;
    this.differences = differences;
  }
}

export type ResourceDifference = {
  field: string;
  declared: unknown;
  actual: unknown;
};

/** The encoded Sync envelope exceeds the primitive's payload limit. */
export class PayloadTooLargeError extends SyncError {
  readonly actualBytes: number;
  readonly maxBytes: number;

  constructor(label: string, actualBytes: number, maxBytes: number) {
    super(`${label} payload is ${actualBytes} bytes, limit is ${maxBytes} bytes`);
    this.actualBytes = actualBytes;
    this.maxBytes = maxBytes;
  }
}

/** A streamed object exceeded the store's maxObjectBytes while being written. */
export class ObjectTooLargeError extends SyncError {
  readonly maxObjectBytes: number;

  constructor(key: string, maxObjectBytes: number) {
    super(`object ${key} exceeds maxObjectBytes (${maxObjectBytes})`);
    this.maxObjectBytes = maxObjectBytes;
  }
}

/**
 * A queue or job declared with `whenFull: "reject"` reached its retention
 * limit (`maxBytes` or `maxMessages`) and refused new work. Nothing was
 * accepted; retry later or shed the work.
 */
export class StoreFullError extends SyncError {}

/** NATS no longer accepts the delivery's ack token (redelivered elsewhere or expired). */
export class StaleDeliveryError extends SyncError {}

/** The requested cursor is older than the retained stream state. */
export class RetentionGapError extends SyncError {
  readonly requested: string;
  /** Cursor of the first event that is still retained. */
  readonly firstAvailable: string;
  /** Pass this as `after` to resume from the first retained event (inclusive). */
  readonly resumeAfter?: string;

  constructor(requested: string, firstAvailable: string, resumeAfter?: string) {
    super(
      `cursor ${requested} is no longer retained; first available is ${firstAvailable}` +
        (resumeAfter !== undefined ? ` — resume with after=${resumeAfter}` : " — re-snapshot and continue from there"),
    );
    this.requested = requested;
    this.firstAvailable = firstAvailable;
    if (resumeAfter !== undefined) this.resumeAfter = resumeAfter;
  }
}

/** A cursor was issued by a different topic resource. */
export class CursorMismatchError extends SyncError {}

/** submitMany failed part-way; prior accepted items remain accepted. */
export class BatchSubmitError extends SyncError {
  readonly accepted: number;
  readonly duplicates: number;

  constructor(message: string, accepted: number, duplicates: number, cause: Error) {
    super(`${message}: ${cause.message} (accepted ${accepted}, duplicates ${duplicates})`);
    this.accepted = accepted;
    this.duplicates = duplicates;
    this.cause = cause;
  }
}

export const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

/** A bounded snapshot exceeded the configured maxEntries. */
export class SnapshotOverflowError extends SyncError {
  readonly maxEntries: number;

  constructor(resource: string, maxEntries: number) {
    super(`${resource} snapshot exceeds maxEntries (${maxEntries}); narrow the tenant/prefix or raise maxEntries`);
    this.maxEntries = maxEntries;
  }
}

/** The API was called in a way its contract forbids (not a name/bounds problem). */
export class SyncUsageError extends SyncError {}

/** A referenced entity (dead letter, schedule, ...) does not exist. */
export class NotFoundError extends SyncError {}

/** An optimistic-concurrency expectation (expectedAfter) was not met. */
export class ConflictError extends SyncError {}
