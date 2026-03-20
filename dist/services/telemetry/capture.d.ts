/**
 * List of telemetry events for typesafety.
 *
 * - prefix with `SDK` to avoid collisions with other apps
 * - use past tense to indicate that the event is completed
 */
type TelemetryEvent = "LIX-SDK lix opened";
/**
 * Capture an event.
 *
 * - manually calling the PostHog API because the SDKs were not platform angostic (and generally bloated)
 */
export declare const capture: (event: TelemetryEvent, args: {
    lixId: string;
    accountId: string;
    /**
     * The value of the telemetry key-value pair.
     *
     * @example
     *   const telemetryKeyValue = await lix.db.selectFrom("key_value").select("value").where("key", "=", "lix_telemetry").executeTakeFirstOrThrow();
     *   await capture("LIX-SDK opened lix", { lixId: "test", accountId: "test", telemetryKeyValue: telemetryKeyValue.value, properties: {} });
     */
    telemetryKeyValue: string;
    /**
     * Please use snake_case for property names.
     */
    properties: Record<string, any>;
}) => Promise<void>;
export {};
//# sourceMappingURL=capture.d.ts.map