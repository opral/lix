import { ENV_VARIABLES } from "../env-variables/index.js";
/**
 * Capture an event.
 *
 * - manually calling the PostHog API because the SDKs were not platform angostic (and generally bloated)
 */
export const capture = async (event, args) => {
    if (ENV_VARIABLES.LIX_SDK_POSTHOG_TOKEN === undefined) {
        return;
    }
    else if (args.telemetryKeyValue === "off") {
        return;
    }
    try {
        await fetch("https://eu.posthog.com/capture/", {
            method: "POST",
            body: JSON.stringify({
                api_key: ENV_VARIABLES.LIX_SDK_POSTHOG_TOKEN,
                event,
                distinct_id: args.accountId,
                properties: {
                    $groups: { lix_id: args.lixId },
                    ...args.properties,
                },
            }),
        });
        await identifyLix({
            lixId: args.lixId,
            accountId: args.accountId,
            // using the id for now as a name but can be changed in the future
            // we need at least one property to make a project visible in the dashboard
            properties: { name: args.lixId },
        });
    }
    catch {
        //
    }
};
/**
 * Identifying a project is needed.
 *
 * Otherwise, the project will not be visible in the PostHog dashboard.
 */
const identifyLix = async (args) => {
    // do not send events if the token is not set
    // (assuming this eases testing)
    if (ENV_VARIABLES.LIX_SDK_POSTHOG_TOKEN === undefined) {
        return;
    }
    try {
        await fetch("https://eu.posthog.com/capture/", {
            method: "POST",
            body: JSON.stringify({
                api_key: ENV_VARIABLES.LIX_SDK_POSTHOG_TOKEN,
                event: "$groupidentify",
                distinct_id: args.accountId,
                properties: {
                    $group_type: "lix",
                    $group_key: args.lixId,
                    $group_set: {
                        ...args.properties,
                    },
                },
            }),
        });
    }
    catch {
        //
    }
};
//# sourceMappingURL=capture.js.map