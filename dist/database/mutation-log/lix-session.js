import { v4 as uuid_v4 } from "uuid";
export function createSession() {
    const sessionId = uuid_v4(); // we use v4 here since the order of session should explicetly not matter!
    let time = 0;
    return {
        /**
         * Returns the current lix session id.
         */
        id: () => {
            return sessionId;
        },
        sessionClockTick: () => {
            time += 1;
            return time;
        },
    };
}
//# sourceMappingURL=lix-session.js.map