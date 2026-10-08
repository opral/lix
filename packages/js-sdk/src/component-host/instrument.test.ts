import { instrumentCore } from "./instrument.js";
import { registerCoreInstrumentationTests } from "./instrument.suite.js";

// Preserve the Node baseline while sharing all adversarial cases with browser tests.
registerCoreInstrumentationTests(instrumentCore);
