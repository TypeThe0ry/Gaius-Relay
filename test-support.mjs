import {randomBytes} from "node:crypto";

export function ephemeralCredential(label) {
    return `${label}-${randomBytes(16).toString("hex")}`;
}
