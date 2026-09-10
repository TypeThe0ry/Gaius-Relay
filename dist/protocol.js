// The RelayNode only needs a small, versioned view of the Minecraft wire
// protocol. Keep this module local to the bridge image: the Docker build
// intentionally copies apps/bridge/dist without the browser/client packages.
// Packet ids below are the play/configuration ids for the corresponding
// protocol profile; login and configuration packet ids are unchanged between
// 1.21.11 (774) and 26.2 (776).

const commonLoginIds = Object.freeze({
    clientboundDisconnect: 0,
    clientboundEncryptionRequest: 1,
    clientboundLoginFinished: 2,
    clientboundCompression: 3,
    serverboundHello: 0,
    serverboundKey: 1,
    serverboundLoginAcknowledged: 3,
});

const commonConfigurationIds = Object.freeze({
    clientboundDisconnect: 2,
    clientboundFinish: 3,
    clientboundKeepAlive: 4,
    clientboundPing: 5,
    clientboundKnownPacks: 14,
    clientboundResourcePackPush: 9,
    clientboundShowDialog: 18,
    clientboundCodeOfConduct: 19,
    serverboundFinish: 3,
    serverboundKeepAlive: 4,
    serverboundPong: 5,
    serverboundSelectKnownPacks: 7,
    serverboundResourcePack: 6,
    serverboundCustomClickAction: 8,
    serverboundAcceptCodeOfConduct: 9,
});

function profile(name, protocolVersion, play) {
    return Object.freeze({
        name,
        protocolVersion,
        login: commonLoginIds,
        configuration: commonConfigurationIds,
        play: Object.freeze(play),
    });
}

export const MINECRAFT_1_21_11 = profile("1.21.11", 774, {
    clientboundChunkBatchFinished: 11,
    clientboundChunkBatchStart: 12,
    clientboundCustomPayload: 24,
    clientboundDisconnect: 32,
    clientboundKeepAlive: 43,
    clientboundPing: 59,
    clientboundLogin: 48,
    clientboundChunk: 44,
    clientboundSetChunkCacheCenter: 92,
    clientboundSetChunkCacheRadius: 93,
    clientboundSetSimulationDistance: 109,
    clientboundStartConfiguration: 116,
    serverboundCustomPayload: 21,
    serverboundChunkBatchReceived: 10,
    serverboundKeepAlive: 27,
    serverboundPong: 44,
    serverboundPlayerLoaded: 43,
    serverboundClientTickEnd: 12,
    serverboundConfigurationAcknowledged: 15,
});

export const MINECRAFT_26_2 = profile("26.2", 776, {
    clientboundChunkBatchFinished: 11,
    clientboundChunkBatchStart: 12,
    clientboundCustomPayload: 24,
    clientboundDisconnect: 32,
    clientboundKeepAlive: 44,
    clientboundPing: 61,
    clientboundLogin: 49,
    clientboundChunk: 45,
    clientboundSetChunkCacheCenter: 94,
    clientboundSetChunkCacheRadius: 95,
    clientboundSetSimulationDistance: 111,
    clientboundStartConfiguration: 118,
    serverboundCustomPayload: 22,
    serverboundChunkBatchReceived: 11,
    serverboundKeepAlive: 28,
    serverboundPong: 45,
    serverboundPlayerLoaded: 44,
    serverboundClientTickEnd: 13,
    serverboundConfigurationAcknowledged: 16,
});

const byVersion = new Map([
    [MINECRAFT_1_21_11.protocolVersion, MINECRAFT_1_21_11],
    [MINECRAFT_26_2.protocolVersion, MINECRAFT_26_2],
]);

export function resolveMinecraftProfile(protocolVersion) {
    return byVersion.get(protocolVersion);
}

// ClientboundLoginPacket carries the initial view/simulation distances. A
// vanilla server is not required to repeat those values with the later
// set_chunk_cache_radius/set_simulation_distance packets unless they change.
// Keep this prefix parser independent from Buffer so the bridge and smoke
// harnesses can share the exact wire contract.
export function decodeClientboundLoginDistances(payload) {
    if (payload === undefined || payload === null ||
        !Number.isInteger(payload.byteLength) || payload.byteLength < 6) {
        throw new Error("clientbound login payload is truncated");
    }

    // int playerId + boolean hardcore
    let offset = 5;
    const levelCount = decodeVarIntAt(payload, offset, "level count");
    offset = levelCount.nextOffset;
    if (levelCount.value < 0 || levelCount.value > 1024) {
        throw new Error(`clientbound login level count is invalid: ${levelCount.value}`);
    }
    for (let index = 0; index < levelCount.value; index++) {
        const levelLength = decodeVarIntAt(payload, offset, "level name length");
        offset = levelLength.nextOffset;
        if (levelLength.value < 1 || levelLength.value > 32767 ||
            offset + levelLength.value > payload.byteLength) {
            throw new Error("clientbound login level name is invalid");
        }
        offset += levelLength.value;
    }

    const maxPlayers = decodeVarIntAt(payload, offset, "max players");
    offset = maxPlayers.nextOffset;
    const chunkRadius = decodeVarIntAt(payload, offset, "chunk radius");
    offset = chunkRadius.nextOffset;
    const simulationDistance = decodeVarIntAt(payload, offset, "simulation distance");
    if (chunkRadius.value < 2 || chunkRadius.value > 32 ||
        simulationDistance.value < 2 || simulationDistance.value > 32) {
        throw new Error(
            `clientbound login distance contract is invalid: ` +
            `${chunkRadius.value}/${simulationDistance.value}`,
        );
    }
    return Object.freeze({
        chunkRadius: chunkRadius.value,
        simulationDistance: simulationDistance.value,
        prefixBytesRead: simulationDistance.nextOffset,
    });
}

function decodeVarIntAt(bytes, offset, label) {
    let value = 0;
    for (let index = 0; index < 5; index++) {
        const position = offset + index;
        if (position >= bytes.byteLength) {
            throw new Error(`clientbound login ${label} is truncated`);
        }
        const next = bytes[position];
        value |= (next & 0x7f) << (index * 7);
        if ((next & 0x80) === 0) {
            return { value: value | 0, nextOffset: position + 1 };
        }
    }
    throw new Error(`clientbound login ${label} exceeded five bytes`);
}
