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
    clientboundCustomPayload: 24,
    clientboundDisconnect: 32,
    clientboundKeepAlive: 43,
    clientboundPing: 59,
    clientboundLogin: 48,
    clientboundChunk: 44,
    clientboundStartConfiguration: 116,
    serverboundCustomPayload: 21,
    serverboundKeepAlive: 27,
    serverboundPong: 44,
    serverboundPlayerLoaded: 43,
    serverboundClientTickEnd: 12,
    serverboundConfigurationAcknowledged: 15,
});

export const MINECRAFT_26_2 = profile("26.2", 776, {
    clientboundCustomPayload: 24,
    clientboundDisconnect: 32,
    clientboundKeepAlive: 44,
    clientboundPing: 61,
    clientboundLogin: 49,
    clientboundChunk: 45,
    clientboundStartConfiguration: 118,
    serverboundCustomPayload: 22,
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
