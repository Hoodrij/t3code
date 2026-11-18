import type { ModelCapabilities, ProviderDriverKind } from "@t3tools/contracts";

const FAST_MODE_DESCRIPTOR = { id: "fastMode", label: "Fast Mode", type: "boolean" } as const;

/**
 * Fork: Claude Code runs Fable in fast mode, but the upstream manifest declares it only for Opus.
 * Applied after decode so the upstream manifest refresh cannot drop it.
 */
export function withForkCapabilities(
  driverKind: ProviderDriverKind,
  profileId: string | undefined,
  capabilities: ModelCapabilities | null,
): ModelCapabilities | null {
  if (driverKind !== "claudeAgent" || capabilities === null || !profileId?.startsWith("fable")) {
    return capabilities;
  }
  const descriptors = capabilities.optionDescriptors ?? [];
  if (descriptors.some((descriptor) => descriptor.id === FAST_MODE_DESCRIPTOR.id)) {
    return capabilities;
  }
  return { ...capabilities, optionDescriptors: [...descriptors, FAST_MODE_DESCRIPTOR] };
}
