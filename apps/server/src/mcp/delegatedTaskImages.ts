import {
  ChatAttachmentId,
  OrchestratorMcpFailure,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  type ChatAttachment,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { createAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import {
  releaseClaimedAttachments,
  validateAttachmentLimits,
} from "../orchestration-v2/AttachmentClaims.ts";

const IMAGE_MIME_TYPE_BY_EXTENSION: Readonly<Record<string, string>> = {
  ".gif": "image/gif",
  ".jpeg": "image/jpeg",
  ".jpg": "image/jpeg",
  ".png": "image/png",
  ".webp": "image/webp",
};

export interface StagedDelegatedTaskImages {
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly release: Effect.Effect<void>;
}

/** Copies the images into the child thread's attachments once its id is known. */
export type DelegatedTaskImages = (
  childThreadId: ThreadId,
) => Effect.Effect<StagedDelegatedTaskImages, OrchestratorMcpFailure>;

const invalid = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });

/** Validates local image paths up front, so a bad path fails before the task exists. */
export const prepareDelegatedTaskImages = Effect.fn("DelegatedTaskImages.prepare")(function* (
  paths: ReadonlyArray<string>,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { attachmentsDir } = yield* ServerConfig.ServerConfig;

  const images = yield* Effect.forEach(paths, (source) =>
    Effect.gen(function* () {
      if (!path.isAbsolute(source)) {
        return yield* invalid(`Image path must be absolute: ${source}`);
      }
      const extension = path.extname(source).toLowerCase();
      const mimeType = IMAGE_MIME_TYPE_BY_EXTENSION[extension];
      if (mimeType === undefined) {
        return yield* invalid(`Unsupported image type: ${source}. Use PNG, JPEG, GIF or WebP.`);
      }
      const info = yield* fileSystem
        .stat(source)
        .pipe(Effect.mapError(() => invalid(`Image not found: ${source}`)));
      const sizeBytes = Number(info.size);
      if (
        info.type !== "File" ||
        sizeBytes === 0 ||
        sizeBytes > PROVIDER_SEND_TURN_MAX_IMAGE_BYTES
      ) {
        return yield* invalid(`Image must be a non-empty file of at most 10 MiB: ${source}`);
      }
      return { source, extension, mimeType, sizeBytes, name: path.basename(source) };
    }),
  );

  const stage: DelegatedTaskImages = (childThreadId) => {
    const copied: Array<string> = [];
    const release = releaseClaimedAttachments(copied).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
    );
    return Effect.forEach(
      images,
      (image) =>
        Effect.gen(function* () {
          const id = createAttachmentId(childThreadId, image.extension);
          const attachment: ChatAttachment | null =
            id === null
              ? null
              : {
                  type: "image",
                  id: ChatAttachmentId.make(id),
                  name: image.name,
                  mimeType: image.mimeType,
                  sizeBytes: image.sizeBytes,
                };
          const destination =
            attachment === null ? null : resolveAttachmentPath({ attachmentsDir, attachment });
          if (attachment === null || destination === null) {
            return yield* invalid(`Unable to store image ${image.name} for the task.`);
          }
          yield* fileSystem.makeDirectory(path.dirname(destination), { recursive: true }).pipe(
            Effect.andThen(fileSystem.copyFile(image.source, destination)),
            Effect.mapError(() => invalid(`Unable to copy image ${image.source}.`)),
            Effect.andThen(Effect.sync(() => copied.push(destination))),
            Effect.uninterruptible,
          );
          return attachment;
        }),
      { concurrency: 1 },
    ).pipe(
      Effect.tap((attachments) =>
        validateAttachmentLimits(attachments).pipe(
          Effect.mapError((error) => invalid(error.message)),
        ),
      ),
      Effect.map((attachments) => ({ attachments, release })),
      Effect.onError(() => release),
    );
  };
  return stage;
});

export type DelegatedTaskImagePreparer = (
  paths: ReadonlyArray<string>,
) => Effect.Effect<DelegatedTaskImages, OrchestratorMcpFailure>;

/** Reads the file services when the toolkit layer is built, so `delegate_task` declares no new dependencies. */
export const delegatedTaskImagePreparer = Effect.gen(function* () {
  const fileSystem = yield* Effect.serviceOption(FileSystem.FileSystem);
  const path = yield* Effect.serviceOption(Path.Path);
  const config = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const prepare: DelegatedTaskImagePreparer = (paths) =>
    Option.isSome(fileSystem) && Option.isSome(path) && Option.isSome(config)
      ? prepareDelegatedTaskImages(paths).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem.value),
          Effect.provideService(Path.Path, path.value),
          Effect.provideService(ServerConfig.ServerConfig, config.value),
        )
      : Effect.fail(
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "This server cannot attach images to tasks.",
          }),
        );
  return prepare;
});
