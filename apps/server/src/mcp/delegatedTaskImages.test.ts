import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { prepareDelegatedTaskImages } from "./delegatedTaskImages.ts";

const layer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-delegated-task-images-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);

describe("prepareDelegatedTaskImages", () => {
  it.effect("copies images into the child thread and releases the copies", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { attachmentsDir } = yield* ServerConfig.ServerConfig;
      const dir = yield* fileSystem.makeTempDirectoryScoped();
      const source = path.join(dir, "Screen Shot.PNG");
      yield* fileSystem.writeFile(source, new Uint8Array([137, 80, 78, 71]));

      const stage = yield* prepareDelegatedTaskImages([source]);
      const staged = yield* stage(ThreadId.make("thread:child"));
      const [attachment] = staged.attachments;
      assert.deepInclude(attachment, {
        type: "image",
        name: "Screen Shot.PNG",
        mimeType: "image/png",
        sizeBytes: 4,
      });
      assert.match(attachment?.id ?? "", /^thread-child-/);
      const stored = resolveAttachmentPath({ attachmentsDir, attachment: attachment! });
      assert.isTrue(yield* fileSystem.exists(stored!));

      yield* staged.release;
      assert.isFalse(yield* fileSystem.exists(stored!));
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect.each([
    { name: "a relative path", file: "screen.png", relative: true },
    { name: "an unsupported type", file: "notes.txt", relative: false },
    { name: "a missing file", file: "missing.png", relative: false },
  ])("refuses $name before anything is copied", ({ file, relative }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fileSystem.makeTempDirectoryScoped();
      if (file !== "missing.png") yield* fileSystem.writeFileString(path.join(dir, file), "x");

      const error = yield* prepareDelegatedTaskImages([
        relative ? file : path.join(dir, file),
      ]).pipe(Effect.flip);
      assert.equal(error.code, "invalid_request");
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
