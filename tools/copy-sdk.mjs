import { mkdir, copyFile } from "node:fs/promises";
await mkdir("target/dx-ts/sdk/typescript", {recursive:true});
await copyFile("sdk/typescript/client.mjs", "target/dx-ts/sdk/typescript/client.mjs");
