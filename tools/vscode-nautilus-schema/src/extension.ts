import * as cp from "child_process";
import * as fs from "fs";
import * as https from "https";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

const GITHUB_REPO = "nautilus-env/nautilus";
const BIN_NAME = process.platform === "win32" ? "nautilus-lsp.exe" : "nautilus-lsp";
const NPM_PACKAGE = "nautilus-orm-lsp";
const DOWNLOADED_RELEASE_TAG_KEY = "nautilus.downloadedLspReleaseTag";

interface GitHubReleaseAsset {
  name: string;
  browser_download_url: string;
}

interface GitHubRelease {
  tag_name: string;
  assets: GitHubReleaseAsset[];
}

interface ResolvedReleaseAsset {
  tagName: string;
  downloadUrl: string;
}

let client: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  let serverOptions: ServerOptions;

  try {
    serverOptions = await resolveServerOptions(context);
  } catch (err) {
    vscode.window.showErrorMessage(
      `nautilus-lsp: could not resolve binary - ${err}. ` +
        `Set "nautilus.lspPath" in your settings or add nautilus-lsp to PATH.`
    );
    return;
  }

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "nautilus" }],
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher("**/*.nautilus"),
    },
  };

  client = new LanguageClient(
    "nautilus-lsp",
    "Nautilus LSP",
    serverOptions,
    clientOptions
  );

  client.start();
  context.subscriptions.push(client);
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}

// Path resolution

/**
 * Resolves how to launch `nautilus-lsp`.
 *
 * Search order:
 * 1. `nautilus.lspPath` VS Code setting (user-defined override).
 * 2. Dev build: `<repo-root>/target/debug/nautilus-lsp[.exe]`.
 * 3. Global storage cache (auto-downloaded binary, refreshed from GitHub when a newer release exists).
 * 4. `nautilus-lsp[.exe]` on PATH.
 * 5. Local npm install: `<workspace>/node_modules/.bin/nautilus-lsp`.
 * 6. npm package available via `npx` (global or local install).
 * 7. Auto-download from GitHub Releases -> cache in global storage.
 */
async function resolveServerOptions(
  context: vscode.ExtensionContext
): Promise<ServerOptions> {
  const rawSetting = vscode.workspace
    .getConfiguration("nautilus")
    .get<string>("lspPath");
  if (rawSetting && rawSetting.trim() !== "") {
    const setting = rawSetting.trim().replace(/^~(?=$|\/|\\)/, os.homedir());
    if (fs.existsSync(setting)) {
      return binaryServerOptions(setting);
    }
  }

  const devBuild = path.join(
    context.extensionPath,
    "..",
    "..",
    "target",
    "debug",
    BIN_NAME
  );
  if (fs.existsSync(devBuild)) {
    return binaryServerOptions(devBuild);
  }

  const cachedPath = getCachedBinPath(context);
  if (fs.existsSync(cachedPath)) {
    const refreshedPath = await maybeUpdateCachedLsp(context);
    return binaryServerOptions(refreshedPath);
  }

  if (isOnPath(BIN_NAME)) {
    return binaryServerOptions(BIN_NAME);
  }

  const localBin = findInNodeModules();
  if (localBin) {
    return binaryServerOptions(localBin);
  }

  if (isInstalledVianpm()) {
    return npxServerOptions();
  }

  const downloaded = await downloadLsp(context);
  return binaryServerOptions(downloaded);
}

function binaryServerOptions(binPath: string): ServerOptions {
  return {
    command: binPath,
    transport: TransportKind.stdio,
  };
}

function npxServerOptions(): ServerOptions {
  const npx = process.platform === "win32" ? "npx.cmd" : "npx";
  return {
    command: npx,
    args: [NPM_PACKAGE],
    transport: TransportKind.stdio,
  };
}

function getCachedBinPath(context: vscode.ExtensionContext): string {
  return path.join(context.globalStorageUri.fsPath, BIN_NAME);
}

/** Best-effort check whether a binary exists in any PATH directory. */
function isOnPath(bin: string): boolean {
  const pathEnv = process.env.PATH ?? "";
  const dirs = pathEnv.split(path.delimiter);
  return dirs.some((dir) => fs.existsSync(path.join(dir, bin)));
}

/**
 * Looks for `nautilus-lsp` in `node_modules/.bin` of each open workspace
 * folder. Returns the first match, or `undefined` if none found.
 */
function findInNodeModules(): string | undefined {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders) {
    return undefined;
  }
  for (const folder of folders) {
    const candidate = path.join(
      folder.uri.fsPath,
      "node_modules",
      ".bin",
      BIN_NAME
    );
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

/**
 * Returns `true` when the `nautilus-orm-lsp` npm package is already installed
 * locally or globally and can be invoked without downloading.
 *
 * Uses `npm ls` so no extra binary is fetched; errors are silently ignored.
 */
function isInstalledVianpm(): boolean {
  try {
    const npm = process.platform === "win32" ? "npm.cmd" : "npm";
    // Check local install first, then global (-g).
    for (const extra of [[], ["-g"]]) {
      const result = cp.spawnSync(
        npm,
        ["ls", "--depth=0", "--json", NPM_PACKAGE, ...extra],
        { encoding: "utf8", timeout: 5000 }
      );
      if (result.status === 0 && result.stdout) {
        const data = JSON.parse(result.stdout) as {
          dependencies?: Record<string, unknown>;
        };
        if (data.dependencies && NPM_PACKAGE in data.dependencies) {
          return true;
        }
      }
    }
  } catch {
    // npm not available or parse error - fall through
  }
  return false;
}

// Auto-download

/** Maps Node platform/arch to the Rust target triple used in release artifacts. */
function platformTarget(): string {
  const plat = process.platform;
  const arch = process.arch;

  if (plat === "linux" && arch === "x64") {
    return "x86_64-unknown-linux-gnu";
  }
  if (plat === "darwin" && arch === "x64") {
    return "x86_64-apple-darwin";
  }
  if (plat === "darwin" && arch === "arm64") {
    return "aarch64-apple-darwin";
  }
  if (plat === "win32" && arch === "x64") {
    return "x86_64-pc-windows-msvc";
  }

  throw new Error(`Unsupported platform: ${plat}/${arch}`);
}

function releaseDownloadUrl(target: string): string {
  return `https://github.com/${GITHUB_REPO}/releases/latest/download/${releaseAssetName(
    target
  )}`;
}

function releaseAssetName(target: string): string {
  return process.platform === "win32"
    ? `nautilus-lsp-${target}.exe`
    : `nautilus-lsp-${target}`;
}

async function maybeUpdateCachedLsp(
  context: vscode.ExtensionContext
): Promise<string> {
  const cachedPath = getCachedBinPath(context);
  const currentTag = context.globalState.get<string>(DOWNLOADED_RELEASE_TAG_KEY);

  try {
    const release = await fetchLatestReleaseAsset(platformTarget());
    if (currentTag === release.tagName) {
      return cachedPath;
    }

    const notificationMessage = currentTag
      ? `Found a newer Nautilus LSP version (${currentTag} -> ${release.tagName}). Downloading it now.`
      : `Found a newer Nautilus LSP version (${release.tagName}). Downloading it now.`;
    void vscode.window.showInformationMessage(notificationMessage);

    const startMessage = currentTag
      ? `Updating nautilus-lsp to ${release.tagName}...`
      : "Refreshing cached nautilus-lsp binary...";
    const doneMessage = currentTag
      ? `nautilus-lsp updated to ${release.tagName}.`
      : `Cached nautilus-lsp binary refreshed to ${release.tagName}.`;

    return downloadLspToCache(context, {
      release,
      startMessage,
      doneMessage,
    });
  } catch {
    // Keep using the cached binary when release lookup or download fails.
    return cachedPath;
  }
}

async function downloadLsp(context: vscode.ExtensionContext): Promise<string> {
  return downloadLspToCache(context, {});
}

async function downloadLspToCache(
  context: vscode.ExtensionContext,
  options: {
    release?: ResolvedReleaseAsset;
    startMessage?: string;
    doneMessage?: string;
  }
): Promise<string> {
  const target = platformTarget();
  const dest = getCachedBinPath(context);
  let release = options.release;

  if (!release) {
    try {
      release = await fetchLatestReleaseAsset(target);
    } catch {
      release = undefined;
    }
  }

  const url = release?.downloadUrl ?? releaseDownloadUrl(target);
  const startMessage =
    options.startMessage ?? "Downloading nautilus-lsp binary...";
  const doneMessage =
    options.doneMessage ??
    (release
      ? `nautilus-lsp downloaded (${release.tagName}).`
      : "nautilus-lsp downloaded.");

  ensureDirectory(context.globalStorageUri.fsPath);

  return vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: "Nautilus LSP",
      cancellable: false,
    },
    async (progress) => {
      progress.report({ message: startMessage });
      await downloadBinary(url, dest);
      await context.globalState.update(
        DOWNLOADED_RELEASE_TAG_KEY,
        release?.tagName
      );
      progress.report({ message: doneMessage });
      return dest;
    }
  );
}

async function fetchLatestReleaseAsset(
  target: string
): Promise<ResolvedReleaseAsset> {
  const release = await httpsGetJson<GitHubRelease>(
    `https://api.github.com/repos/${GITHUB_REPO}/releases/latest`
  );
  const assetName = releaseAssetName(target);
  const asset = release.assets.find((candidate) => candidate.name === assetName);

  if (!asset) {
    throw new Error(
      `Latest GitHub release does not include the ${assetName} asset.`
    );
  }

  return {
    tagName: release.tag_name,
    downloadUrl: asset.browser_download_url,
  };
}

function ensureDirectory(dirPath: string): void {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

async function downloadBinary(url: string, dest: string): Promise<void> {
  const tempDest = `${dest}.tmp`;
  removeIfExists(tempDest);

  try {
    await httpsDownload(url, tempDest);
    if (process.platform !== "win32") {
      fs.chmodSync(tempDest, 0o755);
    }
    replaceFile(tempDest, dest);
  } catch (error) {
    removeIfExists(tempDest);
    throw error;
  }
}

function replaceFile(source: string, dest: string): void {
  removeIfExists(dest);
  fs.renameSync(source, dest);
}

function removeIfExists(filePath: string): void {
  if (fs.existsSync(filePath)) {
    fs.rmSync(filePath, { force: true });
  }
}

function httpsGetJson<T>(url: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const follow = (currentUrl: string) => {
      https
        .get(
          currentUrl,
          {
            headers: {
              Accept: "application/vnd.github+json",
              "User-Agent": "nautilus-vscode-extension",
            },
          },
          (res) => {
            if (
              res.statusCode &&
              res.statusCode >= 300 &&
              res.statusCode < 400 &&
              res.headers.location
            ) {
              res.resume();
              follow(new URL(res.headers.location, currentUrl).toString());
              return;
            }

            if (res.statusCode !== 200) {
              res.resume();
              reject(
                new Error(
                  `HTTP ${res.statusCode ?? "?"} requesting ${currentUrl}`
                )
              );
              return;
            }

            let body = "";
            res.setEncoding("utf8");
            res.on("data", (chunk) => {
              body += chunk;
            });
            res.on("end", () => {
              try {
                resolve(JSON.parse(body) as T);
              } catch (error) {
                reject(error);
              }
            });
          }
        )
        .on("error", reject);
    };

    follow(url);
  });
}

/** Downloads `url` (following HTTP redirects) to `dest`. Rejects on HTTP error. */
function httpsDownload(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const follow = (currentUrl: string) => {
      https
        .get(
          currentUrl,
          {
            headers: {
              "User-Agent": "nautilus-vscode-extension",
            },
          },
          (res) => {
            if (
              res.statusCode &&
              res.statusCode >= 300 &&
              res.statusCode < 400 &&
              res.headers.location
            ) {
              res.resume();
              follow(new URL(res.headers.location, currentUrl).toString());
              return;
            }

            if (res.statusCode !== 200) {
              res.resume();
              reject(
                new Error(
                  `HTTP ${res.statusCode ?? "?"} downloading ${currentUrl}`
                )
              );
              return;
            }

            const file = fs.createWriteStream(dest);
            res.pipe(file);
            file.on("finish", () => file.close(() => resolve()));
            file.on("error", (err) => {
              fs.unlink(dest, () => undefined);
              reject(err);
            });
          }
        )
        .on("error", reject);
    };

    follow(url);
  });
}
