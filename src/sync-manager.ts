/**
 * Sync Manager
 *
 * Orchestrates syncing from reMarkable cloud to local filesystem.
 * Handles incremental sync, folder structure, and file management.
 * Obsidian-independent via abstracted file I/O.
 */

import {
	RemarkableCloudClient,
	type DocumentMetadata,
	type FileOps,
	buildFolderTree,
	isDocument,
} from "./cloud-client";
import { convertDocument } from "./document-converter";
import { DEFAULT_PDF_FOLDER, SYNC_LOG_FILENAME, SYNC_LOG_MAX_BYTES } from "./constants";

// --- Sync state ---

export interface SyncedDocInfo {
	/** Sub-file count at last sync (the root index has no real revision number). */
	version: number;
	/** Vault-relative path of the PDF. */
	path: string;
	/** Vault-relative path of the note embedding the PDF. Absent in state files from before notes. */
	notePath?: string;
	hash: string;
	syncedAt: string;
	/** Root-index entry hash; changes on any edit. Absent in state files from before entry-hash tracking. */
	entryHash?: string;
}

export class SyncState {
	lastSync: string | null = null;
	syncedDocs: Record<string, SyncedDocInfo> = {};

	static async load(stateFile: string, fileOps: FileOps): Promise<SyncState> {
		const state = new SyncState();
		try {
			const data = await fileOps.readFile(stateFile);
			if (!data) return state;
			const parsed = JSON.parse(data);
			state.lastSync = parsed.last_sync ?? null;
			const docs = parsed.synced_docs;
			// Guard against hand-edited/corrupt state: anything but a plain object resets.
			state.syncedDocs =
				docs && typeof docs === "object" && !Array.isArray(docs) ? docs : {};
		} catch {
			// No state file or invalid JSON
		}
		return state;
	}

	async save(stateFile: string, fileOps: FileOps): Promise<void> {
		const data = JSON.stringify(
			{
				last_sync: this.lastSync,
				synced_docs: this.syncedDocs,
			},
			null,
			2
		);
		await fileOps.writeFile(stateFile, data);
	}

	needsSync(doc: DocumentMetadata): boolean {
		const synced = this.syncedDocs[doc.id];
		if (!synced) return true;
		// The entry hash is authoritative when present: it changes on any edit,
		// while `version` is the sub-file count and misses in-place edits.
		if (doc.entryHash) return synced.entryHash !== doc.entryHash;
		return (synced.version ?? 0) < doc.version;
	}
}

// --- Sync results ---

export interface SyncErrorDetail {
	docId: string;
	path: string;
	message: string;
}

export interface SyncResults {
	synced: string[];
	skipped: string[];
	errors: string[];
	/** Structured per-document failures (richer than the `errors` strings). */
	errorDetails: SyncErrorDetail[];
	/** Timestamped activity lines captured during the run. */
	log: string[];
	startedAt: string;
	finishedAt: string;
	durationMs: number;
	/** Vault-relative path of the log file written for this run, if any. */
	logPath: string | null;
}

/**
 * Split a comma-separated list of reMarkable folder paths (e.g. "Archive,
 * Work/Old") into normalized entries, dropping empty ones.
 */
export function parseFolderList(value: string): string[] {
	return value
		.split(",")
		.map(normalizeFolderPath)
		.filter((f) => f !== "");
}

function normalizeFolderPath(folder: string): string {
	return folder.trim().replace(/^\/+|\/+$/g, "");
}

/**
 * Whether a document path lies inside one of the excluded folders. Matches
 * whole path segments, so excluding "Work" does not exclude "Workouts".
 */
export function isInExcludedFolder(docPath: string, excluded: string[]): boolean {
	return excluded.some((folder) => docPath.startsWith(`${folder}/`));
}

interface DocPaths {
	pdf: string;
	note: string;
}

function noteEmbed(pdfVaultPath: string): string {
	return `![[${pdfVaultPath}]]`;
}

export type ProgressCallback = (message: string) => void;

// --- Sync manager ---

export interface SyncOptions {
	folderFilter?: string;
	/** reMarkable folder paths whose documents (including subfolders) are skipped. */
	excludedFolders?: string[];
	/** Folder inside the sync folder that holds the PDFs (default: DEFAULT_PDF_FOLDER). */
	pdfFolder?: string;
	force?: boolean;
	dryRun?: boolean;
	subfolder?: string;
	onProgress?: ProgressCallback;
	/** Write a human-readable log file into the sync folder (default: true). */
	writeLog?: boolean;
	/** Override the log filename (default: SYNC_LOG_FILENAME). */
	logFileName?: string;
}

export class SyncManager {
	private outputDir: string;
	private stateFile: string;
	private fileOps: FileOps;
	private state: SyncState;
	private vaultPath: string;

	constructor(
		vaultPath: string,
		subfolder: string,
		fileOps: FileOps,
		state: SyncState
	) {
		this.vaultPath = vaultPath;
		this.outputDir = joinPath(vaultPath, subfolder);
		this.stateFile = joinPath(this.outputDir, ".remarkable-sync-state.json");
		this.fileOps = fileOps;
		this.state = state;
	}

	static async create(
		vaultPath: string,
		subfolder: string,
		fileOps: FileOps
	): Promise<SyncManager> {
		const outputDir = joinPath(vaultPath, subfolder);
		const stateFile = joinPath(outputDir, ".remarkable-sync-state.json");
		const state = await SyncState.load(stateFile, fileOps);
		return new SyncManager(vaultPath, subfolder, fileOps, state);
	}

	async sync(
		client: RemarkableCloudClient,
		opts: SyncOptions = {}
	): Promise<SyncResults> {
		const startMs = Date.now();
		const results: SyncResults = {
			synced: [],
			skipped: [],
			errors: [],
			errorDetails: [],
			log: [],
			startedAt: new Date().toISOString(),
			finishedAt: "",
			durationMs: 0,
			logPath: null,
		};
		const writeLog = opts.writeLog ?? true;
		const logFileName = opts.logFileName ?? SYNC_LOG_FILENAME;

		// Capture every progress line into the run log, then forward to the caller.
		const userProgress = opts.onProgress ?? (() => {});
		const progress = (message: string) => {
			results.log.push(`[${new Date().toISOString()}] ${message}`);
			userProgress(message);
		};

		const finalize = async (): Promise<void> => {
			results.finishedAt = new Date().toISOString();
			results.durationMs = Date.now() - startMs;
			if (writeLog) {
				try {
					results.logPath = await this.writeRunLog(results, logFileName);
				} catch (e) {
					// Never let logging failures break a sync.
					userProgress(`(could not write sync log: ${(e as Error).message})`);
				}
			}
		};

		if (!client.isAuthenticated) {
			const message =
				"Not authenticated. Please register with reMarkable first.";
			progress(`[FAIL] ${message}`);
			await finalize();
			throw new Error(message);
		}

		progress("Fetching document list from reMarkable cloud...");
		let documents: DocumentMetadata[];
		try {
			documents = await client.listDocuments();
		} catch (e) {
			const message = (e as Error).message;
			progress(`[FAIL] Could not list documents: ${message}`);
			await finalize();
			throw e;
		}
		const folderPaths = buildFolderTree(documents);

		// Filter to documents only
		const docsToSync = documents.filter(
			(doc) => isDocument(doc) && !doc.isTrashed
		);

		// Apply folder filter
		const filtered = opts.folderFilter
			? docsToSync.filter((doc) =>
					(folderPaths.get(doc.id) ?? "").startsWith(opts.folderFilter!)
				)
			: docsToSync;

		const excluded = (opts.excludedFolders ?? [])
			.map(normalizeFolderPath)
			.filter((f) => f !== "");
		const included = excluded.length
			? filtered.filter(
					(doc) => !isInExcludedFolder(folderPaths.get(doc.id) ?? doc.name, excluded)
				)
			: filtered;

		progress(`Found ${included.length} documents to check`);

		const pdfFolder = normalizeFolderPath(opts.pdfFolder ?? "") || DEFAULT_PDF_FOLDER;

		for (const doc of included) {
			const docPath = folderPaths.get(doc.id) ?? doc.name;
			const paths = this.docPaths(docPath, pdfFolder);

			// A changed PDF folder setting or a state file from before notes moves
			// files even when the document itself is unchanged.
			const previous = this.state.syncedDocs[doc.id];
			const relocated =
				previous !== undefined &&
				(previous.path !== this.toVaultRelative(paths.pdf) ||
					previous.notePath !== this.toVaultRelative(paths.note));

			if (!opts.force && !relocated && !this.state.needsSync(doc)) {
				results.skipped.push(docPath);
				continue;
			}

			if (opts.dryRun) {
				progress(`[dry-run] Would sync: ${docPath}`);
				results.synced.push(docPath);
				continue;
			}

			try {
				await this.syncDocument(client, doc, docPath, paths, progress);
				results.synced.push(docPath);
				progress(`[OK] Synced: ${docPath}`);
			} catch (e) {
				const message = (e as Error).message;
				results.errors.push(`${docPath}: ${message}`);
				results.errorDetails.push({
					docId: doc.id,
					path: docPath,
					message,
				});
				progress(`[FAIL] Error: ${docPath}: ${message}`);
			}
		}

		if (!opts.dryRun) {
			this.state.lastSync = new Date().toISOString();
			try {
				await this.state.save(this.stateFile, this.fileOps);
			} catch (e) {
				// Per-doc saves already persisted progress; still finalize and log.
				progress(`[FAIL] Could not save sync state: ${(e as Error).message}`);
			}
		}

		progress(
			`Sync finished — ${results.synced.length} synced, ` +
				`${results.skipped.length} skipped, ${results.errors.length} errors`
		);

		await finalize();

		return results;
	}

	/**
	 * Write a human-readable Markdown log of the latest run into the sync folder.
	 * Keeps a capped history of previous runs so users can troubleshoot failures.
	 * Returns the vault-relative path of the log file.
	 */
	private async writeRunLog(
		results: SyncResults,
		fileName: string
	): Promise<string> {
		const logFilePath = joinPath(this.outputDir, fileName);

		let previous = "";
		try {
			previous = (await this.fileOps.readFile(logFilePath)) ?? "";
		} catch {
			previous = "";
		}
		previous = stripLogHeader(previous).trim();

		const section = formatRunSection(results);
		let body = previous ? section + "\n\n" + previous : section;
		if (body.length > SYNC_LOG_MAX_BYTES) {
			body =
				body.slice(0, SYNC_LOG_MAX_BYTES) +
				"\n\n_…older log entries truncated…_\n";
		}

		const content = LOG_HEADER + body + "\n";

		if (this.outputDir) await this.fileOps.mkdir(this.outputDir);
		await this.fileOps.writeFile(logFilePath, content);

		return logFilePath.startsWith(this.vaultPath + "/")
			? logFilePath.substring(this.vaultPath.length + 1)
			: logFilePath;
	}

	private async syncDocument(
		client: RemarkableCloudClient,
		doc: DocumentMetadata,
		docPath: string,
		paths: DocPaths,
		progress: ProgressCallback
	): Promise<void> {
		progress(`Downloading: ${docPath}...`);
		const zipData = await client.downloadDocument(doc.id);

		progress(`Converting: ${docPath}...`);
		const pdfData = await convertDocument(doc.id, zipData);

		await this.ensureParentDir(paths.pdf);

		const hash = simpleHash(pdfData);
		const pdfRel = this.toVaultRelative(paths.pdf);
		const noteRel = this.toVaultRelative(paths.note);
		const previous = this.state.syncedDocs[doc.id];

		// Metadata-only changes re-render the same bytes; skip the write to spare
		// vault watchers (Obsidian Sync, backups) a spurious mtime bump.
		const unchanged =
			previous?.hash === hash &&
			previous?.path === pdfRel &&
			(await this.fileOps.exists(paths.pdf));
		if (!unchanged) {
			await this.fileOps.writeBinaryFile(paths.pdf, pdfData);
		}

		// A rename/move re-syncs to a new path; drop the stale PDF at the old one.
		if (previous && previous.path !== pdfRel) {
			const oldPath = joinPath(this.vaultPath, previous.path);
			try {
				if (await this.fileOps.exists(oldPath)) {
					await this.fileOps.deleteFile(oldPath);
					progress(`Removed old copy: ${previous.path}`);
				}
			} catch (e) {
				progress(`(could not remove old copy ${previous.path}: ${(e as Error).message})`);
			}
		}

		await this.writeNote(previous, paths.note, noteRel, pdfRel, progress);

		this.state.syncedDocs[doc.id] = {
			version: doc.version,
			path: pdfRel,
			notePath: noteRel,
			hash,
			syncedAt: new Date().toISOString(),
			entryHash: doc.entryHash,
		};
		await this.state.save(this.stateFile, this.fileOps);
	}

	/**
	 * Create the note embedding the PDF. The note belongs to the user after
	 * that: it is never overwritten, only moved along with the document and its
	 * embed repointed when the PDF moves.
	 */
	private async writeNote(
		previous: SyncedDocInfo | undefined,
		notePath: string,
		noteRel: string,
		pdfRel: string,
		progress: ProgressCallback
	): Promise<void> {
		const repoint = (content: string): string =>
			previous ? content.split(noteEmbed(previous.path)).join(noteEmbed(pdfRel)) : content;

		if (await this.fileOps.exists(notePath)) {
			if (previous && previous.path !== pdfRel) {
				const content = (await this.fileOps.readFile(notePath)) ?? "";
				const updated = repoint(content);
				if (updated !== content) await this.fileOps.writeFile(notePath, updated);
			}
			return;
		}

		await this.ensureParentDir(notePath);

		const oldNote =
			previous?.notePath && previous.notePath !== noteRel
				? joinPath(this.vaultPath, previous.notePath)
				: null;
		if (oldNote && (await this.fileOps.exists(oldNote))) {
			const content = (await this.fileOps.readFile(oldNote)) ?? "";
			await this.fileOps.writeFile(notePath, repoint(content));
			await this.fileOps.deleteFile(oldNote);
			progress(`Moved note: ${previous!.notePath} -> ${noteRel}`);
			return;
		}

		await this.fileOps.writeFile(notePath, noteEmbed(pdfRel) + "\n");
	}

	/** Where a document's note and PDF live, mirroring its reMarkable folder path. */
	private docPaths(docPath: string, pdfFolder: string): DocPaths {
		// Windows-invalid characters, plus the ones that break a wikilink embed.
		const safePath = docPath.replace(/[<>:"|?*\\#^[\]]/g, "_");
		return {
			pdf: joinPath(this.outputDir, joinPath(pdfFolder, safePath + ".pdf")),
			note: joinPath(this.outputDir, safePath + ".md"),
		};
	}

	private async ensureParentDir(filePath: string): Promise<void> {
		const parentDir = filePath.substring(0, filePath.lastIndexOf("/"));
		if (parentDir) await this.fileOps.mkdir(parentDir);
	}

	/** Strip the vault prefix so state paths stay portable across vault locations. */
	private toVaultRelative(p: string): string {
		return p.startsWith(this.vaultPath + "/")
			? p.substring(this.vaultPath.length + 1)
			: p;
	}

	async listRemote(
		client: RemarkableCloudClient
	): Promise<
		{ id: string; name: string; path: string; version: number; modified: string; synced: boolean }[]
	> {
		const documents = await client.listDocuments();
		const folderPaths = buildFolderTree(documents);
		const result: { id: string; name: string; path: string; version: number; modified: string; synced: boolean }[] = [];

		for (const doc of documents) {
			if (isDocument(doc) && !doc.isTrashed) {
				result.push({
					id: doc.id,
					name: doc.name,
					path: folderPaths.get(doc.id) ?? doc.name,
					version: doc.version,
					modified: doc.modifiedTime,
					synced: !this.state.needsSync(doc),
				});
			}
		}
		return result;
	}

	get lastSyncTime(): string | null {
		return this.state.lastSync;
	}

	get isAuthenticated(): boolean {
		return false; // Caller should check client.isAuthenticated
	}
}

// --- Log formatting ---

const LOG_HEADER =
	"# reMarkable Sync Log\n\n" +
	"_Auto-generated by the reMarkable Sync plugin. Most recent run first._\n\n";

/** Remove the standard header so previous runs can be re-appended cleanly. */
function stripLogHeader(content: string): string {
	if (content.startsWith(LOG_HEADER)) {
		return content.slice(LOG_HEADER.length);
	}
	// Fall back to dropping the first heading line if present.
	const idx = content.indexOf("## ");
	return idx >= 0 ? content.slice(idx) : content;
}

/** Build the Markdown section for a single sync run. */
function formatRunSection(results: SyncResults): string {
	const seconds = (results.durationMs / 1000).toFixed(1);
	const lines: string[] = [];

	lines.push(`## Sync ${results.startedAt}`);
	lines.push("");
	lines.push(
		`- **Result:** ${results.synced.length} synced, ` +
			`${results.skipped.length} skipped, ${results.errors.length} errors`
	);
	lines.push(`- **Duration:** ${seconds}s`);
	lines.push("");

	if (results.errorDetails.length > 0) {
		lines.push(`### Errors (${results.errorDetails.length})`);
		lines.push("");
		for (const err of results.errorDetails) {
			lines.push(`- \`${err.path}\` — ${err.message}`);
		}
		lines.push("");
	}

	if (results.synced.length > 0) {
		lines.push(`### Synced (${results.synced.length})`);
		lines.push("");
		for (const path of results.synced) {
			lines.push(`- \`${path}\``);
		}
		lines.push("");
	}

	lines.push("<details>");
	lines.push(`<summary>Activity (${results.log.length} lines)</summary>`);
	lines.push("");
	lines.push("```");
	for (const line of results.log) {
		lines.push(line);
	}
	lines.push("```");
	lines.push("");
	lines.push("</details>");
	lines.push("");

	return lines.join("\n");
}

// --- Utilities ---

// Join a base directory and a child path. The base may be empty — used when
// the file ops are vault-relative (Obsidian) — in which case the child is
// returned as-is, avoiding a spurious leading "/". A non-empty base (e.g. the
// CLI's absolute output dir) is joined with a single separator.
function joinPath(base: string, child: string): string {
	if (!base) return child;
	if (!child) return base;
	return base.replace(/\/+$/, "") + "/" + child;
}

function simpleHash(data: Uint8Array): string {
	// Simple FNV-1a hash as a hex string (not cryptographic, just for change detection)
	let hash = 0x811c9dc5;
	for (let i = 0; i < data.length; i++) {
		hash ^= data[i];
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0).toString(16).padStart(8, "0");
}
