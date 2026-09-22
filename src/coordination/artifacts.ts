import { createHash, randomUUID } from "node:crypto";
import {
	closeSync,
	constants,
	fstatSync,
	fsyncSync,
	linkSync,
	lstatSync,
	mkdirSync,
	openSync,
	readSync,
	realpathSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { join, resolve } from "node:path";
import { CoordinationPolicyError } from "./policy.ts";
import type {
	ArtifactRef,
	ArtifactStateRecord,
	CoordinationActor,
	CoordinationProjection,
} from "./protocol.ts";

export const MAX_ARTIFACT_BYTES = 64 * 1024 * 1024;
export function sha256Bytes(bytes: Buffer | string): string {
	return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}
function tampered(message: string): never {
	throw new CoordinationPolicyError("ARTIFACT_TAMPERED", message);
}

/** Read through a non-following descriptor, with a fixed allocation and stable inode. */
function readStableFile(path: string): Buffer {
	let fd = -1;
	try {
		const pathStat = lstatSync(path);
		if (!pathStat.isFile() || pathStat.isSymbolicLink())
			tampered("Artifact must be a regular, non-symlink file.");
		fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
		const before = fstatSync(fd);
		if (!before.isFile() || before.ino !== pathStat.ino || before.dev !== pathStat.dev)
			tampered("Artifact source changed before opening.");
		if (before.size > MAX_ARTIFACT_BYTES)
			throw new CoordinationPolicyError(
				"BOUNDS_EXCEEDED",
				`Artifacts are limited to ${MAX_ARTIFACT_BYTES} bytes.`,
			);
		const bytes = Buffer.alloc(before.size);
		let offset = 0;
		while (offset < bytes.length) {
			const count = readSync(fd, bytes, offset, bytes.length - offset, offset);
			if (!count) tampered("Artifact shrank while being read.");
			offset += count;
		}
		const extra = Buffer.alloc(1);
		if (readSync(fd, extra, 0, 1, offset)) tampered("Artifact grew while being read.");
		const after = fstatSync(fd);
		const current = lstatSync(path);
		if (
			before.size !== after.size ||
			before.mtimeMs !== after.mtimeMs ||
			before.ctimeMs !== after.ctimeMs ||
			current.ino !== after.ino ||
			current.dev !== after.dev ||
			current.isSymbolicLink()
		)
			tampered("Artifact changed while being read.");
		return bytes;
	} catch (error) {
		if (error instanceof CoordinationPolicyError) throw error;
		return tampered(
			`Artifact could not be verified: ${(error as NodeJS.ErrnoException).code ?? "I/O failure"}.`,
		);
	} finally {
		if (fd >= 0) closeSync(fd);
	}
}

/** Publish a flushed, read-only file with an atomic, no-clobber hard link. */
export function sealContentAddressedArtifact(params: {
	projection: CoordinationProjection;
	actor: CoordinationActor;
	artifactId: string;
	sourcePath: string;
	storeDir: string;
	privateTo?: string[];
}): ArtifactStateRecord {
	const bytes = readStableFile(resolve(params.sourcePath));
	const digest = sha256Bytes(bytes);
	mkdirSync(params.storeDir, { recursive: true, mode: 0o700 });
	const requestedStore = resolve(params.storeDir);
	if (lstatSync(requestedStore).isSymbolicLink()) tampered("Artifact store must not be a symlink.");
	const store = realpathSync(requestedStore);
	const directory = lstatSync(store);
	if (!directory.isDirectory()) tampered("Artifact store must be a directory.");
	const finalPath = join(store, digest.slice(7));
	const temporary = join(store, `.tmp-${randomUUID()}`);
	let fd = -1;
	try {
		fd = openSync(
			temporary,
			constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_NOFOLLOW,
			0o400,
		);
		writeFileSync(fd, bytes);
		fsyncSync(fd);
		closeSync(fd);
		fd = -1;
		if (sha256Bytes(readStableFile(temporary)) !== digest)
			tampered("Staged artifact digest changed.");
		try {
			linkSync(temporary, finalPath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
		}
		if (sha256Bytes(readStableFile(finalPath)) !== digest)
			tampered("Content-addressed artifact destination was tampered with.");
		const current = lstatSync(store);
		if (
			current.isSymbolicLink() ||
			current.ino !== directory.ino ||
			current.dev !== directory.dev ||
			realpathSync(requestedStore) !== store
		)
			tampered("Artifact store directory changed while sealing.");
	} finally {
		if (fd >= 0) closeSync(fd);
		rmSync(temporary, { force: true });
	}
	const versions = params.projection.artifacts[params.artifactId] ?? [];
	return {
		artifactId: params.artifactId,
		version: (versions.at(-1)?.version ?? 0) + 1,
		digest,
		bytes: bytes.length,
		path: finalPath,
		state: "sealed",
		producer: params.actor,
		privateTo: params.privateTo,
		consumers: [],
	};
}

export function freezeArtifact(artifact: ArtifactStateRecord): ArtifactStateRecord {
	if (artifact.state !== "sealed")
		throw new CoordinationPolicyError("INVALID_TRANSITION", "Only sealed artifacts can be frozen.");
	verifyArtifactBytes(artifact);
	return { ...artifact, state: "frozen" };
}

export function verifyArtifactBytes(artifact: ArtifactStateRecord): ArtifactRef {
	if (!artifact.path) tampered("Artifact has no sealed path.");
	const bytes = readStableFile(artifact.path);
	if (bytes.length !== artifact.bytes || sha256Bytes(bytes) !== artifact.digest)
		tampered("Consume-time artifact verification detected tampering.");
	return { artifactId: artifact.artifactId, version: artifact.version, digest: artifact.digest };
}

export function supersedeArtifact(
	previous: ArtifactStateRecord,
	successor: ArtifactStateRecord,
	policy: "cancel" | "finish_as_invalid" | "audited_override",
): { previous: ArtifactStateRecord; successor: ArtifactStateRecord } {
	if (!["frozen", "sealed"].includes(previous.state))
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"Only live artifacts can be superseded.",
		);
	if (previous.artifactId !== successor.artifactId || successor.version <= previous.version)
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"A successor must be a newer version of the same artifact.",
		);
	return {
		previous: { ...previous, state: "superseded", invalidationPolicy: policy },
		successor: {
			...successor,
			supersedes: {
				artifactId: previous.artifactId,
				version: previous.version,
				digest: previous.digest,
			},
		},
	};
}
