import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IBinaryData,
} from 'n8n-workflow';
import  {
	NodeOperationError,
} from 'n8n-workflow';
import * as net from 'net';
import { spawn } from 'child_process';
import { performance } from 'perf_hooks';
import { WyomingCredentials } from '../../credentials/WyomingApi.credentials';

// --- Constants ---
const __version__ = '1.0.0';
const SAMPLE_RATE = 16000; // Target sample rate for Wyoming (STT input, TTS output likely)
const SAMPLE_WIDTH = 2; // Target sample width (bytes) for Wyoming (16-bit) (STT input, TTS output likely)
const SAMPLE_CHANNELS = 1; // Target channels for Wyoming (Mono) (STT input, TTS output likely)
const AUDIO_CHUNK_SIZE = 64 * 1024; // 64 KiB chunks for sending audio
const NODE_NAME_LOG_PREFIX = '[WyomingNode]';
const NEWLINE = Buffer.from('\n', 'utf-8');
const WYOMING_AUDIO_FORMAT_DESC = `PCM, ${SAMPLE_RATE / 1000}kHz sample rate, ${SAMPLE_WIDTH * 8}-bit depth, Mono channel`;
const SYNTHESIZE_EVENT_TYPE = 'synthesize';
const TRANSCRIBE_EVENT_TYPE = 'transcribe';
const AUDIO_START_EVENT_TYPE = 'audio-start';
const AUDIO_CHUNK_EVENT_TYPE = 'audio-chunk';
const AUDIO_STOP_EVENT_TYPE = 'audio-stop';
const TRANSCRIPT_EVENT_TYPE = 'transcript';
const ERROR_EVENT_TYPE = 'error';
const DEFAULT_TTS_OUTPUT_MIME_TYPE = 'audio/wav';

// --- Utility Functions ---

const yieldEventLoop = () => new Promise(resolve => setImmediate(resolve));

const _waitForDrain = (client: net.Socket, logPrefix: string): Promise<void> => {
	return new Promise((resolve) => {
		// logger.trace(`${logPrefix} Write buffer full. Waiting for drain event...`);
		client.once('drain', () => {
			// logger.trace(`${logPrefix} Drain event received. Resuming writes.`);
			resolve();
		});
	});
};

function _addWavHeader(
	pcmData: Buffer,
	sampleRate: number,
	sampleWidthBytes: number,
	channels: number
): Buffer {
	const bitsPerSample = sampleWidthBytes * 8;
	const blockAlign = channels * sampleWidthBytes;
	const byteRate = sampleRate * blockAlign;
	const dataSize = pcmData.length;
	const fileSize = 36 + dataSize;

	const header = Buffer.alloc(44);

	header.write('RIFF', 0);
	header.writeUInt32LE(fileSize, 4);
	header.write('WAVE', 8);

	header.write('fmt ', 12);
	header.writeUInt32LE(16, 16);
	header.writeUInt16LE(1, 20);
	header.writeUInt16LE(channels, 22);
	header.writeUInt32LE(sampleRate, 24);
	header.writeUInt32LE(byteRate, 28);
	header.writeUInt16LE(blockAlign, 32);
	header.writeUInt16LE(bitsPerSample, 34);

	header.write('data', 36);
	header.writeUInt32LE(dataSize, 40);

	return Buffer.concat([header, pcmData]);
}

async function _sendWyomingEventRevised(
	execContext: IExecuteFunctions, // Pass the full context
	client: net.Socket,
	eventType: string,
	eventData: object | null,
	payload: Buffer | null = null,
	logPrefix: string
): Promise<void> {
	const logger = execContext.logger; // Get logger from context
	try {
		const primaryJson: Record<string, any> = { type: eventType, version: __version__ };
		let dataBytes: Buffer | null = null;
		if (eventData && Object.keys(eventData).length > 0) {
			try {
				dataBytes = Buffer.from(JSON.stringify(eventData), 'utf-8');
				primaryJson['data_length'] = dataBytes.length;
			} catch(e: any) {
				logger.error(`${logPrefix} Failed to stringify event data for type ${eventType}: ${e.message}`);
				throw e;
			}
		}
		if (payload) {
			primaryJson['payload_length'] = payload.length;
		}
		const primaryJsonString = JSON.stringify(primaryJson);
		const primaryJsonBuffer = Buffer.from(primaryJsonString, 'utf-8');
		logger.debug(`${logPrefix} Sending event: Type=${eventType}, DataLength=${primaryJson.data_length ?? 0}, PayloadLength=${primaryJson.payload_length ?? 0}`);

		await yieldEventLoop();
		if (!client.write(primaryJsonBuffer)) await _waitForDrain(client, logPrefix);
		if (!client.write(NEWLINE)) await _waitForDrain(client, logPrefix);

		if (dataBytes) {
			if (!client.write(dataBytes)) await _waitForDrain(client, logPrefix);
		}

		if (payload) {
			if (!client.write(payload)) await _waitForDrain(client, logPrefix);
		}

	} catch (error: any) {
		logger.error(`${logPrefix} Error during event construction/write for event ${eventType}: ${error.message}`, error);
		await yieldEventLoop(); // Allow potential error handling on socket
		// Re-throw the error to be caught by the calling context (_handleWyomingCommunication)
		// Use execContext.getNode() here
		throw new NodeOperationError(execContext.getNode(), `Failed to send Wyoming event ${eventType}: ${error.message}`);
	}
}


async function _handleWyomingCommunication<T>(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	timeoutMs: number,
	initialSendLogic: (execContext: IExecuteFunctions, client: net.Socket, logPrefix: string) => Promise<void>, // Pass context
	processReceivedEvent: (
		eventType: string,
		eventData: Record<string, any> | null,
		payload: Buffer | null,
		state: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error },
		logPrefix: string
	) => Promise<void>,
	getFinalResult: (state: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error }) => T | Promise<T>,
	operationType: 'STT' | 'TTS'
): Promise<T> {
	const logger = execContext.logger;
	const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex} (${operationType}):`;

	return new Promise(async (resolve, reject) => {
		let host: string;
		let port: number;
		try {
			const parts = serverAddress.split(':');
			if (parts.length !== 2 || !parts[0] || !parts[1] || isNaN(parseInt(parts[1], 10))) { throw new NodeOperationError(execContext.getNode(), 'Invalid server address format. Use host:port'); }
			host = parts[0]; port = parseInt(parts[1], 10);
			if (port <= 0 || port > 65535) { throw new NodeOperationError(execContext.getNode(), 'Invalid port number.'); }
		} catch (error: any) {
			logger.error(`${logPrefix} Error parsing server address: ${error.message}`);
			const addrError = new NodeOperationError(execContext.getNode(), `Invalid Wyoming Server Address: ${error.message}`, { itemIndex });
			(addrError as any).isConfigurationError = true;
			return reject(addrError);
		}

		const client = new net.Socket();
		let receivedDataBuffer = Buffer.alloc(0);
		let connectionClosed = false;
		let appTimeoutHandle: NodeJS.Timeout | null = null;
		const processingState: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error } = {
			audioChunks: [],
			done: false,
		};

		const cleanup = async (reason?: string) => {
			if (appTimeoutHandle) { clearTimeout(appTimeoutHandle); appTimeoutHandle = null; }
			if (!connectionClosed) {
				connectionClosed = true;
				if (!client.destroyed) {
					logger.debug(`${logPrefix} Cleaning up connection (${reason || 'normal close'}). State: ${client.readyState}`);
					await yieldEventLoop();
					client.removeAllListeners();
					client.destroy();
				}
			}
		};

		appTimeoutHandle = setTimeout(() => {
			const errorMsg = `Application timeout reached after ${timeoutMs}ms waiting for ${operationType} result`;
			logger.warn(`${logPrefix} ${errorMsg}`);
			const timeoutError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
			(timeoutError as any).isTimeout = true;
			processingState.error = timeoutError;
			processingState.done = true;
			appTimeoutHandle = null;
			if (!connectionClosed) {
				connectionClosed = true;
				client.removeAllListeners();
				client.destroy();
			}
			reject(timeoutError);
		}, timeoutMs);

		client.on('connect', async () => {
			logger.debug(`${logPrefix} TCP connection established with ${host}:${port}.`);
			await yieldEventLoop();
			try {
				// Pass execContext to initialSendLogic
				await initialSendLogic(execContext, client, logPrefix);
				logger.debug(`${logPrefix} Successfully sent initial request sequence.`);
				await yieldEventLoop();
			} catch (err: any) {
				const errorMsg = `Error during initial data sending sequence: ${err.message}`;
				logger.error(`${logPrefix} ${errorMsg}`, err);
				await cleanup('write error sequence');
				if (appTimeoutHandle && !connectionClosed) {
					// Use the error directly if it's already a NodeOperationError
					reject(err instanceof NodeOperationError ? err : new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex }));
				}
			}
		});

		client.on('data', async (chunk: Buffer) => {
			receivedDataBuffer = Buffer.concat([receivedDataBuffer, chunk]);
			await yieldEventLoop();

			while (true) {
				const newlineIndex = receivedDataBuffer.indexOf(NEWLINE);
				if (newlineIndex === -1) {
					break;
				}

				const primaryJsonBuffer = receivedDataBuffer.subarray(0, newlineIndex);
				let eventDict: Record<string, any>;
				try {
					const primaryJsonString = primaryJsonBuffer.toString('utf-8');
					eventDict = JSON.parse(primaryJsonString);

					const dataLength = eventDict['data_length'] as number | undefined ?? 0;
					const payloadLength = eventDict['payload_length'] as number | undefined ?? 0;

					const messageJsonLineLength = primaryJsonBuffer.length + NEWLINE.length;
					const messageDataPayloadLength = dataLength + payloadLength;
					const fullMessageLength = messageJsonLineLength + messageDataPayloadLength;

					if (!processingState.done && receivedDataBuffer.length < fullMessageLength) {
						break;
					}

					let dataDict: object | null = eventDict['data'] || null;
					let dataBytes: Buffer | null = null;
					if (dataLength > 0) {
						const dataStart = messageJsonLineLength;
						const dataEnd = dataStart + dataLength;
						dataBytes = receivedDataBuffer.subarray(dataStart, dataEnd);
						try {
							const separateDataDict = JSON.parse(dataBytes.toString('utf-8'));
							if (!dataDict) dataDict = {};
							Object.assign(dataDict, separateDataDict);
						} catch (parseError: any) {
							logger.error(`${logPrefix} Failed to parse separate Data Bytes: ${dataBytes.toString('utf-8')}. Error: ${parseError.message}. Ignoring data part for this event.`);
							dataDict = dataDict || {};
						}
					}

					let payloadBytes: Buffer | null = null;
					if (payloadLength > 0) {
						const payloadStart = messageJsonLineLength + dataLength;
						const payloadEnd = payloadStart + payloadLength;
						payloadBytes = receivedDataBuffer.subarray(payloadStart, payloadEnd);
					}

					receivedDataBuffer = receivedDataBuffer.subarray(fullMessageLength);
					await yieldEventLoop();

					const eventType = eventDict['type'] as string;
					// logger.debug(`${logPrefix} Rcvd & Parsed Complete Event: Type=${eventType}, DataKeys=${Object.keys(dataDict || {}).join(',') || 'None'}, PayloadLength=${payloadBytes?.length ?? 0}`);
					await yieldEventLoop();

					try {
						await processReceivedEvent(eventType, dataDict, payloadBytes, processingState, logPrefix);
					} catch (processorError: any) {
						logger.error(`${logPrefix} Error processing received event type ${eventType}: ${processorError.message}`, processorError);
						processingState.error = processorError instanceof NodeOperationError ? processorError : new NodeOperationError(execContext.getNode(), processorError.message || String(processorError), { itemIndex });
						processingState.done = true;
					}

				} catch (parseError: any) {
					logger.error(`${logPrefix} Failed to parse Primary JSON Line: "${primaryJsonBuffer.toString('utf-8')}". Error: ${parseError.message}. Discarding line.`);
					processingState.error = new NodeOperationError(execContext.getNode(), "Invalid server response", { itemIndex });
					processingState.done = true;
					receivedDataBuffer = receivedDataBuffer.subarray(newlineIndex + NEWLINE.length);
					await yieldEventLoop();
				}

				if (processingState.done) {
					logger.info(`${logPrefix} Operation marked as done. Cleaning up.`);
					await cleanup(processingState.error ? 'error processing event' : 'operation complete');
					if (processingState.error) {
						reject(processingState.error);
					} else {
						try {
							const finalResult = await getFinalResult(processingState);
							resolve(finalResult);
						} catch (resultError: any)
						 {
							logger.error(`${logPrefix} Error finalizing result: ${resultError.message}`, resultError);
							reject(resultError instanceof NodeOperationError ? resultError : new NodeOperationError(execContext.getNode(), resultError.message || String(resultError), { itemIndex }));
						}
					}
					return;
				}

				if (receivedDataBuffer.length === 0) {
					break;
				}
			}
		});

		client.on('end', async () => {
			await cleanup('server ended connection');
			if (!processingState.done && appTimeoutHandle && !connectionClosed) {
				const errorMsg = `Connection closed by server before ${operationType} completed.`;
				logger.warn(`${logPrefix} ${errorMsg}`);
				const closeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(closeError as any).isConnectionClosed = true;
				reject(closeError);
			}
		});

		client.on('close', async (hadError: boolean) => {
			const cleanupReason = `closed${hadError ? ' with error' : ''}`;
			const shouldReject = !processingState.done && appTimeoutHandle && !connectionClosed;
			await cleanup(cleanupReason);
			if (shouldReject) {
				 const errorMsg = `Connection closed unexpectedly${hadError ? ' with error' : ''}.`;
				 logger.warn(`${logPrefix} ${errorMsg}`);
				 const closeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				 (closeError as any).isConnectionClosed = true;
				 reject(closeError);
			 }
		});

		client.on('error', async (err: Error & { code?: string }) => {
			const code = err.code;
			let errorMsg = `Socket error: ${err.message}`;
			if (code) errorMsg += ` (Code: ${code})`;
			logger.error(`${logPrefix} ${errorMsg}`, err);
			const shouldReject = !processingState.done && appTimeoutHandle && !connectionClosed;
			await cleanup('socket error');
			if (shouldReject) {
				const nodeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(nodeError as any).originalCode = code;
				if (code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code === 'EHOSTUNREACH') {
					(nodeError as any).isConfigurationError = true;
				}
				reject(nodeError);
			}
		});

		client.on('drain', async () => {
			// Drain event is handled internally by _sendWyomingEventRevised using _waitForDrain
			// logger.trace(`${logPrefix} Socket write buffer drained.`);
			await yieldEventLoop();
		});

		logger.debug(`${logPrefix} Attempting to connect to ${host}:${port}...`);
		await yieldEventLoop();
		client.connect(port, host);
	});
}

const toTitleCase = (input: string | null | undefined): string | null => {
    if (!input) return null;
    return input.match(/[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+/g)
        ?.map((x) => x.slice(0, 1).toUpperCase() + x.slice(1).toLowerCase()) // Ensure correct casing
        .join(' ') || input; // Return original if regex fails
}

async function _transcribeAudio(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	audioBuffer: Buffer,
	language: string,
	timeoutMs: number,
): Promise<string> {

	// Update signature to accept execContext
	const initialSendLogic = async (execCtx: IExecuteFunctions, client: net.Socket, logPrefix: string) => {
		await _sendWyomingEventRevised(execCtx, client, TRANSCRIBE_EVENT_TYPE, { language }, null, logPrefix);
		await _sendWyomingEventRevised(execCtx, client, AUDIO_START_EVENT_TYPE, { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, null, logPrefix);

		execCtx.logger.debug(`${logPrefix} Sending audio data in chunks (${AUDIO_CHUNK_SIZE} bytes each)...`);
		for (let i = 0; i < audioBuffer.length; i += AUDIO_CHUNK_SIZE) {
			const chunk = audioBuffer.subarray(i, Math.min(i + AUDIO_CHUNK_SIZE, audioBuffer.length));
			// execCtx.logger.trace(`${logPrefix} Sending audio chunk: ${chunk.length} bytes.`);
			await _sendWyomingEventRevised(execCtx, client, AUDIO_CHUNK_EVENT_TYPE, { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, chunk, logPrefix);
		}
		execCtx.logger.debug(`${logPrefix} Finished sending audio chunks.`);

		await _sendWyomingEventRevised(execCtx, client, AUDIO_STOP_EVENT_TYPE, {}, null, logPrefix);
	};

	const processReceivedEvent = async (
		eventType: string,
		eventData: Record<string, any> | null,
		_payload: Buffer | null,
		state: { audioChunks: Buffer[], done: boolean, result?: string, error?: Error },
		logPrefix: string
	) => {
		if (eventType === TRANSCRIPT_EVENT_TYPE && eventData && typeof eventData.text === 'string') {
			const transcription = eventData.text;
			execContext.logger.info(`${logPrefix} Transcription successful: "${transcription}"`);
			state.result = transcription;
			state.done = true;
		} else if (eventType === ERROR_EVENT_TYPE) {
			const codeStr = toTitleCase(eventData?.code);
			const textStr = eventData?.text;
			let errMsg = eventData?.message || 'Unknown server error during transcription';
			if (codeStr && textStr) {
				errMsg = `${codeStr}: ${textStr}`;
			} else if (codeStr) {
				errMsg = codeStr;
			}
			execContext.logger.error(`${logPrefix} Received error event from server: ${errMsg}`);
			const serverError = new NodeOperationError(execContext.getNode(), `Wyoming server error (STT): ${errMsg}`, { itemIndex });
			(serverError as any).isServerError = true;
			state.error = serverError;
			state.done = true;
		} else {
			execContext.logger.warn(`${logPrefix} Received unhandled/ignored event type during STT: ${eventType}`);
		}
	};

	const getFinalResult = (state: { audioChunks: Buffer[], done: boolean, result?: string, error?: Error }): string => {
		if (state.result !== undefined) {
			return state.result;
		}
		throw new NodeOperationError(execContext.getNode(), 'Transcription finished unexpectedly without result or error.', { itemIndex });
	};

	return _handleWyomingCommunication<string>(
		execContext,
		itemIndex,
		serverAddress,
		timeoutMs,
		initialSendLogic,
		processReceivedEvent,
		getFinalResult,
		'STT'
	);
}

async function _synthesizeAudio(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	textToSpeak: string,
	voiceName: string | undefined,
	timeoutMs: number,
): Promise<Buffer> { // Returns raw PCM buffer

	// Update signature to accept execContext
	const initialSendLogic = async (execCtx: IExecuteFunctions, client: net.Socket, logPrefix: string) => {
		const synthesizeData: Record<string, any> = { text: textToSpeak };
		if (voiceName) {
			synthesizeData.voice = { name: voiceName };
		}
		// If no voiceName, the server uses its default.
		await _sendWyomingEventRevised(execCtx, client, SYNTHESIZE_EVENT_TYPE, synthesizeData, null, logPrefix);
	};

	const processReceivedEvent = async (
		eventType: string,
		eventData: Record<string, any> | null,
		payload: Buffer | null,
		state: { audioChunks: Buffer[], done: boolean, result?: Buffer, error?: Error },
		logPrefix: string
	) => {
		if (eventType === AUDIO_START_EVENT_TYPE) {
			const rate = eventData?.rate ?? 'unknown';
			const width = eventData?.width ?? 'unknown';
			const channels = eventData?.channels ?? 'unknown';
			execContext.logger.info(`${logPrefix} Received audio-start. Format: ${rate} Hz, ${width} bytes/sample, ${channels} channel(s).`);
			if (rate !== SAMPLE_RATE || width !== SAMPLE_WIDTH || channels !== SAMPLE_CHANNELS) {
				execContext.logger.warn(`${logPrefix} Received audio format differs from node defaults. Using node defaults for WAV header.`);
			}
			state.audioChunks = [];
		} else if (eventType === AUDIO_CHUNK_EVENT_TYPE) {
			if (payload) {
				state.audioChunks.push(payload);
			} else {
				execContext.logger.warn(`${logPrefix} Received audio-chunk event with no payload.`);
			}
		} else if (eventType === AUDIO_STOP_EVENT_TYPE) {
			execContext.logger.info(`${logPrefix} Received audio-stop. Synthesis complete.`);
			state.done = true;
		} else if (eventType === ERROR_EVENT_TYPE) {
			const codeStr = toTitleCase(eventData?.code);
			const textStr = eventData?.text;
			let errMsg = eventData?.message || 'Unknown server error during synthesis';
			if (codeStr && textStr) {
				errMsg = `${codeStr}: ${textStr}`;
			} else if (codeStr) {
				errMsg = codeStr;
			}
			execContext.logger.error(`${logPrefix} Received error event from server: ${errMsg}`);
			const serverError = new NodeOperationError(execContext.getNode(), `Wyoming server error (TTS): ${errMsg}`, { itemIndex });
			(serverError as any).isServerError = true;
			state.error = serverError;
			state.done = true;
		} else {
			execContext.logger.warn(`${logPrefix} Received unhandled/ignored event type during TTS: ${eventType}`);
		}
	};

	const getFinalResult = (state: { audioChunks: Buffer[], done: boolean, result?: Buffer, error?: Error }): Buffer => {
		if (state.audioChunks.length > 0) {
			return Buffer.concat(state.audioChunks);
		} else if (state.error) {
            throw state.error;
        } else {
			execContext.logger.warn(`[WyomingNode] TTS Synthesis finished but no audio data was received.`);
			return Buffer.alloc(0);
		}
	};

	return _handleWyomingCommunication<Buffer>(
		execContext,
		itemIndex,
		serverAddress,
		timeoutMs,
		initialSendLogic,
		processReceivedEvent,
		getFinalResult,
		'TTS'
	);
}

async function convertAudioToPcm(
	inputBuffer: Buffer,
	_inputFileNameHint: string | undefined,
	targetSampleRate: number,
	targetChannels: number,
	logger: IExecuteFunctions['logger'],
	logPrefix: string,
	timeoutMs: number,
): Promise<Buffer> {

	const ffmpegArgs = [
		'-loglevel', 'error',
		'-i', 'pipe:0',
		'-f', 's16le',
		'-ar', String(targetSampleRate),
		'-ac', String(targetChannels),
		'-',
	];
	logger.info(`${logPrefix} Starting audio conversion with ffmpeg. Target: ${targetSampleRate}Hz, ${targetChannels === 1 ? 'Mono' : 'Stereo'} ${SAMPLE_WIDTH * 8}-bit PCM.`);
	logger.debug(`${logPrefix} Running ffmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

	return new Promise<Buffer>((resolve, reject) => {
		const ffmpegProcess = spawn('ffmpeg', ffmpegArgs, { stdio: ['pipe', 'pipe', 'pipe'] });
		const outputChunks: Buffer[] = [];
		const errorChunks: Buffer[] = [];
		let settled = false;

		const timeoutHandle = setTimeout(() => {
			if (settled) return;
			settled = true;
			logger.warn(`${logPrefix} ffmpeg conversion timed out after ${timeoutMs}ms. Killing process.`);
			ffmpegProcess.kill('SIGKILL');
			const timeoutError = new Error(`ffmpeg conversion timed out after ${timeoutMs}ms`);
			(timeoutError as any).isTimeout = true;
			reject(timeoutError);
		}, timeoutMs);

		const resolveOnce = (value: Buffer) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeoutHandle);
			resolve(value);
		};

		const rejectOnce = (error: Error) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeoutHandle);
			reject(error);
		};

		ffmpegProcess.stdout.on('data', (chunk: Buffer) => {
			outputChunks.push(chunk);
		});

		ffmpegProcess.stderr.on('data', (chunk: Buffer) => {
			errorChunks.push(chunk);
		});

		ffmpegProcess.on('close', (code) => {
			const stderrOutput = Buffer.concat(errorChunks).toString('utf-8').trim();
			if (code !== 0) {
				logger.error(`${logPrefix} ffmpeg process exited with code ${code}.`);
				if (stderrOutput) logger.error(`${logPrefix} ffmpeg stderr: ${stderrOutput}`);
				else logger.error(`${logPrefix} ffmpeg stderr: (empty)`);

				let userMessage = `ffmpeg conversion failed (exit code ${code})`;
				if (stderrOutput.includes('Invalid data found when processing input')) {
					userMessage += ': Input audio data seems invalid or corrupted.';
				} else if (stderrOutput.includes('Output file #0 does not contain any stream')) {
					userMessage += ': ffmpeg could not produce output, possibly due to input format issues or invalid parameters.';
				} else if (stderrOutput) {
					// Only append stderr if it's not excessively long
					userMessage += `: ${stderrOutput.substring(0, 200)}${stderrOutput.length > 200 ? '...' : ''}`;
				}

				const ffmpegError = new Error(userMessage);
				(ffmpegError as any).isFFmpegError = true;
				rejectOnce(ffmpegError);
			} else {
				const pcmBuffer = Buffer.concat(outputChunks);
				if (pcmBuffer.length === 0) {
					logger.error(`${logPrefix} ffmpeg conversion succeeded (code 0) but produced empty output.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (may contain clues): ${stderrOutput}`);
					const ffmpegError = new Error('ffmpeg conversion produced empty output.');
					(ffmpegError as any).isFFmpegError = true;
					rejectOnce(ffmpegError);
				} else {
					logger.info(`${logPrefix} ffmpeg conversion successful. Resulting PCM buffer size: ${pcmBuffer.length} bytes.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (possibly warnings): ${stderrOutput}`);
					resolveOnce(pcmBuffer);
				}
			}
		});

		ffmpegProcess.on('error', (err) => {
			logger.error(`${logPrefix} Failed to start ffmpeg process: ${err.message}`);
			const spawnError = new Error(`Failed to spawn ffmpeg: ${err.message}. Is ffmpeg installed and in PATH?`);
			(spawnError as any).isSpawnError = true;
			rejectOnce(spawnError);
		});

		ffmpegProcess.stdin.on('error', (err: NodeJS.ErrnoException) => {
			if (err.code !== 'EPIPE') {
				logger.warn(`${logPrefix} Error writing to ffmpeg stdin (process might have exited): ${err.message} (${err.code})`);
			}
		});

		try {
			ffmpegProcess.stdin.write(inputBuffer, (err) => {
				if (err && !ffmpegProcess.killed && (err as NodeJS.ErrnoException).code !== 'EPIPE') {
					logger.warn(`${logPrefix} Error writing buffer chunk to ffmpeg stdin: ${err.message}.`);
				}
				if (!ffmpegProcess.stdin.destroyed) {
					ffmpegProcess.stdin.end((endErr: Error | null | undefined) => {
						if (endErr && !ffmpegProcess.killed && (endErr as NodeJS.ErrnoException).code !== 'EPIPE') {
							logger.warn(`${logPrefix} Error ending ffmpeg stdin: ${endErr.message}.`);
						}
					});
				}
			});
		} catch (error: any) {
			logger.error(`${logPrefix} Exception while initiating write to ffmpeg stdin: ${error.message}`);
			const stdinError = new Error(`Failed writing input to ffmpeg: ${error.message}`);
			(stdinError as any).isStdinError = true;
			rejectOnce(stdinError);
			if (!ffmpegProcess.killed) {
				ffmpegProcess.kill();
			}
		}
	});
}


// --- N8N Node Class Definition ---

export class WyomingNode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Wyoming Protocol',
		name: 'wyomingNode',
		icon: 'file:transcribe.svg',
		group: ['transform'],
		version: 2, // Updated version due to significant changes (backpressure handling)
		description: `Performs Speech-to-Text (STT) or Text-to-Speech (TTS) using the Wyoming protocol. Connects to servers like Piper (TTS) or Whisper (STT). Requires 'ffmpeg' for STT audio conversion. STT Input: Various formats (MP3, WAV, Ogg, etc.). STT Output: Text. TTS Input: Text. TTS Output: WAV Audio (${WYOMING_AUDIO_FORMAT_DESC} internally).`,
		defaults: { name: 'Wyoming Node' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'wyomingApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Speech to Text (STT)',
						value: 'stt',
						description: 'Convert audio input to text',
						action: 'Speech to text',
					},
					{
						name: 'Text to Speech (TTS)',
						value: 'tts',
						description: 'Convert text input to audio',
						action: 'Text to speech',
					},
				],
				default: 'tts',
				description: 'Choose whether to convert speech to text or text to speech',
			},
			{
				displayName: 'Language',
				name: 'language',
				type: 'string',
				default: 'en',
				placeholder: 'e.g., en, de, fr, en-us',
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				description: 'Language code (e.g., ISO 639-1 or IETF) for STT. Check server documentation for supported codes/formats. Required for STT.',
				required: true, // Made required when STT is selected
			},
			{
				displayName: 'Timeout (Ms)',
				name: 'timeoutMs',
				type: 'number',
				typeOptions: { minValue: 1000 },
				default: 60000,
				description: 'Maximum time (milliseconds) to wait for the entire operation (connection, processing, response)',
				required: true,
			},
			{
				displayName: 'Input Binary Field (Audio)',
				name: 'inputBinaryField',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				placeholder: 'e.g., data',
				description: 'Name of the field containing the input binary audio data for STT. Supported formats depend on installed \'ffmpeg\' (e.g., MP3, WAV, Ogg, FLAC, AAC, Opus).',
			},
			{
				displayName: 'Output Field Name (Text)',
				name: 'outputFieldNameStt',
				type: 'string',
				default: 'transcription',
				required: true,
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				description: 'Field name where the resulting transcribed text will be stored',
			},
			{
				displayName: 'Input Text Field',
				name: 'inputText',
				type: 'string',
				default: '',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				placeholder: 'Enter text or e.g. {{$json.message}}',
				description: 'Text to be synthesized into speech. Can use expressions.',
			},
			{
				displayName: 'Voice Name/ID',
				name: 'voice',
				type: 'string',
				default: 'en_US-lessac-medium',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				placeholder: 'e.g., en_US-lessac-medium (Optional)',
				description: '(Optional) Name or ID of the voice for TTS. Leave empty to use the server\'s default. Check your TTS server (e.g., Piper) documentation.',
			},
			{
				displayName: 'Output Field Name (Audio)',
				name: 'outputFieldNameTts',
				type: 'string',
				default: 'audio',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				description: `Field name where the resulting synthesized audio (as binary WAV data) will be stored. MIME Type: ${DEFAULT_TTS_OUTPUT_MIME_TYPE}.`,
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const logger = this.logger;

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			const item = items[itemIndex];
			const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex}:`;
			const overallStartTime = performance.now();

			const operation = this.getNodeParameter('operation', itemIndex) as 'stt' | 'tts';
			const credentials = await this.getCredentials('wyomingApi') as WyomingCredentials;
			const serverAddress = `${credentials.host}:${credentials.port}`;
			const timeoutMs = this.getNodeParameter('timeoutMs', itemIndex, 60000) as number;
			const operationDeadline = performance.now() + timeoutMs;
			const remainingTimeoutMs = () => Math.max(1, Math.ceil(operationDeadline - performance.now()));
			try {
				let newItem: INodeExecutionData | null = null;

				if (operation === 'stt') {
					const language = this.getNodeParameter('language', itemIndex) as string; // Required for STT now
					const inputBinaryField = this.getNodeParameter('inputBinaryField', itemIndex) as string;
					const outputFieldName = this.getNodeParameter('outputFieldNameStt', itemIndex) as string;

					// Language validation happens implicitly as it's required
					// No need for !language check if required: true is set

					const binaryData = item.binary?.[inputBinaryField];
					if (!binaryData) throw new NodeOperationError(this.getNode(), `STT: No binary data found in input field "${inputBinaryField}".`, { itemIndex });

					const inputAudioBuffer = await this.helpers.getBinaryDataBuffer(itemIndex, inputBinaryField);
					if (!inputAudioBuffer || inputAudioBuffer.length === 0) throw new NodeOperationError(this.getNode(), `STT: Binary data in input field "${inputBinaryField}" is present but empty.`, { itemIndex });

					const fileName = binaryData.fileName;
					logger.debug(`${logPrefix} STT Input Info: Filename='${fileName || 'N/A'}', Size=${inputAudioBuffer.length} bytes`);

					let pcmBufferToSend: Buffer;
					let conversionDurationMs = 0;
					logger.debug(`${logPrefix} STT: Preparing audio using ffmpeg conversion to ${WYOMING_AUDIO_FORMAT_DESC}.`);
					try {
						const conversionStartTime = performance.now();
						// Pass logger directly, it doesn't need the full context
						pcmBufferToSend = await convertAudioToPcm(inputAudioBuffer, fileName, SAMPLE_RATE, SAMPLE_CHANNELS, logger, logPrefix, remainingTimeoutMs());
						const conversionEndTime = performance.now();
						conversionDurationMs = conversionEndTime - conversionStartTime;
						logger.info(`${logPrefix} STT: FFmpeg conversion took ${conversionDurationMs.toFixed(2)} ms.`);
					} catch (conversionError: any) {
						const errorMessage = `STT Error during ffmpeg conversion: ${conversionError.message || String(conversionError)}`;
						const errorOptions: Record<string, any> = { itemIndex, description: errorMessage };
						if ((conversionError as any).isFFmpegError) errorOptions.isFFmpegError = true;
						if ((conversionError as any).isSpawnError) errorOptions.isSpawnError = true;
						throw new NodeOperationError(this.getNode(), errorMessage, errorOptions);
					}
					if (!pcmBufferToSend || pcmBufferToSend.length === 0) {
						throw new NodeOperationError(this.getNode(), 'STT: Audio conversion resulted in an empty buffer.', { itemIndex });
					}

					let transcriptionDurationMs = 0;
					logger.debug(`${logPrefix} STT: Starting transcription process. Language: ${language}`);
					const transcriptionStartTime = performance.now();
					// Pass `this` (IExecuteFunctions) as execContext
					const transcription = await _transcribeAudio(this, itemIndex, serverAddress, pcmBufferToSend, language, remainingTimeoutMs());
					const transcriptionEndTime = performance.now();
					transcriptionDurationMs = transcriptionEndTime - transcriptionStartTime;
					logger.info(`${logPrefix} STT: Wyoming transcription took ${transcriptionDurationMs.toFixed(2)} ms.`);

					const newItemJson = JSON.parse(JSON.stringify(item.json));
					newItemJson[outputFieldName] = transcription;
					newItem = { json: newItemJson, pairedItem: { item: itemIndex } };

					const overallEndTime = performance.now();
					logger.info(`${logPrefix} STT: Successfully processed item in ${(overallEndTime - overallStartTime).toFixed(2)} ms (Conversion: ${conversionDurationMs.toFixed(2)} ms, Transcription: ${transcriptionDurationMs.toFixed(2)} ms).`);

				}
				else if (operation === 'tts') {
					const voice = this.getNodeParameter('voice', itemIndex, undefined) as string | undefined; // Optional
					const outputFieldName = this.getNodeParameter('outputFieldNameTts', itemIndex) as string; // Use TTS output field
					const textToSpeak = this.getNodeParameter('inputText', itemIndex) as string;

					if (typeof textToSpeak !== 'string' || textToSpeak.trim().length === 0) {
						throw new NodeOperationError(this.getNode(), `TTS: Input text is missing or empty. Please provide text in the 'Input Text Field' parameter.`, { itemIndex });
					}
					if (voice) logger.debug(`${logPrefix} TTS: Using voice: ${voice}`);
                    else logger.debug(`${logPrefix} TTS: No voice specified, using server default.`);

					let synthesisDurationMs = 0;
					logger.debug(`${logPrefix} TTS: Starting synthesis process for text: "${textToSpeak.substring(0, 50)}${textToSpeak.length > 50 ? '...' : ''}"`);
					const synthesisStartTime = performance.now();
					// Pass `this` (IExecuteFunctions) as execContext
					const rawPcmAudioBuffer = await _synthesizeAudio(this, itemIndex, serverAddress, textToSpeak, voice || undefined, remainingTimeoutMs()); // Pass undefined if empty string
					const synthesisEndTime = performance.now();
					synthesisDurationMs = synthesisEndTime - synthesisStartTime;
					logger.info(`${logPrefix} TTS: Wyoming synthesis took ${synthesisDurationMs.toFixed(2)} ms. Received ${rawPcmAudioBuffer.length} bytes of PCM data.`);

					if (rawPcmAudioBuffer.length === 0) {
						logger.warn(`${logPrefix} TTS: Synthesis resulted in empty audio data. Returning item without audio in field "${outputFieldName}".`);
                         newItem = { json: JSON.parse(JSON.stringify(item.json)), pairedItem: { item: itemIndex } };
					} else {
						const wavBuffer = _addWavHeader(rawPcmAudioBuffer, SAMPLE_RATE, SAMPLE_WIDTH, SAMPLE_CHANNELS);
						logger.debug(`${logPrefix} TTS: Added WAV header. Total WAV size: ${wavBuffer.length} bytes.`);

						const outputFileName = `tts_output_${itemIndex}.wav`;
						const binaryOutputData: IBinaryData = await this.helpers.prepareBinaryData(wavBuffer, outputFileName, DEFAULT_TTS_OUTPUT_MIME_TYPE);

						newItem = {
							json: JSON.parse(JSON.stringify(item.json)),
							binary: {
								[outputFieldName]: binaryOutputData,
							},
							pairedItem: { item: itemIndex },
						};
					}

					const overallEndTime = performance.now();
					logger.info(`${logPrefix} TTS: Successfully processed item in ${(overallEndTime - overallStartTime).toFixed(2)} ms (Synthesis: ${synthesisDurationMs.toFixed(2)} ms).`);
				}

                if (newItem) {
				    returnData.push(newItem);
                } else {
                    logger.warn(`${logPrefix} No output item was generated for an unknown reason. Skipping item.`);
                }

			} catch (error: any) {
				const errorMessage = error.message || String(error);
				const originalCode = (error as any).originalCode;
				const isSpawnError = (error as any).isSpawnError;
				const isConfigError = (error as any).isConfigurationError;
				logger.error(`${logPrefix} Error processing item (${operation.toUpperCase()}): ${errorMessage}`, error);
				const overallEndTime = performance.now();
				logger.info(`${logPrefix} Failed processing item after ${(overallEndTime - overallStartTime).toFixed(2)} ms.`);

				const isCriticalNetworkError = ['ECONNREFUSED', 'ENOTFOUND', 'EHOSTUNREACH', 'EAI_AGAIN'].includes(originalCode) || isConfigError;
				const isCriticalError = isCriticalNetworkError || isSpawnError;

				if (isCriticalError) {
					logger.error(`${logPrefix} Critical error detected (${originalCode || (isSpawnError ? 'ffmpeg spawn failed' : 'config error')}). Stopping workflow execution.`);
					if (error instanceof NodeOperationError) throw error;
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
				} else {
					if (this.continueOnFail()) {
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is true. Recording error and continuing workflow.`);
						const n8nError = error instanceof NodeOperationError ? error : new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
						const errorItem: INodeExecutionData = {
							json: item.json,
							binary: item.binary,
							error: n8nError,
							pairedItem: { item: itemIndex },
						};
						returnData.push(errorItem);
					} else {
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is false. Stopping workflow execution.`);
						if (error instanceof NodeOperationError) throw error;
						throw new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
					}
				}
			}
		}

		logger.debug(`${NODE_NAME_LOG_PREFIX} Finished execution.`);
		return [returnData];
	}
}
