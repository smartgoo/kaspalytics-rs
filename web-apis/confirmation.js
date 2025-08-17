import { json, error } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';

// Validation function for transaction IDs
function isValidTransactionId(txId) {
	// Kaspa transaction IDs are typically 64-character hexadecimal strings
	const pattern = /^[a-fA-F0-9]{64}$/;
	return pattern.test(txId);
}

export async function GET({ params }) {
	const transactionId = params.id;

	// Validate transaction ID format
	if (!isValidTransactionId(transactionId)) {
		throw error(400, {
			message: 'Invalid transaction ID format. Transaction IDs must be 64-character hexadecimal strings.'
		});
	}

	const transactionIdBinary = Buffer.from(transactionId, 'hex');

	try {
		const db = getDb();

		// Get the accepting block hash and its blue score for this transaction
		const transactionQuery = `
			SELECT 
				encode(t.accepting_block_hash, 'hex') as accepting_block_hash,
				b.blue_score as accepting_block_blue_score
			FROM kaspad.transactions t
			LEFT JOIN kaspad.blocks b ON t.accepting_block_hash = b.block_hash
			WHERE t.transaction_id = $1
			AND t.accepting_block_hash IS NOT NULL
		`;

		// Get the current virtual (sink) blue score
		const sinkBlueScoreQuery = `
			SELECT value::bigint as sink_blue_score
			FROM key_value 
			WHERE key = 'SinkBlueScore'
		`;

		const [transactionResult, sinkResult] = await Promise.all([
			db.query(transactionQuery, [transactionIdBinary]),
			db.query(sinkBlueScoreQuery, [])
		]);

		// Check if transaction exists and is accepted
		if (transactionResult.rows.length === 0) {
			throw error(404, {
				message: 'Transaction not found or not accepted'
			});
		}

		// Check if we have sink blue score
		if (sinkResult.rows.length === 0) {
			throw error(500, {
				message: 'Unable to fetch current virtual blue score'
			});
		}

		const transactionData = transactionResult.rows[0];
		const sinkBlueScore = BigInt(sinkResult.rows[0].sink_blue_score);
		const acceptingBlockBlueScore = BigInt(transactionData.accepting_block_blue_score);

		// Calculate confirmation count
		const confirmationCount = Number(sinkBlueScore - acceptingBlockBlueScore);

		return json({
			confirmationCount,
			acceptingBlockHash: transactionData.accepting_block_hash,
			acceptingBlockBlueScore: Number(acceptingBlockBlueScore),
			sinkBlueScore: Number(sinkBlueScore)
		}, {
			headers: {
				'Cache-Control': 'public, max-age=30, s-maxage=30',
				'Vercel-CDN-Cache-Control': 'public, max-age=30'
			}
		});

	} catch (err) {
		console.error(`Error fetching confirmation count for transaction ${transactionId}:`, err);
		
		if (err.status) {
			throw err;
		}
		
		throw error(500, {
			message: 'Failed to fetch confirmation count'
		});
	}
} 