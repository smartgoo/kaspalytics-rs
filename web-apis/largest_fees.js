import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';

export async function GET({ url }) {
	let client;

	try {
		const requestId = Math.random().toString(36).substring(7);
		console.time(`Largest Fees API Query ${requestId}`);
		
		// Get time frame from query parameter, default to 24 hours
		const timeFrame = url.searchParams.get('timeframe') || '24h';
		
		// Map time frame to SQL intervals with parameterized queries
		const timeFrameMap = {
			'15m': "INTERVAL '15 MINUTES'",
			'1h': "INTERVAL '1 HOUR'", 
			'24h': "INTERVAL '24 HOURS'",
			'7d': "INTERVAL '7 DAYS'"
		};

		const intervalExpression = timeFrameMap[timeFrame] || "INTERVAL '24 HOURS'";

		let sql;
		if (timeFrame === 'today') {
			sql = `
				SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, fee
				FROM kaspad.transactions
				WHERE subnetwork_id = 0
				  AND accepting_block_hash IS NOT NULL
				  AND block_time >= date_trunc('day', now())
				ORDER BY fee DESC, block_time DESC
				LIMIT 100;`;
		} else if (timeFrame === 'yesterday') {
			sql = `
				SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, fee
				FROM kaspad.transactions
				WHERE subnetwork_id = 0
				  AND accepting_block_hash IS NOT NULL
				  AND block_time >= (date_trunc('day', now()) - INTERVAL '1 day')
				  AND block_time < date_trunc('day', now())
				ORDER BY fee DESC, block_time DESC
				LIMIT 100;`;
		} else {
			sql = `
				SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, fee
				FROM kaspad.transactions
				WHERE subnetwork_id = 0
				  AND accepting_block_hash IS NOT NULL
				  AND block_time >= (NOW() - ${intervalExpression})
				ORDER BY fee DESC, block_time DESC
				LIMIT 100;`;
		}

		const pool = getDb();
		client = await pool.connect();
		const r = await client.query(sql);
		
		console.timeEnd(`Largest Fees API Query ${requestId}`);
		
		const payload = {
			status: 'success',
			transactions: r.rows,
			timeFrame: timeFrame
		};

		return json(payload, { headers: { 'Cache-Control': 'public, max-age=5, s-maxage=5' } });
	} catch (error) {
		console.error('Database query failed:', error);

		return json({
			status: 'error',
			transactions: [],
			error: error.message,
			timeFrame: url.searchParams.get('timeframe') || '24h'
		}, { status: 500 });
	} finally {
		if (client) client.release();
	}
} 