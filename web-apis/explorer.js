import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';

export async function GET({ params }) {
	const hash = params.hash;
	
	// Validate hex format
	const pattern = /^[a-fA-F0-9]{64}$/;
	if (!pattern.test(hash)) {
		return json({ found: false }, { status: 400 });
	}

	try {
		const db = getDb();
		const hashBinary = Buffer.from(hash, 'hex');
		

		const searchQuery = `
			(SELECT 'transaction' as type, 1 as found
			FROM kaspad.transactions 
			WHERE transaction_id = $1 
			LIMIT 1)
			
			UNION ALL
			
			(SELECT 'block' as type, 1 as found
			FROM kaspad.blocks 
			WHERE block_hash = $1 
			LIMIT 1)
		`;
		
		const result = await db.query(searchQuery, [hashBinary]);
		
		if (result.rows.length === 0) {
			return json({ found: false }, {
				headers: {
					'Cache-Control': 'public, max-age=300, s-maxage=300',
					'Vercel-CDN-Cache-Control': 'public, max-age=300'
				}
			});
		}
		
		// Return the first match (transaction takes precedence if both exist)
		const match = result.rows[0];
		return json({ 
			found: true,
			type: match.type 
		}, {
			headers: {
				'Cache-Control': 'public, max-age=300, s-maxage=300',
				'Vercel-CDN-Cache-Control': 'public, max-age=300'
			}
		});
		
	} catch (error) {
		console.error('Error searching for hash:', error);
		return json({ found: false }, { status: 500 });
	}
} 