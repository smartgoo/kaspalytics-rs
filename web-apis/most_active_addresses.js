import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';




export async function GET({ url }) {
  let client;

  try {
    const requestId = Math.random().toString(36).slice(2);
    console.time(`Most Active Addresses API ${requestId}`);

    // timeframe query param (default 24h)
    const timeFrame = url.searchParams.get('timeframe') || '24h';
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
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= date_trunc('day', now() at time zone 'utc')
          AND agg.minute_bucket < date_trunc('day', now() at time zone 'utc') + INTERVAL '1 day'
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100;`;
    } else if (timeFrame === 'yesterday') {
      sql = `
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= (date_trunc('day', now() at time zone 'utc') - INTERVAL '1 day')
          AND agg.minute_bucket < date_trunc('day', now() at time zone 'utc')
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100;`;
    } else {

      sql = `
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= (now() at time zone 'utc' - ${intervalExpression})
          AND agg.minute_bucket <= (now() at time zone 'utc')
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100;`;
    }
    const db = getDb();
    client = await db.connect();
    const result = await client.query(sql);

    console.timeEnd(`Most Active Addresses API ${requestId}`);
    console.log(`[Most Active Spenders] ${requestId}: timeframe: ${timeFrame}`);
    
    const payload = { 
      status: 'success', 
      addresses: result.rows, 
      timeFrame,
      type: 'spending',
      optimization: 'database_aggregate'
    };
    return json(payload, { headers: { 'Cache-Control': 'public, max-age=5, s-maxage=5' } });
  } catch (error) {
    console.error('Most Active Addresses API error:', error);
    return json({ status: 'error', error: error.message, addresses: [] }, { status: 500 });
  } finally {
    if (client) client.release();
  }
}
