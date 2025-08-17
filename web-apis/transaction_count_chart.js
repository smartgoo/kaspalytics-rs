import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';
import { isValidKaspaAddress } from '$lib/utils/isValidKaspaAddress.js';
import { pgToChartJs } from '$lib/utils/pgToChartJs.js';

export async function GET({ params, url }) {
  const address = params.address;
  if (!isValidKaspaAddress(address)) {
    return json({ status: 'invalid_format', error: 'Invalid address format' }, { status: 400 });
  }

  const range = (url.searchParams.get('range') || '24h').toLowerCase(); // '1h' | '24h' | '7d'
  const includeCoinbaseParam = (url.searchParams.get('include_coinbase') || '1').toLowerCase();
  const includeCoinbase = !(includeCoinbaseParam === '0' || includeCoinbaseParam === 'false' || includeCoinbaseParam === 'no');

  // Determine grouping granularity and interval
  let dateTruncUnit = 'hour';
  let intervalExpr = "interval '24 hours'";
  if (range === '1h') {
    dateTruncUnit = 'minute';
    intervalExpr = "interval '1 hour'";
  } else if (range === '7d' || range === '7days' || range === '7days') {
    dateTruncUnit = 'hour';
    intervalExpr = "interval '7 days'";
  }

  const db = getDb();
  const dbStart = Date.now();
  
  try {

    const coinbaseFilter = includeCoinbase ? '' : 'AND to2.is_coinbase = false';
    

    const inputsQuery = `
      SELECT 
        ti.transaction_id,
        ti.block_time
      FROM kaspad.transactions_inputs ti
      WHERE ti.utxo_script_public_key_address = $1 
        AND ti.block_time >= (now() at time zone 'utc') - ${intervalExpr}
        AND ti.block_time <= (now() at time zone 'utc')
      ORDER BY ti.block_time DESC
    `;
    
    const outputsQuery = `
      SELECT 
        to2.transaction_id,
        to2.block_time
      FROM kaspad.transactions_outputs to2
      WHERE to2.script_public_key_address = $1 
        AND to2.block_time >= (now() at time zone 'utc') - ${intervalExpr}
        AND to2.block_time <= (now() at time zone 'utc')
        ${coinbaseFilter}
      ORDER BY to2.block_time DESC
    `;

    const queryStart = Date.now();
    
    // Execute both queries in parallel for better performance
    const [inputsResult, outputsResult] = await Promise.all([
      db.query(inputsQuery, [address]),
      db.query(outputsQuery, [address])
    ]);
    
    const queryDuration = Date.now() - queryStart;
    console.log(`[tx-count-api] Parallel queries for ${address} took ${queryDuration}ms (inputs: ${inputsResult.rows.length}, outputs: ${outputsResult.rows.length})`);

    // JavaScript aggregation: combine and deduplicate transactions by time bucket
    const jsStart = Date.now();
    const transactionMap = new Map();
    
    // Process inputs - deduplicate by transaction_id first
    const inputTxMap = new Map();
    inputsResult.rows.forEach(row => {
      const txId = row.transaction_id.toString('hex');
      if (!inputTxMap.has(txId)) {
        inputTxMap.set(txId, row.block_time);
      }
    });
    
    // Add deduplicated input transactions to main map
    inputTxMap.forEach((blockTime, txId) => {
      transactionMap.set(txId, blockTime);
    });
    
    // Process outputs - deduplicate by transaction_id first
    const outputTxMap = new Map();
    outputsResult.rows.forEach(row => {
      const txId = row.transaction_id.toString('hex');
      if (!outputTxMap.has(txId)) {
        outputTxMap.set(txId, row.block_time);
      }
    });
    
    // Add deduplicated output transactions to main map (don't overwrite existing inputs)
    outputTxMap.forEach((blockTime, txId) => {
      if (!transactionMap.has(txId)) {
        transactionMap.set(txId, blockTime);
      }
    });
    
    // Group by time buckets in JavaScript
    const bucketCounts = new Map();
    
    transactionMap.forEach((blockTime) => {
      // Truncate to appropriate time bucket
      const date = new Date(blockTime);
      let bucketKey;
      
      if (dateTruncUnit === 'minute') {
        date.setSeconds(0, 0);
        bucketKey = date.toISOString();
      } else { // hour
        date.setMinutes(0, 0, 0);
        bucketKey = date.toISOString();
      }
      
      bucketCounts.set(bucketKey, (bucketCounts.get(bucketKey) || 0) + 1);
    });
    
    // Generate time buckets and fill in data
    const buckets = [];
    const startTime = new Date();
    startTime.setTime(startTime.getTime() - (range === '1h' ? 60*60*1000 : range === '7d' ? 7*24*60*60*1000 : 24*60*60*1000));
    
    if (dateTruncUnit === 'minute') {
      startTime.setSeconds(0, 0);
      for (let time = new Date(startTime); time <= new Date(); time.setMinutes(time.getMinutes() + 1)) {
        const bucketKey = time.toISOString();
        buckets.push({
          timestamp: bucketKey,
          Transactions: bucketCounts.get(bucketKey) || 0
        });
      }
    } else { // hour
      startTime.setMinutes(0, 0, 0);
      for (let time = new Date(startTime); time <= new Date(); time.setHours(time.getHours() + 1)) {
        const bucketKey = time.toISOString();
        buckets.push({
          timestamp: bucketKey,
          Transactions: bucketCounts.get(bucketKey) || 0
        });
      }
    }
    
    const jsDuration = Date.now() - jsStart;
    console.log(`[tx-count-api] JavaScript aggregation for ${address} took ${jsDuration}ms, found ${transactionMap.size} unique transactions across ${buckets.length} buckets`);

    const chartData = pgToChartJs(buckets);
    return json({ status: 'success', chartData }, {
      headers: {
        'Cache-Control': 'public, max-age=30, s-maxage=30',
        'Vercel-CDN-Cache-Control': 'public, max-age=30'
      }
    });
  } catch (e) {
    console.error('Address transactions-count DB query failed:', e);
    return json({ status: 'error', error: 'Internal Server Error' }, { status: 500 });
  } finally {
    const dbDur = Date.now() - dbStart;
    console.log(`[tx-count-api] Transactions count (${range}) total request for ${address} took ${dbDur}ms`);
  }
}


