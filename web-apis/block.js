import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';

function isValidBlockHash(hash) {
  const pattern = /^[a-fA-F0-9]{64}$/;
  return pattern.test(hash);
}

export async function GET({ params }) {
  const blockHash = params.hash;

  if (!isValidBlockHash(blockHash)) {
    return json({
      blockHash,
      blockData: null,
      transactions: [],
      status: 'invalid_format',
      error: 'Invalid block hash format. Block hashes must be 64-character hexadecimal strings.'
    }, { status: 400 });
  }

  const pool = getDb();
  const client = await pool.connect();

  try {
    const blockHashBinary = Buffer.from(blockHash, 'hex');

    const blockSql = `
      SELECT 
        block_time,
        encode(block_hash, 'hex') as block_hash,
        daa_score,
        version,
        encode(hash_merkle_root, 'hex') as hash_merkle_root,
        encode(accepted_id_merkle_root, 'hex') as accepted_id_merkle_root,
        encode(utxo_commitment, 'hex') as utxo_commitment,
        bits,
        nonce,
        encode(blue_work, 'hex') as blue_work,
        blue_score,
        encode(pruning_point, 'hex') as pruning_point,
        difficulty,
        encode(selected_parent_hash, 'hex') as selected_parent_hash,
        is_chain_block
      FROM kaspad.blocks 
      WHERE block_hash = $1
    `;

    const parentsSql = `
      SELECT encode(parent_hash, 'hex') as parent_hash
      FROM kaspad.blocks_parents 
      WHERE block_hash = $1
      ORDER BY parent_hash
    `;

    const childrenSql = `
      SELECT encode(block_hash, 'hex') as block_hash
      FROM kaspad.blocks_parents 
      WHERE parent_hash = $1
      ORDER BY block_hash
    `;

    const transactionsSql = `
      SELECT 
        t.block_time,
        t.transaction_id,
        t.subnetwork_id,
        t.mass,
        t.total_input_amount,
        t.total_output_amount,
        t.protocol_id
      FROM kaspad.transactions t
      INNER JOIN kaspad.blocks_transactions bt ON t.transaction_id = bt.transaction_id
      WHERE bt.block_hash = $1
      ORDER BY bt.index
      LIMIT 1000
    `;

    const [blockResult, parentsResult, childrenResult, transactionsResult] = await Promise.all([
      client.query(blockSql, [blockHashBinary]),
      client.query(parentsSql, [blockHashBinary]),
      client.query(childrenSql, [blockHashBinary]),
      client.query(transactionsSql, [blockHashBinary])
    ]);

    if (blockResult.rows.length === 0) {
      return json({
        blockHash,
        blockData: null,
        transactions: [],
        status: 'not_found',
        error: 'Block not found'
      }, { status: 404 });
    }

    const blockRow = blockResult.rows[0];

    const blockData = {
      hash: blockRow.block_hash,
      version: blockRow.version,
      merkleRoot: blockRow.hash_merkle_root,
      acceptedIdMerkleRoot: blockRow.accepted_id_merkle_root,
      utxoCommitment: blockRow.utxo_commitment,
      timestamp: blockRow.block_time,
      bits: blockRow.bits,
      nonce: blockRow.nonce,
      blueWork: blockRow.blue_work,
      daaScore: parseInt(blockRow.daa_score),
      blueScore: blockRow.blue_score,
      difficulty: blockRow.difficulty,
      selectedParentHash: blockRow.selected_parent_hash,
      isChainBlock: blockRow.is_chain_block,
      pruningPoint: blockRow.pruning_point,
      parentHashes: parentsResult.rows.map((row) => row.parent_hash),
      childrenHashes: childrenResult.rows.map((row) => row.block_hash),
      transactionCount: transactionsResult.rows.length
    };

    const transactions = transactionsResult.rows.map((tx) => {
      const isCoinbase = tx.subnetwork_id === 1;
      const fee = isCoinbase ? 0 : Math.max(0, tx.total_input_amount - tx.total_output_amount);

      let protocol;
      if (tx.protocol_id === 0) {
        protocol = 'KRC';
      } else if (tx.protocol_id === 1) {
        protocol = 'KNS';
      } else if (tx.protocol_id === 2) {
        protocol = 'Kasia';
      }

      return {
        txId: tx.transaction_id.toString('hex'),
        blockTime: tx.block_time,
        mass: tx.mass,
        totalInputAmount: tx.total_input_amount,
        totalOutputAmount: tx.total_output_amount,
        fee,
        isCoinbase,
        protocol
      };
    });

    return json({
      blockHash,
      blockData,
      transactions,
      status: 'success'
    }, {
      headers: {
        'Cache-Control': 'public, max-age=300, s-maxage=300',
        'Vercel-CDN-Cache-Control': 'public, max-age=300'
      }
    });
  } catch (err) {
    console.error(`Block ${blockHash} - API query failed:`, err);
    return json({
      blockHash,
      blockData: null,
      transactions: [],
      status: 'error',
      error: 'Failed to load block data'
    }, { status: 500 });
  } finally {
    client.release();
  }
}


