const fs = require('fs');
const csv = require('csv-parser');

// Função para processar arquivo de estabelecimentos e inserir no banco de dados
async function processEstabelecimentosFile(db, filePath) {
    console.log(`Processando arquivo de estabelecimentos: ${filePath}`);

    try {
        // Set PRAGMA settings before any transaction begins
        await db.run('PRAGMA synchronous = NORMAL'); // Less aggressive than OFF but still faster
        await db.run('PRAGMA journal_mode = WAL'); // Write-Ahead Logging is more robust than MEMORY
        await db.run('PRAGMA temp_store = MEMORY'); // Store temp tables in memory
        await db.run('PRAGMA cache_size = 10000'); // Increase cache size for better performance

        // Preparar statement para inserção em massa
        const stmt = await db.prepare(`
        INSERT OR REPLACE INTO estabelecimentos (
            cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia,
            situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal,
            cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento,
            bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2,
            ddd_fax, fax, email, situacao_especial, data_situacao_especial
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);

        // Processar em lotes para melhor performance
        const batchSize = 5000000; // Reduced batch size for more frequent commits
        let processed = 0;

        // Start transaction
        await db.run('BEGIN TRANSACTION');

        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(filePath, { encoding: 'latin1' })
                .pipe(csv({
                    separator: ';',
                    headers: [
                        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
                        'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                        'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
                        'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                        'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                        'ddd_fax', 'fax', 'email', 'situacao_especial', 'data_situacao_especial'
                    ],
                    skipLines: 0
                }));

            // Handle data events
            stream.on('data', async (row) => {
                try {
                    // Pause the stream while we process this row
                    stream.pause();

                    await stmt.run(
                        row.cnpj_basico,
                        row.cnpj_ordem,
                        row.cnpj_dv,
                        row.matriz_filial,
                        row.nome_fantasia,
                        row.situacao_cadastral,
                        row.data_situacao_cadastral,
                        row.motivo_situacao_cadastral,
                        row.nome_cidade_exterior,
                        row.pais,
                        row.data_inicio_atividade,
                        row.cnae_fiscal_principal,
                        row.cnae_fiscal_secundaria,
                        row.tipo_logradouro,
                        row.logradouro,
                        row.numero,
                        row.complemento,
                        row.bairro,
                        row.cep,
                        row.uf,
                        row.municipio,
                        row.ddd_1,
                        row.telefone_1,
                        row.ddd_2,
                        row.telefone_2,
                        row.ddd_fax,
                        row.fax,
                        row.email,
                        row.situacao_especial,
                        row.data_situacao_especial
                    );

                    processed++;

                    if (processed % batchSize === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${processed} registros de estabelecimentos`);

                        // Add a checkpoint to ensure WAL is written to the main database file
                        await db.run('PRAGMA wal_checkpoint(PASSIVE)');
                    }

                    // Resume the stream
                    stream.resume();
                } catch (err) {
                    stream.destroy(err);
                    reject(err);
                }
            });

            stream.on('end', async () => {
                try {
                    // Commit any remaining changes
                    await db.run('COMMIT');
                    console.log(`Processamento concluído. Total de ${processed} registros.`);

                    // Finalize statement
                    await stmt.finalize();

                    resolve();
                } catch (err) {
                    reject(err);
                }
            });

            stream.on('error', (err) => {
                reject(err);
            });
        });
    } catch (error) {
        console.error(`Erro ao processar arquivo de estabelecimentos: ${error.message}`);
        // Try to rollback if possible
        try {
            await db.run('ROLLBACK');
        } catch (rollbackError) {
            console.error(`Erro ao fazer rollback: ${rollbackError.message}`);
        }
        throw error;
    }
}

module.exports = {
    processEstabelecimentosFile
};