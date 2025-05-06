const fs = require('fs');
const csv = require('csv-parser');

/**
 * Processa um arquivo de Sócios e insere os dados no banco de dados
 * @param {Object} db - Conexão com o banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processSociosFile(db, filePath) {
    console.log(`Processando arquivo de socios: ${filePath}`);

    try {
        // Set PRAGMA settings before any transaction begins
        await db.run('PRAGMA synchronous = NORMAL'); // Less aggressive than OFF but still faster
        await db.run('PRAGMA journal_mode = WAL'); // Write-Ahead Logging is more robust than MEMORY
        await db.run('PRAGMA temp_store = MEMORY'); // Store temp tables in memory
        await db.run('PRAGMA cache_size = 10000'); // Increase cache size for better performance

        // Preparar statement para inserção em massa
        const stmt = await db.prepare(`
        INSERT OR REPLACE INTO socios (
            cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio,
            qualificacao_socio, data_entrada_sociedade, pais, representante_legal,
            nome_representante, qualificacao_representante, faixa_etaria
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
                        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
                        'nome_representante', 'qualificacao_representante', 'faixa_etaria'
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
                        row.identificador_socio,
                        row.nome_socio,
                        row.cnpj_cpf_socio,
                        row.qualificacao_socio,
                        row.data_entrada_sociedade,
                        row.pais,
                        row.representante_legal,
                        row.nome_representante,
                        row.qualificacao_representante,
                        row.faixa_etaria
                    );

                    processed++;

                    if (processed % batchSize === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${processed} registros de socios`);

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
        console.error(`Erro ao processar arquivo de socios: ${error.message}`);
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
    processSociosFile
};