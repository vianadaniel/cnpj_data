const fs = require('fs');
const csv = require('csv-parser');

// Função para processar arquivo de empresas e inserir no banco de dados
async function processEmpresasFile(db, filePath) {
    console.log(`Processando arquivo de empresas: ${filePath}`);

    try {
        // Set PRAGMA settings before any transaction begins
        await db.run('PRAGMA synchronous = NORMAL'); // Less aggressive than OFF but still faster
        await db.run('PRAGMA journal_mode = WAL'); // Write-Ahead Logging is more robust than MEMORY
        await db.run('PRAGMA temp_store = MEMORY'); // Store temp tables in memory
        await db.run('PRAGMA cache_size = 10000'); // Increase cache size for better performance

        // Preparar statement para inserção em massa
        const stmt = await db.prepare(`
        INSERT OR REPLACE INTO empresas (
            cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
            capital_social, porte_empresa, ente_federativo
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
                        'cnpj_basico',
                        'razao_social',
                        'natureza_juridica',
                        'qualificacao_responsavel',
                        'capital_social',
                        'porte_empresa',
                        'ente_federativo'
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
                        row.razao_social,
                        row.natureza_juridica,
                        row.qualificacao_responsavel,
                        row.capital_social,
                        row.porte_empresa,
                        row.ente_federativo
                    );

                    processed++;

                    if (processed % batchSize === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${processed} registros de empresas`);

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
        console.error(`Erro ao processar arquivo de empresas: ${error.message}`);
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
    processEmpresasFile
};