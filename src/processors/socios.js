const fs = require('fs');

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
        const batchSize = 50000; // Reduced batch size for more frequent commits
        let processed = 0;
        let lineBuffer = '';

        // Start transaction
        await db.run('BEGIN TRANSACTION');

        // Criar stream de leitura
        const readStream = fs.createReadStream(filePath, { encoding: 'latin1' });

        await new Promise((resolve, reject) => {
            readStream.on('data', async (chunk) => {
                try {
                    // Pausar o stream para processar o chunk
                    readStream.pause();

                    lineBuffer += chunk;
                    const lines = lineBuffer.split('\n');

                    // O último elemento pode ser uma linha incompleta
                    lineBuffer = lines.pop() || '';

                    for (const line of lines) {
                        if (!line.trim()) continue;

                        // Parse da linha conforme layout dos dados
                        const fields = line.split(';').map(field => field.replace(/^"|"$/g, ''));

                        if (fields.length < 11) continue; // Ignorar linhas inválidas

                        await stmt.run(
                            fields[0],  // cnpj_basico
                            fields[1],  // identificador_socio
                            fields[2],  // nome_socio
                            fields[3],  // cnpj_cpf_socio
                            fields[4],  // qualificacao_socio
                            fields[5],  // data_entrada_sociedade
                            fields[6],  // pais
                            fields[7],  // representante_legal
                            fields[8],  // nome_representante
                            fields[9],  // qualificacao_representante
                            fields[10]  // faixa_etaria
                        );

                        processed++;

                        if (processed % batchSize === 0) {
                            await db.run('COMMIT');
                            await db.run('BEGIN TRANSACTION');
                            console.log(`Processados ${processed} registros de socios`);

                            // Add a checkpoint to ensure WAL is written to the main database file
                            await db.run('PRAGMA wal_checkpoint(PASSIVE)');
                        }
                    }

                    // Retomar o stream
                    readStream.resume();
                } catch (err) {
                    readStream.destroy(); // Ensure stream is closed on error
                    reject(err);
                }
            });

            readStream.on('end', async () => {
                try {
                    // Processar qualquer linha restante no buffer
                    if (lineBuffer.trim()) {
                        const fields = lineBuffer.trim().split(';').map(field => field.replace(/^"|"$/g, ''));

                        if (fields.length >= 11) {
                            await stmt.run(
                                fields[0], fields[1], fields[2], fields[3], fields[4],
                                fields[5], fields[6], fields[7], fields[8], fields[9],
                                fields[10]
                            );
                            processed++;
                        }
                    }

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

            readStream.on('error', (err) => {
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