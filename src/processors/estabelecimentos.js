const fs = require('fs');

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
          cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
          data_situacao_cadastral, motivo_situacao_cadastral, cidade_exterior,
          pais, data_inicio_atividade, cnae_principal, cnaes_secundarios,
          tipo_logradouro, logradouro, numero, complemento, bairro, cep,
          uf, municipio, ddd1, telefone1, ddd2, telefone2, email
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

                        if (fields.length < 25) continue; // Ignorar linhas inválidas

                        await stmt.run(
                            fields[0],  // cnpj_basico
                            fields[1],  // cnpj_ordem
                            fields[2],  // cnpj_dv
                            fields[3],  // nome_fantasia
                            fields[4],  // situacao_cadastral
                            fields[5],  // data_situacao_cadastral
                            fields[6],  // motivo_situacao_cadastral
                            fields[7],  // cidade_exterior
                            fields[8],  // pais
                            fields[9],  // data_inicio_atividade
                            fields[10], // cnae_principal
                            fields[11], // cnaes_secundarios
                            fields[12], // tipo_logradouro
                            fields[13], // logradouro
                            fields[14], // numero
                            fields[15], // complemento
                            fields[16], // bairro
                            fields[17], // cep
                            fields[18], // uf
                            fields[19], // municipio
                            fields[20], // ddd1
                            fields[21], // telefone1
                            fields[22], // ddd2
                            fields[23], // telefone2
                            fields[24]  // email
                        );

                        processed++;

                        if (processed % batchSize === 0) {
                            await db.run('COMMIT');
                            await db.run('BEGIN TRANSACTION');
                            console.log(`Processados ${processed} registros de estabelecimentos`);

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

                        if (fields.length >= 25) {
                            await stmt.run(
                                fields[0], fields[1], fields[2], fields[3], fields[4],
                                fields[5], fields[6], fields[7], fields[8], fields[9],
                                fields[10], fields[11], fields[12], fields[13], fields[14],
                                fields[15], fields[16], fields[17], fields[18], fields[19],
                                fields[20], fields[21], fields[22], fields[23], fields[24]
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