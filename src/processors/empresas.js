const fs = require('fs');

// Função para processar arquivo de empresas e inserir no banco de dados
async function processEmpresasFile(db, filePath) {
    console.log(`Processando arquivo de empresas: ${filePath}`);

    // Preparar statement para inserção em massa
    const stmt = await db.prepare(`
    INSERT OR REPLACE INTO empresas (
      cnpj_basico, razao_social, natureza_juridica,
      qualificacao_responsavel, capital_social, porte_empresa, ente_federativo
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    // Processar em lotes para melhor performance
    const batchSize = 20000;
    let processed = 0;
    let lineBuffer = '';


    await db.run('PRAGMA synchronous = OFF'); // Desativa escrita segura (mais rápido)
    await db.run('PRAGMA journal_mode = MEMORY'); // Usa journal na RAM (mais rápido)

    await db.run('BEGIN TRANSACTION');

    // Criar stream de leitura
    const readStream = fs.createReadStream(filePath, { encoding: 'latin1' });

    await new Promise((resolve, reject) => {
        readStream.on('data', async (chunk) => {
            // Pausar o stream para processar o chunk
            readStream.pause();

            lineBuffer += chunk;
            const lines = lineBuffer.split('\n');

            // O último elemento pode ser uma linha incompleta
            lineBuffer = lines.pop() || '';

            try {
                for (const line of lines) {
                    if (!line.trim()) continue;

                    // Parse da linha conforme layout dos dados
                    const fields = line.split(';').map(field => field.replace(/^"|"$/g, ''));

                    if (fields.length < 7) continue; // Ignorar linhas inválidas

                    await stmt.run(
                        fields[0], // cnpj_basico
                        fields[1], // razao_social
                        fields[2], // natureza_juridica
                        fields[3], // qualificacao_responsavel
                        parseFloat(fields[4].replace(',', '.')) || 0, // capital_social
                        fields[5], // porte_empresa
                        fields[6]  // ente_federativo
                    );

                    processed++;

                    if (processed % batchSize === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${processed} registros de empresas`);
                    }
                }

                // Retomar o stream
                readStream.resume();
            } catch (err) {
                reject(err);
            }
        });

        readStream.on('end', async () => {
            // Processar qualquer linha restante no buffer
            if (lineBuffer.trim()) {
                const fields = lineBuffer.trim().split(';').map(field => field.replace(/^"|"$/g, ''));

                if (fields.length >= 7) {
                    try {
                        await stmt.run(
                            fields[0], fields[1], fields[2], fields[3],
                            parseFloat(fields[4].replace(',', '.')) || 0,
                            fields[5], fields[6]
                        );
                        processed++;
                    } catch (err) {
                        reject(err);
                    }
                }
            }
            resolve();
        });

        readStream.on('error', (err) => {
            reject(err);
        });
    });

    await db.run('COMMIT');
    await stmt.finalize();

    console.log(`Processamento do arquivo de empresas concluído. Total: ${processed} registros`);
}

module.exports = {
    processEmpresasFile
};