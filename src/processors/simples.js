const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de Simples Nacional e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processSimplesFile(filePath) {
    console.log(`Iniciando processamento de Simples Nacional: ${filePath}`);

    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Criar tabela temporária para COPY
        await client.query(`
            CREATE TEMP TABLE temp_simples (
                cnpj TEXT,
                opcao_simples TEXT,
                data_opcao_simples TEXT,
                data_exclusao_simples TEXT,
                opcao_mei TEXT,
                data_opcao_mei TEXT,
                data_exclusao_mei TEXT
            ) ON COMMIT DROP
        `);

        console.log('Iniciando processamento...');
        let processed = 0;
        const BATCH_SIZE = 1000;
        let batch = [];

        // Criar stream de leitura com csv-parser
        const stream = fs.createReadStream(filePath, { encoding: 'latin1' })
            .pipe(csv({
                separator: ';',
                headers: ['cnpj', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.cnpj) {
                // Debug log para verificar os dados
                if (processed < 5) {
                    console.log('Dados sendo processados:', {
                        original: row
                    });
                }

                batch.push([
                    row.cnpj,
                    row.opcao_simples,
                    row.data_opcao_simples,
                    row.data_exclusao_simples,
                    row.opcao_mei,
                    row.data_opcao_mei,
                    row.data_exclusao_mei
                ]);

                if (batch.length >= BATCH_SIZE) {
                    const values = [];
                    const placeholders = [];
                    let paramCount = 1;

                    for (const record of batch) {
                        values.push(...record);
                        placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6})`);
                        paramCount += 7;
                    }

                    await client.query(`
                        INSERT INTO temp_simples (cnpj, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei)
                        VALUES ${placeholders.join(', ')}
                    `, values);

                    processed += batch.length;
                    batch = [];
                    console.log(`Processados ${processed} registros de Simples Nacional`);
                }
            }
        }

        // Processar registros restantes no batch
        if (batch.length > 0) {
            const values = [];
            const placeholders = [];
            let paramCount = 1;

            for (const record of batch) {
                values.push(...record);
                placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6})`);
                paramCount += 7;
            }

            await client.query(`
                INSERT INTO temp_simples (cnpj, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei)
                VALUES ${placeholders.join(', ')}
            `, values);

            processed += batch.length;
            console.log(`Processados registros finais: ${batch.length}`);
        }

        // Verificar dados na tabela temporária
        const tempCount = await client.query('SELECT COUNT(*) FROM temp_simples');
        console.log(`Registros na tabela temporária: ${tempCount.rows[0].count}`);

        if (tempCount.rows[0].count === 0) {
            throw new Error('Nenhum registro foi inserido na tabela temporária');
        }

        console.log('Iniciando inserção na tabela final...');
        // Inserir dados da tabela temporária na tabela final
        const result = await client.query(`
            INSERT INTO simples (cnpj, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei)
            SELECT cnpj, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei FROM temp_simples
            ON CONFLICT (cnpj) DO UPDATE
            SET opcao_simples = EXCLUDED.opcao_simples,
                data_opcao_simples = EXCLUDED.data_opcao_simples,
                data_exclusao_simples = EXCLUDED.data_exclusao_simples,
                opcao_mei = EXCLUDED.opcao_mei,
                data_opcao_mei = EXCLUDED.data_opcao_mei,
                data_exclusao_mei = EXCLUDED.data_exclusao_mei
            RETURNING cnpj
        `);

        console.log(`Registros inseridos/atualizados: ${result.rowCount}`);

        // Verificar dados na tabela final
        const finalCount = await client.query('SELECT COUNT(*) FROM simples');
        console.log(`Total de registros na tabela final: ${finalCount.rows[0].count}`);

        await client.query('COMMIT');
        console.log(`Processamento de Simples Nacional concluído. Total processado: ${processed} registros`);

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro ao processar arquivo de Simples Nacional:', error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processSimplesFile
};