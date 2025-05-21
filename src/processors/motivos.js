const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de motivos de situação cadastral e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processMotivosFile(filePath) {
    console.log(`Iniciando processamento de motivos: ${filePath}`);

    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Criar tabela temporária para COPY
        await client.query(`
            CREATE TEMP TABLE temp_motivos (
                codigo TEXT,
                descricao TEXT
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
                headers: ['codigo', 'descricao'],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.codigo && row.descricao) {
                // Debug log para verificar os dados
                if (processed < 5) {
                    console.log('Dados sendo processados:', {
                        original: row
                    });
                }

                batch.push([row.codigo, row.descricao]);

                if (batch.length >= BATCH_SIZE) {
                    const values = [];
                    const placeholders = [];
                    let paramCount = 1;

                    for (const [codigo, descricao] of batch) {
                        values.push(codigo, descricao);
                        placeholders.push(`($${paramCount}, $${paramCount + 1})`);
                        paramCount += 2;
                    }

                    await client.query(`
                        INSERT INTO temp_motivos (codigo, descricao)
                        VALUES ${placeholders.join(', ')}
                    `, values);

                    processed += batch.length;
                    batch = [];
                    console.log(`Processados ${processed} registros de motivos`);
                }
            }
        }

        // Processar registros restantes no batch
        if (batch.length > 0) {
            const values = [];
            const placeholders = [];
            let paramCount = 1;

            for (const [codigo, descricao] of batch) {
                values.push(codigo, descricao);
                placeholders.push(`($${paramCount}, $${paramCount + 1})`);
                paramCount += 2;
            }

            await client.query(`
                INSERT INTO temp_motivos (codigo, descricao)
                VALUES ${placeholders.join(', ')}
            `, values);

            processed += batch.length;
            console.log(`Processados registros finais: ${batch.length}`);
        }

        // Verificar dados na tabela temporária
        const tempCount = await client.query('SELECT COUNT(*) FROM temp_motivos');
        console.log(`Registros na tabela temporária: ${tempCount.rows[0].count}`);

        if (tempCount.rows[0].count === 0) {
            throw new Error('Nenhum registro foi inserido na tabela temporária');
        }

        console.log('Iniciando inserção na tabela final...');
        // Inserir dados da tabela temporária na tabela final
        const result = await client.query(`
            INSERT INTO motivos (codigo, descricao)
            SELECT codigo, descricao FROM temp_motivos
            ON CONFLICT (codigo) DO UPDATE
            SET descricao = EXCLUDED.descricao
            RETURNING codigo
        `);

        console.log(`Registros inseridos/atualizados: ${result.rowCount}`);

        // Verificar dados na tabela final
        const finalCount = await client.query('SELECT COUNT(*) FROM motivos');
        console.log(`Total de registros na tabela final: ${finalCount.rows[0].count}`);

        await client.query('COMMIT');
        console.log(`Processamento de motivos concluído. Total processado: ${processed} registros`);

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro ao processar arquivo de motivos:', error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processMotivosFile
};