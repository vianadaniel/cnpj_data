const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de países e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processPaisesFile(filePath) {
    console.log(`Iniciando processamento de países: ${filePath}`);

    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Criar tabela temporária para COPY
        await client.query(`
            CREATE TEMP TABLE temp_paises (
                codigo TEXT,
                nome TEXT
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
                headers: ['codigo', 'nome'],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.codigo && row.nome) {
                // Debug log para verificar os dados
                if (processed < 5) {
                    console.log('Dados sendo processados:', {
                        original: row
                    });
                }

                batch.push([row.codigo, row.nome]);

                if (batch.length >= BATCH_SIZE) {
                    const values = [];
                    const placeholders = [];
                    let paramCount = 1;

                    for (const [codigo, nome] of batch) {
                        values.push(codigo, nome);
                        placeholders.push(`($${paramCount}, $${paramCount + 1})`);
                        paramCount += 2;
                    }

                    await client.query(`
                        INSERT INTO temp_paises (codigo, nome)
                        VALUES ${placeholders.join(', ')}
                    `, values);

                    processed += batch.length;
                    batch = [];
                    console.log(`Processados ${processed} registros de países`);
                }
            }
        }

        // Processar registros restantes no batch
        if (batch.length > 0) {
            const values = [];
            const placeholders = [];
            let paramCount = 1;

            for (const [codigo, nome] of batch) {
                values.push(codigo, nome);
                placeholders.push(`($${paramCount}, $${paramCount + 1})`);
                paramCount += 2;
            }

            await client.query(`
                INSERT INTO temp_paises (codigo, nome)
                VALUES ${placeholders.join(', ')}
            `, values);

            processed += batch.length;
            console.log(`Processados registros finais: ${batch.length}`);
        }

        // Verificar dados na tabela temporária
        const tempCount = await client.query('SELECT COUNT(*) FROM temp_paises');
        console.log(`Registros na tabela temporária: ${tempCount.rows[0].count}`);

        if (tempCount.rows[0].count === 0) {
            throw new Error('Nenhum registro foi inserido na tabela temporária');
        }

        console.log('Iniciando inserção na tabela final...');
        // Inserir dados da tabela temporária na tabela final
        const result = await client.query(`
            INSERT INTO paises (codigo, nome)
            SELECT codigo, nome FROM temp_paises
            ON CONFLICT (codigo) DO UPDATE
            SET nome = EXCLUDED.nome
            RETURNING codigo
        `);

        console.log(`Registros inseridos/atualizados: ${result.rowCount}`);

        // Verificar dados na tabela final
        const finalCount = await client.query('SELECT COUNT(*) FROM paises');
        console.log(`Total de registros na tabela final: ${finalCount.rows[0].count}`);

        await client.query('COMMIT');
        console.log(`Processamento de países concluído. Total processado: ${processed} registros`);

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro ao processar arquivo de países:', error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processPaisesFile
};